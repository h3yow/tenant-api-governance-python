"""写请求幂等治理。

保证：
- 同一幂等键的重复/并发提交只有第一个请求执行业务处理器（副作用一次），
  其余请求等待并返回与首次完全一致的结论（含业务错误）。
- 同一幂等键携带语义不同的载荷 -> 明确拒绝（IDEMPOTENCY_CONFLICT），
  不静默以先到者为准。
- 存储依赖异常按 IdemFailMode 处理：
    closed       -> 拒绝（DEPENDENCY_UNAVAILABLE），不透出底层异常；
    local_dedupe -> 仅在本进程内去重（record.degraded=True 明确标注），
                    存储恢复后若发现远端键属于他人/载荷冲突，仍按冲突/回放处理。
- 首次执行期间进程崩溃留下的 in_flight 记录，超过超时阈值后可被接管，
  避免永久卡死。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Protocol

from app.errors import DependencyError, ErrorCode, GovernanceError
from app.logging_setup import log_decision
from app.models import Principal


class RecordStatus(str, Enum):
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"


class IdemFailMode(str, Enum):
    CLOSED = "closed"
    LOCAL_DEDUPE = "local_dedupe"


class AcquireOutcome(str, Enum):
    FIRST = "first"
    REPLAY = "replay"
    IN_FLIGHT = "in_flight"   # 等待超时，首个请求仍未完成
    CONFLICT = "conflict"
    DEGRADED_LOCAL_FIRST = "degraded_local_first"
    DEGRADED_LOCAL_REPLAY = "degraded_local_replay"
    DEGRADED_REJECT = "degraded_reject"


@dataclass(frozen=True)
class IdempotencyRecord:
    key: str
    tenant_id: str
    fingerprint: str
    status: RecordStatus
    result: Any = None
    error: GovernanceError | None = None
    owner_token: str = ""
    created_at: float = 0.0
    degraded: bool = False


@dataclass(frozen=True)
class AcquireResult:
    outcome: AcquireOutcome
    record: IdempotencyRecord | None = None

    @property
    def may_execute(self) -> bool:
        return self.outcome in (AcquireOutcome.FIRST, AcquireOutcome.DEGRADED_LOCAL_FIRST)

    @property
    def owner_token(self) -> str:
        return self.record.owner_token if self.record else ""


class IdempotencyStore(Protocol):
    def put_if_absent(self, record: IdempotencyRecord) -> IdempotencyRecord: ...
    def get(self, key: str) -> IdempotencyRecord | None: ...
    def compare_and_complete(
        self, key: str, owner_token: str, result: Any, error: GovernanceError | None
    ) -> bool: ...
    def delete_if_owner(self, key: str, owner_token: str) -> None: ...


def fingerprint_payload(payload: Any) -> str:
    """对载荷做规范化哈希；JSON 类型一致即指纹一致（键序无关）。"""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


@dataclass
class IdempotencyPolicy:
    in_flight_timeout_seconds: float = 10.0
    poll_interval_seconds: float = 0.01
    fail_mode: IdemFailMode = IdemFailMode.CLOSED


class InMemoryIdempotencyStore:
    """线程安全内存幂等存储，支持故障注入。"""

    def __init__(self) -> None:
        self._records: dict[str, IdempotencyRecord] = {}
        self._lock = threading.RLock()
        self._failing = False

    def set_failing(self, failing: bool) -> None:
        with self._lock:
            self._failing = failing

    @property
    def failing(self) -> bool:
        return self._failing

    def reset(self) -> None:
        with self._lock:
            self._records.clear()
            self._failing = False

    def put_if_absent(self, record: IdempotencyRecord) -> IdempotencyRecord:
        with self._lock:
            if self._failing:
                raise DependencyError("idempotency store injected failure")
            existing = self._records.get(record.key)
            if existing is None:
                self._records[record.key] = record
                return record
            return existing

    def take_over_stale(
        self, key: str, new_record: IdempotencyRecord, timeout: float
    ) -> IdempotencyRecord:
        with self._lock:
            if self._failing:
                raise DependencyError("idempotency store injected failure")
            existing = self._records.get(key)
            if existing is None:
                self._records[key] = new_record
                return new_record
            if existing.status is RecordStatus.COMPLETED:
                return existing
            # 需比超时窗口再陈旧一个宽限才可接管：首个等待者先得到 IN_FLIGHT(409)，
            # 由客户端决定是否重试接管，避免“恰好超时”的竞态导致静默双执行。
            grace = 0.05
            if new_record.created_at - existing.created_at > timeout + grace:
                self._records[key] = new_record
                return new_record
            return existing

    def get(self, key: str) -> IdempotencyRecord | None:
        with self._lock:
            if self._failing:
                raise DependencyError("idempotency store injected failure")
            return self._records.get(key)

    def compare_and_complete(
        self, key: str, owner_token: str, result: Any, error: GovernanceError | None
    ) -> bool:
        with self._lock:
            if self._failing:
                raise DependencyError("idempotency store injected failure")
            existing = self._records.get(key)
            if existing is None or existing.owner_token != owner_token:
                return False
            self._records[key] = IdempotencyRecord(
                key=existing.key,
                tenant_id=existing.tenant_id,
                fingerprint=existing.fingerprint,
                status=RecordStatus.COMPLETED,
                result=result,
                error=error,
                owner_token=owner_token,
                created_at=existing.created_at,
                degraded=existing.degraded,
            )
            return True

    def delete_if_owner(self, key: str, owner_token: str) -> None:
        with self._lock:
            if self._failing:
                raise DependencyError("idempotency store injected failure")
            existing = self._records.get(key)
            if existing is not None and existing.owner_token == owner_token:
                del self._records[key]


class IdempotencyService:
    def __init__(
        self,
        store: IdempotencyStore,
        policy: IdempotencyPolicy,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._store = store
        self._policy = policy
        self._clock = clock or time.time
        # local_dedupe 降级路径的进程内表：key -> record
        self._local: dict[str, IdempotencyRecord] = {}
        self._local_lock = threading.RLock()

    @property
    def fail_mode(self) -> IdemFailMode:
        return self._policy.fail_mode

    @fail_mode.setter
    def fail_mode(self, mode: IdemFailMode) -> None:
        self._policy.fail_mode = mode

    # ---- 内部工具 ----
    def _check_existing(
        self, existing: IdempotencyRecord, fingerprint: str
    ) -> AcquireOutcome:
        if existing.fingerprint != fingerprint:
            return AcquireOutcome.CONFLICT
        if existing.status is RecordStatus.COMPLETED:
            return AcquireOutcome.REPLAY
        return AcquireOutcome.IN_FLIGHT

    def _new_inflight(self, key: str, principal: Principal, fingerprint: str,
                      *, degraded: bool) -> IdempotencyRecord:
        return IdempotencyRecord(
            key=key,
            tenant_id=principal.tenant_id,
            fingerprint=fingerprint,
            status=RecordStatus.IN_FLIGHT,
            owner_token=uuid.uuid4().hex,
            created_at=self._clock(),
            degraded=degraded,
        )

    # ---- 对外 API ----
    def acquire(self, key: str, principal: Principal, payload: Any) -> AcquireResult:
        fingerprint = fingerprint_payload(payload)
        try:
            record = self._new_inflight(key, principal, fingerprint, degraded=False)
            existing = self._store.put_if_absent(record)
        except DependencyError:
            return self._acquire_degraded(key, principal, fingerprint)

        if existing is record:
            log_decision("idem_acquire", tenant_id=principal.tenant_id,
                         idempotency_key=key, decision="first",
                         reason=f"fingerprint={fingerprint[:8]} owner={record.owner_token[:8]}")
            return AcquireResult(AcquireOutcome.FIRST, record)

        outcome = self._check_existing(existing, fingerprint)
        if outcome is AcquireOutcome.CONFLICT:
            log_decision("idem_acquire", tenant_id=principal.tenant_id,
                         idempotency_key=key, decision="conflict",
                         reason=f"payload fingerprint differs: {fingerprint[:8]} != {existing.fingerprint[:8]}")
            return AcquireResult(AcquireOutcome.CONFLICT, existing)
        if outcome is AcquireOutcome.REPLAY:
            log_decision("idem_acquire", tenant_id=principal.tenant_id,
                         idempotency_key=key, decision="replay",
                         reason="completed record exists with same fingerprint")
            return AcquireResult(AcquireOutcome.REPLAY, existing)

        # IN_FLIGHT：等待恰好一个超时窗口，期间完成则回放/冲突
        waited = self._wait_completion(key, fingerprint)
        if waited is not None:
            log_decision("idem_acquire", tenant_id=principal.tenant_id,
                         idempotency_key=key, decision=waited.outcome.value,
                         reason="waited for in-flight owner")
            return waited
        # 等待超时：尝试接管一次；接管不成（仍有人在执行）则让调用方稍后重试，
        # 不在请求内部循环等待，避免反复抢占形成活锁。
        takeover = self._try_takeover(key, principal, fingerprint)
        if takeover is not None:
            return takeover
        existing_now = self._safe_get(key)
        if existing_now is not None:
            outcome_now = self._check_existing(existing_now, fingerprint)
            if outcome_now is AcquireOutcome.REPLAY:
                return AcquireResult(AcquireOutcome.REPLAY, existing_now)
            if outcome_now is AcquireOutcome.CONFLICT:
                return AcquireResult(AcquireOutcome.CONFLICT, existing_now)
        log_decision("idem_acquire", tenant_id=principal.tenant_id,
                     idempotency_key=key, decision="in_flight",
                     reason="owner still in progress after wait window; caller may retry")
        return AcquireResult(AcquireOutcome.IN_FLIGHT, existing_now)

    def _safe_get(self, key: str) -> IdempotencyRecord | None:
        try:
            return self._store.get(key)
        except DependencyError:
            return None

    def _wait_completion(self, key: str, fingerprint: str) -> AcquireResult | None:
        deadline = self._clock() + self._policy.in_flight_timeout_seconds
        wall_deadline = time.monotonic() + max(self._policy.in_flight_timeout_seconds, 5.0)
        interval = self._policy.poll_interval_seconds
        while self._clock() < deadline and time.monotonic() < wall_deadline:
            time.sleep(interval)
            try:
                rec = self._store.get(key)
            except DependencyError:
                # 等待期间存储故障：交由降级路径处理
                return None
            if rec is None:
                return None
            if rec.fingerprint != fingerprint:
                return AcquireResult(AcquireOutcome.CONFLICT, rec)
            if rec.status is RecordStatus.COMPLETED:
                return AcquireResult(AcquireOutcome.REPLAY, rec)
        return None

    def _try_takeover(
        self, key: str, principal: Principal, fingerprint: str
    ) -> AcquireResult | None:
        record = self._new_inflight(key, principal, fingerprint, degraded=False)
        try:
            existing = self._store.take_over_stale(
                key, record, self._policy.in_flight_timeout_seconds
            )
        except DependencyError:
            return None
        if existing is record:
            log_decision("idem_acquire", tenant_id=principal.tenant_id,
                         idempotency_key=key, decision="first",
                         reason="took over stale in-flight record")
            return AcquireResult(AcquireOutcome.FIRST, record)
        outcome = self._check_existing(existing, fingerprint)
        if outcome is AcquireOutcome.REPLAY:
            return AcquireResult(AcquireOutcome.REPLAY, existing)
        if outcome is AcquireOutcome.CONFLICT:
            return AcquireResult(AcquireOutcome.CONFLICT, existing)
        return None

    def _acquire_degraded(
        self, key: str, principal: Principal, fingerprint: str
    ) -> AcquireResult:
        if self._policy.fail_mode is IdemFailMode.CLOSED:
            log_decision("idem_acquire", tenant_id=principal.tenant_id,
                         idempotency_key=key, decision="degraded_reject",
                         reason="store unavailable; fail_mode=closed")
            raise GovernanceError(
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "idempotency subsystem unavailable; write rejected by fail-closed policy",
                reason="fail_mode=closed",
            )

        with self._local_lock:
            local = self._local.get(key)
            if local is None:
                record = self._new_inflight(key, principal, fingerprint, degraded=True)
                self._local[key] = record
                log_decision("idem_acquire", tenant_id=principal.tenant_id,
                             idempotency_key=key, decision="degraded_local_first",
                             reason="store unavailable; dedupe within this process only")
                return AcquireResult(AcquireOutcome.DEGRADED_LOCAL_FIRST, record)
            if local.fingerprint != fingerprint:
                log_decision("idem_acquire", tenant_id=principal.tenant_id,
                             idempotency_key=key, decision="conflict",
                             reason="local degraded record has different fingerprint")
                return AcquireResult(AcquireOutcome.CONFLICT, local)
            in_flight = local.status is RecordStatus.IN_FLIGHT

        # 等待必须在锁外进行：否则首请求 complete() 无法获取锁，形成死锁
        if in_flight:
            self._wait_local_completion(key)
            with self._local_lock:
                local = self._local[key]
        outcome = (
            AcquireOutcome.DEGRADED_LOCAL_REPLAY
            if local.status is RecordStatus.COMPLETED
            else AcquireOutcome.IN_FLIGHT
        )
        log_decision("idem_acquire", tenant_id=principal.tenant_id,
                     idempotency_key=key, decision=outcome.value,
                     reason="local dedupe replay while store unavailable")
        return AcquireResult(outcome, local)

    def _wait_local_completion(self, key: str) -> None:
        deadline = self._clock() + self._policy.in_flight_timeout_seconds
        wall_deadline = time.monotonic() + max(self._policy.in_flight_timeout_seconds, 5.0)
        while self._clock() < deadline and time.monotonic() < wall_deadline:
            time.sleep(self._policy.poll_interval_seconds)
            with self._local_lock:
                rec = self._local.get(key)
            if rec is None or rec.status is RecordStatus.COMPLETED:
                return

    def complete(
        self, key: str, owner_token: str, result: Any,
        error: GovernanceError | None = None,
    ) -> bool:
        """以 compare-and-swap 发布结论；返回远端记录是否由本 owner 成功更新。"""
        local = self._local.get(key)
        if local is not None and local.owner_token == owner_token:
            with self._local_lock:
                rec = self._local[key]
                self._local[key] = IdempotencyRecord(
                    key=rec.key, tenant_id=rec.tenant_id, fingerprint=rec.fingerprint,
                    status=RecordStatus.COMPLETED, result=result, error=error,
                    owner_token=owner_token, created_at=rec.created_at, degraded=True,
                )
        try:
            return self._store.compare_and_complete(key, owner_token, result, error)
        except DependencyError:
            log_decision("idem_complete", idempotency_key=key,
                         decision="store_update_failed",
                         reason="outcome kept in local state; store masked")
            return False

    def abandon(self, key: str, owner_token: str) -> None:
        with self._local_lock:
            rec = self._local.get(key)
            if rec is not None and rec.owner_token == owner_token:
                del self._local[key]
        try:
            self._store.delete_if_owner(key, owner_token)
        except DependencyError:
            log_decision("idem_abandon", idempotency_key=key,
                         decision="store_delete_failed", reason="masked; record may be taken over after timeout")
