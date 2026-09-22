"""写请求幂等：同一幂等键只产生一次业务副作用。

保证
----
1. 同一 ``idempotency_key`` 的并发/重复提交，业务动作 ``action`` 只执行
   一次；等待方拿到与首请求 *完全相同* 的结论。
2. 同一幂等键携带语义不同的载荷（规范化后哈希不同）立即拒绝
   （IDEMPOTENCY_CONFLICT），不会静默以先到者为准。
3. 首请求业务失败时释放占位，允许调用方用同键重试（不产生半成品状态）。
4. 存储故障映射为 DEPENDENCY_UNAVAILABLE，底层异常不透出。
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Protocol

from governance.errors import DenyReason, GovernanceError

logger = logging.getLogger("governance.idempotency")


class StoreUnavailable(RuntimeError):
    """底层幂等存储不可用，仅在治理层内部流转。"""


class IdemStatus(str, Enum):
    INFLIGHT = "inflight"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class IdemRecord:
    key: str
    tenant_id: str
    payload_hash: str
    status: IdemStatus
    result: Any = None


def stable_payload_hash(payload: Any) -> str:
    """对载荷做规范化后取 SHA-256，保证语义相同的载荷哈希一致。"""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class IdempotencyStore(Protocol):
    """幂等记录存储协议。"""

    def acquire(
        self, key: str, tenant_id: str, payload_hash: str
    ) -> tuple[IdemRecord, bool]:
        """尝试占位，返回 ``(记录, 是否为首个占位者)``。"""
        ...

    def complete(self, key: str, result: Any) -> None: ...

    def fail(self, key: str) -> None: ...

    def release(self, key: str) -> None: ...

    def get(self, key: str) -> IdemRecord | None: ...


class InMemoryIdempotencyStore:
    """线程安全的内存幂等存储。

    acquire 只在锁内做状态机切换并立即释放锁；首请求执行业务期间不持锁，
    等待方在该键的 Condition 上阻塞，完成时被唤醒并读取同一结论。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._conditions: dict[str, threading.Condition] = {}
        self._records: dict[str, IdemRecord] = {}

    def _cond(self, key: str) -> threading.Condition:
        cv = self._conditions.get(key)
        if cv is None:
            cv = threading.Condition(self._lock)
            self._conditions[key] = cv
        return cv

    def acquire(
        self, key: str, tenant_id: str, payload_hash: str
    ) -> tuple[IdemRecord, bool]:
        with self._lock:
            cv = self._cond(key)
            existing = self._records.get(key)
            if existing is None:
                record = IdemRecord(
                    key=key, tenant_id=tenant_id,
                    payload_hash=payload_hash, status=IdemStatus.INFLIGHT,
                )
                self._records[key] = record
                return record, True
            if existing.payload_hash != payload_hash:
                raise GovernanceError(
                    DenyReason.IDEMPOTENCY_CONFLICT,
                    f"幂等键 {key!r} 已携带不同载荷提交，拒绝语义冲突的请求",
                )
            return existing, False

    def complete(self, key: str, result: Any) -> None:
        with self._lock:
            record = self._records[key]
            record.status = IdemStatus.COMPLETED
            record.result = result
            self._cond(key).notify_all()

    def fail(self, key: str) -> None:
        with self._lock:
            self._records[key].status = IdemStatus.FAILED
            self._cond(key).notify_all()

    def release(self, key: str) -> None:
        with self._lock:
            self._records.pop(key, None)
            self._cond(key).notify_all()

    def get(self, key: str) -> IdemRecord | None:
        with self._lock:
            return self._records.get(key)

    def wait_until_settled(
        self, key: str, timeout: float | None = None
    ) -> IdemRecord:
        """等待同键首请求落定（完成/失败）。"""
        with self._lock:
            cv = self._cond(key)
            record = self._records[key]
            while record.status is IdemStatus.INFLIGHT:
                if not cv.wait(timeout=timeout):
                    raise StoreUnavailable(
                        f"等待幂等键 {key!r} 的首请求落定超时"
                    )
            return record


class FailingIdempotencyStore:
    """测试用：acquire 即故障的存储。"""

    def acquire(self, key, tenant_id, payload_hash):
        raise StoreUnavailable("idempotency backend unavailable")

    def complete(self, key, result): ...
    def fail(self, key): ...
    def release(self, key): ...
    def get(self, key):
        raise StoreUnavailable("idempotency backend unavailable")


class IdempotencyService:
    """幂等执行器：副作用一次、结论一致、冲突明确拒绝。"""

    def __init__(
        self,
        store: IdempotencyStore,
        wait_timeout: float = 10.0,
    ) -> None:
        self._store = store
        self._wait_timeout = wait_timeout

    def execute(
        self,
        key: str | None,
        tenant_id: str,
        payload: Any,
        action: Callable[[], Any],
    ) -> Any:
        # 未携带幂等键：按普通请求直接执行，不纳入幂等保护。
        if not key:
            logger.info("idempotency_key=<none> tenant=%s decision=execute "
                        "basis=no_idempotency_key", tenant_id)
            return action()

        payload_hash = stable_payload_hash(payload)
        try:
            record, is_leader = self._store.acquire(key, tenant_id, payload_hash)
        except GovernanceError:
            logger.info("idempotency_key=%s tenant=%s decision=deny "
                        "basis=payload_conflict", key, tenant_id)
            raise
        except StoreUnavailable as exc:
            logger.warning("idempotency_key=%s tenant=%s decision=deny "
                           "basis=idempotency_store_unavailable error=%s",
                           key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "幂等服务暂不可用，已拒绝该写请求以避免重复副作用",
            ) from exc

        if is_leader:
            return self._run_as_leader(key, tenant_id, action)
        return self._run_as_follower(key, tenant_id, record)

    def _run_as_leader(
        self, key: str, tenant_id: str, action: Callable[[], Any]
    ) -> Any:
        logger.info("idempotency_key=%s tenant=%s decision=execute "
                    "basis=first_submission", key, tenant_id)
        try:
            result = action()
        except Exception:
            # 业务失败：标记并释放占位，允许调用方安全重试。
            try:
                self._store.fail(key)
                self._store.release(key)
            except StoreUnavailable as exc:
                logger.error("idempotency_key=%s tenant=%s release_failed error=%s",
                             key, tenant_id, exc)
            logger.info("idempotency_key=%s tenant=%s decision=release "
                        "basis=business_failed", key, tenant_id)
            raise
        try:
            self._store.complete(key, result)
        except StoreUnavailable as exc:
            # 副作用已发生但无法落定：不能重试（否则会重复副作用）。
            logger.error("idempotency_key=%s tenant=%s complete_failed error=%s",
                         key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "请求已处理但结果暂时无法确认，请稍后凭同一幂等键查询",
            ) from exc
        logger.info("idempotency_key=%s tenant=%s decision=completed "
                    "basis=side_effect_committed", key, tenant_id)
        return result

    def _run_as_follower(
        self, key: str, tenant_id: str, record: IdemRecord
    ) -> Any:
        if record.status is IdemStatus.COMPLETED:
            logger.info("idempotency_key=%s tenant=%s decision=replay "
                        "basis=duplicate_after_completion", key, tenant_id)
            return record.result
        try:
            settled = self._store.wait_until_settled(key, self._wait_timeout)
        except StoreUnavailable as exc:
            logger.warning("idempotency_key=%s tenant=%s decision=deny "
                           "basis=wait_settle_unavailable error=%s",
                           key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "重复请求的处理结果暂不可确认，请稍后凭同一幂等键重试",
            ) from exc
        if settled.status is IdemStatus.FAILED:
            # 首请求业务失败且占位可能已释放：当前跟随者不重试，
            # 返回明确的依赖/处理不可确认错误，由调用方安全重试。
            logger.info("idempotency_key=%s tenant=%s decision=deny "
                        "basis=leader_failed_retry_allowed", key, tenant_id)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "首次提交执行失败，请凭同一幂等键重试",
            )
        logger.info("idempotency_key=%s tenant=%s decision=replay "
                    "basis=concurrent_duplicate", key, tenant_id)
        return settled.result
