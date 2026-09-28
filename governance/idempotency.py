"""写请求幂等：同一提交标识只产生一次业务副作用，重试结论一致。

保证
----
1. 同一 ``idempotency_key`` 的并发/重复/断线重连提交，业务动作只执行
   一次；跟随方拿到与首请求 *完全相同* 的结论，且不再占用配额。
2. 同一键携带语义不同的载荷（规范化哈希不同）立即拒绝
   （IDEMPOTENCY_CONFLICT），不会静默以先到者为准，也不会变成回放。
3. 已完成结论按 TTL 与容量上限保留；超期或被 LRU 淘汰后，同键再次提交
   得到明确的 RESULT_EXPIRED 失败（墓碑保证不会静默二次执行），
   调用方需换新标识。
4. 副作用已发生但结论无法落定时进入 UNVERIFIABLE，只允许查询确认，
   绝不重放为新请求再执行一次。
5. 查询按 (租户, 键) 隔离，只读、不占配额、不产生副作用。
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Protocol

from governance.errors import DenyReason, GovernanceError
from governance.policy import RetentionConfig

logger = logging.getLogger("governance.idempotency")


class StoreUnavailable(RuntimeError):
    """底层幂等存储不可用，仅在治理层内部流转。"""


class IdemStatus(str, Enum):
    INFLIGHT = "inflight"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"            # 超期/容量淘汰后的墓碑
    UNVERIFIABLE = "unverifiable"  # 副作用可能已发生但结论无法确认


class SubmissionStatus(str, Enum):
    """只读查询返回的可区分状态。"""

    PROCESSING = "processing"               # 首请求仍在处理中
    COMPLETED = "completed"                 # 已完成，附带结论
    FAILED = "failed"                       # 首请求失败，可用同键重试
    EXPIRED_OR_UNKNOWN = "expired_or_unknown"  # 已过期/被淘汰/从未见过
    UNVERIFIABLE = "unverifiable"           # 副作用可能已发生，结论暂不可确认


@dataclass(frozen=True)
class SubmissionView:
    """提交结论的只读视图。"""

    status: SubmissionStatus
    result: Any = None


@dataclass
class IdemRecord:
    key: str
    tenant_id: str
    payload_hash: str
    status: IdemStatus
    result: Any = None
    last_accessed: float = 0.0
    completed_at: float | None = None
    # 首请求失败（配额拒绝/依赖拒绝/业务异常）时保存原始异常，
    # 供并发等待方拿到与首次一致的结论；同键重试时原子回收为新占位。
    error: BaseException | None = None


def stable_payload_hash(payload: Any) -> str:
    """对载荷做规范化后取 SHA-256，保证语义相同的载荷哈希一致。"""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _retention_ttl(retention: RetentionConfig | None) -> float:
    return retention.ttl_seconds if retention else 3600.0


def _retention_cap(retention: RetentionConfig | None) -> int:
    return retention.max_completed if retention else 10_000


class IdempotencyStore(Protocol):
    """幂等记录存储协议（键空间按租户隔离）。"""

    def acquire(
        self,
        tenant_id: str,
        key: str,
        payload_hash: str,
        retention: RetentionConfig | None = None,
    ) -> tuple[IdemRecord, bool]:
        """尝试占位，返回 ``(记录, 是否为首个占位者)``。"""
        ...

    def complete(
        self,
        tenant_id: str,
        key: str,
        result: Any,
        retention: RetentionConfig | None = None,
    ) -> None: ...

    def fail(
        self,
        tenant_id: str,
        key: str,
        error: BaseException,
        retention: RetentionConfig | None = None,
    ) -> None:
        """把首请求落定为失败终态并携带原始原因，唤醒全部等待方。"""
        ...

    def reclaim(
        self,
        tenant_id: str,
        key: str,
        payload_hash: str,
    ) -> tuple[IdemRecord, bool]:
        """同键重试：在锁内把失败终态原子回收为新的 INFLIGHT 占位。

        返回 ``(记录, 是否成为新的首请求)``；记录当前不是失败终态时
        返回 ``(现有记录, False)``（例如已被并发等待方抢先回收）。
        """
        ...

    def release(self, tenant_id: str, key: str) -> None: ...

    def mark_unverifiable(
        self,
        tenant_id: str,
        key: str,
        retention: RetentionConfig | None = None,
    ) -> None: ...

    def lookup(
        self,
        tenant_id: str,
        key: str,
        retention: RetentionConfig | None = None,
    ) -> IdemRecord | None:
        """只读查询；从未存在返回 None，过期/淘汰返回 EXPIRED 墓碑。"""
        ...

    def wait_until_settled(
        self,
        tenant_id: str,
        key: str,
        timeout: float | None = None,
    ) -> IdemRecord: ...


class InMemoryIdempotencyStore:
    """线程安全的内存幂等存储，时钟与保留策略可注入。

    - 内部以 ``(租户, 键)`` 为存储键，天然按租户隔离；
    - acquire 只在锁内做状态机切换；首请求执行业务期间不持锁，
      跟随方在该键的 Condition 上阻塞；
    - 已完成记录受 TTL 与容量上限保护：超期翻成 EXPIRED 墓碑，
      超容量按最久未访问（LRU）淘汰为墓碑；INFLIGHT 与 UNVERIFIABLE
      记录永不淘汰，刚被访问的完成记录也不会被 LRU 选中。
    """

    def __init__(
        self,
        clock: Callable[[], float] | None = None,
        retention: RetentionConfig | None = None,
    ) -> None:
        self._clock = clock or time.time
        self._retention = retention
        self._lock = threading.Lock()
        self._conditions: dict[tuple[str, str], threading.Condition] = {}
        self._records: dict[tuple[str, str], IdemRecord] = {}

    # 测试/装配辅助：热更新保留策略的默认值。
    def set_retention(self, retention: RetentionConfig | None) -> None:
        self._retention = retention

    def _rk(self, tenant_id: str, key: str) -> tuple[str, str]:
        return tenant_id, key

    def _cond(self, rk: tuple[str, str]) -> threading.Condition:
        cv = self._conditions.get(rk)
        if cv is None:
            cv = threading.Condition(self._lock)
            self._conditions[rk] = cv
        return cv

    def _is_expired(self, record: IdemRecord, now: float,
                    retention: RetentionConfig | None) -> bool:
        ttl = _retention_ttl(retention)
        if ttl <= 0 or record.completed_at is None:
            return False
        return now - record.completed_at > ttl

    def _expire_if_needed(
        self, record: IdemRecord, now: float,
        retention: RetentionConfig | None,
    ) -> bool:
        """完成记录超 TTL 则翻成墓碑，返回是否已过期。"""
        if (record.status is IdemStatus.COMPLETED
                and self._is_expired(record, now, retention)):
            record.status = IdemStatus.EXPIRED
            record.result = None
            return True
        return False

    def _evict_if_over_capacity(
        self, now: float, retention: RetentionConfig | None
    ) -> None:
        """已完成记录超过容量上限时，按 LRU 淘汰最久未访问者为墓碑。"""
        cap = _retention_cap(retention)
        if cap <= 0:
            return
        completed = [r for r in self._records.values()
                     if r.status is IdemStatus.COMPLETED]
        while len(completed) > cap:
            victim = min(completed, key=lambda r: r.last_accessed)
            victim.status = IdemStatus.EXPIRED
            victim.result = None
            completed.remove(victim)
            logger.info(
                "idempotency_key=%s tenant=%s decision=evict "
                "basis=lru_capacity_exceeded",
                victim.key, victim.tenant_id,
            )

    def acquire(
        self,
        tenant_id: str,
        key: str,
        payload_hash: str,
        retention: RetentionConfig | None = None,
    ) -> tuple[IdemRecord, bool]:
        retention = retention if retention is not None else self._retention
        rk = self._rk(tenant_id, key)
        with self._lock:
            now = self._clock()
            existing = self._records.get(rk)
            if existing is None:
                record = IdemRecord(
                    key=key, tenant_id=tenant_id,
                    payload_hash=payload_hash, status=IdemStatus.INFLIGHT,
                    last_accessed=now,
                )
                self._records[rk] = record
                return record, True

            if existing.status is IdemStatus.EXPIRED:
                logger.info(
                    "idempotency_key=%s tenant=%s decision=deny "
                    "basis=expired_tombstone", key, tenant_id,
                )
                raise GovernanceError(
                    DenyReason.RESULT_EXPIRED,
                    f"提交标识 {key!r} 的结论已过期或曾被淘汰，"
                    "请更换新的提交标识重新发起",
                )
            if existing.status is IdemStatus.UNVERIFIABLE:
                logger.info(
                    "idempotency_key=%s tenant=%s decision=deny "
                    "basis=unverifiable_side_effect", key, tenant_id,
                )
                raise GovernanceError(
                    DenyReason.RESULT_UNVERIFIABLE,
                    f"提交标识 {key!r} 的副作用可能已发生但结论暂不可确认，"
                    "请凭同一标识查询确认，切勿更换载荷重试",
                )
            if existing.payload_hash != payload_hash:
                logger.info(
                    "idempotency_key=%s tenant=%s decision=deny "
                    "basis=payload_conflict", key, tenant_id,
                )
                raise GovernanceError(
                    DenyReason.IDEMPOTENCY_CONFLICT,
                    f"提交标识 {key!r} 已携带不同载荷提交，拒绝语义冲突的请求",
                )
            if existing.status is IdemStatus.COMPLETED:
                if self._expire_if_needed(existing, now, retention):
                    logger.info(
                        "idempotency_key=%s tenant=%s decision=deny "
                        "basis=ttl_expired", key, tenant_id,
                    )
                    raise GovernanceError(
                        DenyReason.RESULT_EXPIRED,
                        f"提交标识 {key!r} 的结论已超过保留时长，"
                        "请更换新的提交标识重新发起",
                    )
                existing.last_accessed = now
            # FAILED 是失败终态：跟随方读到后回放首次失败原因；
            # 是否重试由后续显式 reclaim 在锁内原子决定。
            return existing, False

    def reclaim(
        self,
        tenant_id: str,
        key: str,
        payload_hash: str,
    ) -> tuple[IdemRecord, bool]:
        rk = self._rk(tenant_id, key)
        with self._lock:
            existing = self._records.get(rk)
            if (existing is None
                    or existing.status is not IdemStatus.FAILED
                    or existing.payload_hash != payload_hash):
                return existing, False  # type: ignore[return-value]
            now = self._clock()
            existing.status = IdemStatus.INFLIGHT
            existing.result = None
            existing.error = None
            existing.last_accessed = now
            existing.completed_at = None
            return existing, True

    def fail(
        self,
        tenant_id: str,
        key: str,
        error: BaseException,
        retention: RetentionConfig | None = None,
    ) -> None:
        rk = self._rk(tenant_id, key)
        with self._lock:
            record = self._records.get(rk)
            if record is not None:
                record.status = IdemStatus.FAILED
                record.result = None
                record.error = error
                record.completed_at = None
            self._cond(rk).notify_all()

    def complete(
        self,
        tenant_id: str,
        key: str,
        result: Any,
        retention: RetentionConfig | None = None,
    ) -> None:
        retention = retention if retention is not None else self._retention
        rk = self._rk(tenant_id, key)
        with self._lock:
            now = self._clock()
            record = self._records[rk]
            record.status = IdemStatus.COMPLETED
            record.result = result
            record.last_accessed = now
            record.completed_at = now
            self._evict_if_over_capacity(now, retention)
            self._cond(rk).notify_all()

    def release(self, tenant_id: str, key: str) -> None:
        rk = self._rk(tenant_id, key)
        with self._lock:
            self._records.pop(rk, None)
            self._cond(rk).notify_all()

    def mark_unverifiable(
        self,
        tenant_id: str,
        key: str,
        retention: RetentionConfig | None = None,
    ) -> None:
        retention = retention if retention is not None else self._retention
        rk = self._rk(tenant_id, key)
        with self._lock:
            record = self._records.get(rk)
            if record is None:
                # 连占位都无法写回：用最小墓碑占位，阻止同键被当成新请求。
                record = IdemRecord(
                    key=key, tenant_id=tenant_id, payload_hash="",
                    status=IdemStatus.UNVERIFIABLE,
                )
                self._records[rk] = record
            else:
                record.status = IdemStatus.UNVERIFIABLE
                record.result = None
            self._cond(rk).notify_all()

    def lookup(
        self,
        tenant_id: str,
        key: str,
        retention: RetentionConfig | None = None,
    ) -> IdemRecord | None:
        retention = retention if retention is not None else self._retention
        rk = self._rk(tenant_id, key)
        with self._lock:
            record = self._records.get(rk)
            if record is None:
                return None
            if record.status is IdemStatus.COMPLETED:
                now = self._clock()
                if self._expire_if_needed(record, now, retention):
                    return record
                record.last_accessed = now
            return record

    def wait_until_settled(
        self,
        tenant_id: str,
        key: str,
        timeout: float | None = None,
    ) -> IdemRecord:
        """等待同键首请求落定（完成/失败/不可确认）。"""
        rk = self._rk(tenant_id, key)
        with self._lock:
            cv = self._cond(rk)
            record = self._records[rk]
            while record.status is IdemStatus.INFLIGHT:
                if not cv.wait(timeout=timeout):
                    raise StoreUnavailable(
                        f"等待提交标识 {key!r} 的首请求落定超时"
                    )
            return record


class FailingIdempotencyStore:
    """测试用：acquire 即故障的存储。"""

    def acquire(self, tenant_id, key, payload_hash, retention=None):
        raise StoreUnavailable("idempotency backend unavailable")

    def complete(self, tenant_id, key, result, retention=None):
        raise StoreUnavailable("idempotency backend unavailable")

    def fail(self, tenant_id, key, error, retention=None):
        raise StoreUnavailable("idempotency backend unavailable")

    def reclaim(self, tenant_id, key, payload_hash):
        raise StoreUnavailable("idempotency backend unavailable")

    def release(self, tenant_id, key):
        raise StoreUnavailable("idempotency backend unavailable")

    def mark_unverifiable(self, tenant_id, key, retention=None):
        raise StoreUnavailable("idempotency backend unavailable")

    def lookup(self, tenant_id, key, retention=None):
        raise StoreUnavailable("idempotency backend unavailable")

    def wait_until_settled(self, tenant_id, key, timeout=None):
        raise StoreUnavailable("idempotency backend unavailable")


class CompleteFailingStore(InMemoryIdempotencyStore):
    """测试用：占位成功、业务已执行，但 complete 落定失败的存储。

    模拟"副作用已经发生、结论却无法确认"的窗口：mark_unverifiable 仍可写回，
    从而允许调用方稍后凭同一键查询到 unverifiable，而不是重新执行。
    """

    def complete(self, tenant_id, key, result, retention=None):
        raise StoreUnavailable("idempotency commit failed")


class IdempotencyService:
    """幂等执行器：副作用一次、结论一致、冲突/过期明确拒绝、可查询。"""

    def __init__(
        self,
        store: IdempotencyStore,
        wait_timeout: float = 10.0,
        retention_provider: Callable[[], RetentionConfig | None] | None = None,
    ) -> None:
        self._store = store
        self._wait_timeout = wait_timeout
        # 每次执行读取一次保留策略，与策略注册表的快照语义保持一致。
        self._retention_provider = retention_provider or (lambda: None)

    def execute(
        self,
        key: str | None,
        tenant_id: str,
        payload: Any,
        action: Callable[[], Any],
        consume_quota: Callable[[], None] | None = None,
    ) -> Any:
        # 未携带提交标识：按普通请求直接执行（是否允许由策略层先裁决）。
        if not key:
            logger.info("idempotency_key=<none> tenant=%s decision=execute "
                        "basis=no_idempotency_key", tenant_id)
            if consume_quota is not None:
                consume_quota()
            return action()

        payload_hash = stable_payload_hash(payload)

        # 失败终态的同键重试：在存储锁内原子回收占位。
        # 回收成功 -> 成为新的首请求；失败（已被并发等待方抢先回收，或
        # 状态已变化）-> 退回到 acquire 的正常判定路径。
        try:
            reclaimed, became_leader = self._store.reclaim(
                tenant_id, key, payload_hash
            )
        except GovernanceError:
            raise
        except StoreUnavailable as exc:
            logger.warning("idempotency_key=%s tenant=%s decision=deny "
                           "basis=idempotency_store_unavailable error=%s",
                           key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "幂等服务暂不可用，已拒绝该写请求以避免重复副作用",
            ) from exc
        if became_leader:
            logger.info("idempotency_key=%s tenant=%s decision=execute "
                        "basis=retry_after_failure", key, tenant_id)
            return self._run_as_leader(
                key, tenant_id, action, consume_quota,
                self._retention_provider(),
            )

        retention = self._retention_provider()
        try:
            record, is_leader = self._store.acquire(
                tenant_id, key, payload_hash, retention
            )
        except GovernanceError:
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
            # 只有真正的首请求占用配额；跟随方（重复/并发/断线重连）
            # 一律不再经过配额闸门，因此额度耗尽或跨窗口都能回放首结论。
            return self._run_as_leader(
                key, tenant_id, action, consume_quota, retention
            )
        return self._run_as_follower(key, tenant_id, record, retention)

    def _run_as_leader(
        self,
        key: str,
        tenant_id: str,
        action: Callable[[], Any],
        consume_quota: Callable[[], None] | None,
        retention: RetentionConfig | None,
    ) -> Any:
        logger.info("idempotency_key=%s tenant=%s decision=execute "
                    "basis=first_submission", key, tenant_id)
        if consume_quota is not None:
            try:
                consume_quota()
            except GovernanceError as exc:
                # 首请求未过配额闸门：落定失败终态并携带原始拒绝原因，
                # 让并发等待方拿到与首次一致的 quota_exhausted/503，
                # 不产生业务副作用；调用方随后可用同一标识重试。
                self._settle_failure(key, tenant_id, exc, "quota_denied")
                raise
        try:
            result = action()
        except Exception as exc:
            # 业务失败：落定失败终态，等待方看到一致失败；同键可安全重试
            # （reclaim 在锁内原子回收，不会出现重复副作用窗口）。
            self._settle_failure(key, tenant_id, exc, "business_failed")
            raise
        try:
            self._store.complete(tenant_id, key, result, retention)
        except StoreUnavailable as exc:
            # 副作用已发生但无法落定：标记 UNVERIFIABLE，绝不允许同键再执行。
            logger.error("idempotency_key=%s tenant=%s complete_failed error=%s",
                         key, tenant_id, exc)
            try:
                self._store.mark_unverifiable(tenant_id, key, retention)
            except StoreUnavailable as mark_exc:
                logger.error("idempotency_key=%s tenant=%s "
                             "mark_unverifiable_failed error=%s",
                             key, tenant_id, mark_exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "请求可能已处理但结果暂时无法确认，请稍后凭同一提交标识查询",
            ) from exc
        logger.info("idempotency_key=%s tenant=%s decision=completed "
                    "basis=side_effect_committed", key, tenant_id)
        return result

    def _settle_failure(
        self,
        key: str,
        tenant_id: str,
        error: BaseException,
        basis: str,
    ) -> None:
        try:
            self._store.fail(tenant_id, key, error)
        except StoreUnavailable as exc:
            # 落定失败本身无法确认时，转为不可确认，绝不允许同键静默重放。
            logger.error("idempotency_key=%s tenant=%s fail_settle_failed "
                         "error=%s", key, tenant_id, exc)
            try:
                self._store.mark_unverifiable(tenant_id, key)
            except StoreUnavailable as mark_exc:
                logger.error("idempotency_key=%s tenant=%s "
                             "mark_unverifiable_failed error=%s",
                             key, tenant_id, mark_exc)
        logger.info("idempotency_key=%s tenant=%s decision=failed basis=%s",
                    key, tenant_id, basis)

    def _run_as_follower(
        self,
        key: str,
        tenant_id: str,
        record: IdemRecord,
        retention: RetentionConfig | None,
    ) -> Any:
        if record.status is IdemStatus.COMPLETED:
            logger.info("idempotency_key=%s tenant=%s decision=replay "
                        "basis=duplicate_after_completion", key, tenant_id)
            return record.result
        if record.status is IdemStatus.FAILED and record.error is not None:
            logger.info("idempotency_key=%s tenant=%s decision=replay "
                        "basis=duplicate_after_failure", key, tenant_id)
            self._raise_record_error(record)
        try:
            settled = self._store.wait_until_settled(
                tenant_id, key, self._wait_timeout
            )
        except StoreUnavailable as exc:
            logger.warning("idempotency_key=%s tenant=%s decision=deny "
                           "basis=wait_settle_unavailable error=%s",
                           key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "重复请求的处理结果暂不可确认，请稍后凭同一提交标识查询",
            ) from exc
        if settled.status is IdemStatus.COMPLETED:
            logger.info("idempotency_key=%s tenant=%s decision=replay "
                        "basis=concurrent_duplicate", key, tenant_id)
            return settled.result
        if settled.status is IdemStatus.FAILED:
            # 首请求失败：回放与首次一致的失败结论，而不是笼统 503。
            # 配额类治理拒绝直接重抛同一原因；业务异常等其他失败给出
            # 可区分、可重试的说明。
            logger.info("idempotency_key=%s tenant=%s decision=replay "
                        "basis=concurrent_failure", key, tenant_id)
            self._raise_record_error(settled)
        if settled.status is IdemStatus.UNVERIFIABLE:
            logger.info("idempotency_key=%s tenant=%s decision=deny "
                        "basis=unverifiable_side_effect", key, tenant_id)
            raise GovernanceError(
                DenyReason.RESULT_UNVERIFIABLE,
                "请求可能已处理但结果暂时无法确认，请凭同一提交标识查询",
            )
        # EXPIRED 不会出现在等待落定路径，但仍明确拒绝而非静默再执行。
        raise GovernanceError(
            DenyReason.RESULT_EXPIRED,
            f"提交标识 {key!r} 的结论已过期，请更换新标识重新发起",
        )

    @staticmethod
    def _raise_record_error(record: IdemRecord) -> None:
        error = record.error
        if isinstance(error, GovernanceError):
            # 与首次结论完全一致的治理拒绝（如 quota_exhausted、503）。
            raise error
        raise GovernanceError(
            DenyReason.DEPENDENCY_UNAVAILABLE,
            "首次提交执行失败，请凭同一提交标识重试",
        ) from error

    def query(
        self,
        key: str,
        tenant_id: str,
    ) -> SubmissionView:
        """按 (租户, 键) 只读查询结论：不占配额、不产生副作用。"""
        retention = self._retention_provider()
        try:
            record = self._store.lookup(tenant_id, key, retention)
        except StoreUnavailable as exc:
            logger.warning("idempotency_key=%s tenant=%s query=deny "
                           "basis=lookup_unavailable error=%s",
                           key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "结论存储暂不可用，请稍后凭同一提交标识重试查询",
            ) from exc
        if record is None:
            logger.info("idempotency_key=%s tenant=%s query=not_found",
                        key, tenant_id)
            return SubmissionView(SubmissionStatus.EXPIRED_OR_UNKNOWN)
        mapping = {
            IdemStatus.INFLIGHT: SubmissionStatus.PROCESSING,
            IdemStatus.COMPLETED: SubmissionStatus.COMPLETED,
            IdemStatus.FAILED: SubmissionStatus.FAILED,
            IdemStatus.EXPIRED: SubmissionStatus.EXPIRED_OR_UNKNOWN,
            IdemStatus.UNVERIFIABLE: SubmissionStatus.UNVERIFIABLE,
        }
        status = mapping.get(record.status, SubmissionStatus.EXPIRED_OR_UNKNOWN)
        result = record.result if status is SubmissionStatus.COMPLETED else None
        logger.info("idempotency_key=%s tenant=%s query=%s",
                    key, tenant_id, status.value)
        return SubmissionView(status, result)
