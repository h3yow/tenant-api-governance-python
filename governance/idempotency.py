"""写请求幂等与“对客户端重试友好”的结算层。

保证
----
1. 同一提交标识（``idempotency_key``，按租户隔离）的 *首次* 请求落定后，
   后续重复提交、并发重试、断线重连一律回放首次结论：业务动作只执行一次，
   且 **重复请求不占配额**——即使额度已耗尽或重试落在新时间窗口，也返回
   首次结论。只有真正第一次出现的写请求才占用额度（由 leader 占位后调用
   ``reserve_quota``）。全新的提交标识在额度耗尽时仍照常被配额拒绝。
2. 同键携带语义不同的载荷（规范化哈希不同）立即拒绝
   （IDEMPOTENCY_CONFLICT），不会静默回放。
3. 结论保留有时长与容量上限：超期或被 LRU 淘汰的旧标识进入“墓碑”，
   再被提交时明确返回 RESULT_EXPIRED（要求换新标识），绝不静默当新请求
   执行第二遍。处理中、正在被等待或刚被访问的结论不会被淘汰；同一标识
   在任何时刻最多对应一份结论。
4. 依赖恢复后可凭提交标识只读查询（不产生副作用、不占额度），区分：
   处理中 / 已完成 / 已过期或不存在 / 副作用已发生但结论暂不可确认。
5. 存储故障映射为 DEPENDENCY_UNAVAILABLE，底层异常不透出；放行降级
   （FAIL_OPEN）只放开配额闸门，结算闸门永远保守，绝不放过重复副作用。
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Protocol

from governance.config import RetentionConfig, WritePolicy
from governance.errors import DenyReason, GovernanceError

logger = logging.getLogger("governance.idempotency")


class StoreUnavailable(RuntimeError):
    """底层结算存储不可用，仅在治理层内部流转。"""


class IdemStatus(str, Enum):
    INFLIGHT = "inflight"          # 首请求处理中
    COMPLETED = "completed"        # 副作用已落定，结论可回放
    FAILED = "failed"              # 首请求业务失败（占位随即释放，允许重试）
    UNCERTAIN = "uncertain"        # 副作用可能已发生，但结论无法确认


class AcquireOutcome(str, Enum):
    LEADER = "leader"              # 首个占位者：负责占额度 + 执行业务
    REPLAY = "replay"              # 已有已完成结论：直接回放，不占额度
    INFLIGHT = "inflight"          # 首请求处理中：等待其落定，不占额度


class QueryStatus(str, Enum):
    INFLIGHT = "inflight"
    COMPLETED = "completed"
    EXPIRED_OR_NOT_FOUND = "expired_or_not_found"
    UNCERTAIN = "uncertain"


@dataclass
class IdemRecord:
    key: str
    tenant_id: str
    payload_hash: str
    status: IdemStatus
    result: Any = None
    completed_at: float | None = None
    last_accessed_at: float | None = None


@dataclass(frozen=True)
class QueryOutcome:
    """只读查询结论（按租户隔离，不产生副作用、不占额度）。"""

    status: QueryStatus
    result: Any = None


def stable_payload_hash(payload: Any) -> str:
    """对载荷做规范化后取 SHA-256，保证语义相同的载荷哈希一致。"""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_StorageKey = tuple[str, str]


class IdempotencyStore(Protocol):
    """结算记录存储协议（可替换为 Redis 等远端实现）。"""

    def acquire(
        self, tenant_id: str, key: str, payload_hash: str
    ) -> tuple[IdemRecord, AcquireOutcome]:
        """尝试占位/复用，返回 ``(记录, 裁决)``；冲突/过期抛 GovernanceError。"""
        ...

    def complete(self, tenant_id: str, key: str, result: Any) -> None: ...
    def fail(self, tenant_id: str, key: str) -> None: ...
    def release(self, tenant_id: str, key: str) -> None: ...
    def mark_uncertain(self, tenant_id: str, key: str) -> None: ...
    def wait_until_settled(
        self, tenant_id: str, key: str, timeout: float | None = None
    ) -> IdemRecord: ...
    def query(
        self, tenant_id: str, key: str
    ) -> QueryOutcome: ...
    def sweep(self) -> None: ...
    def reset(self) -> None: ...


class InMemoryIdempotencyStore:
    """线程安全的内存结算存储，带保留时长、LRU 容量与墓碑保护。

    所有状态切换都在同一把锁内完成；业务执行期间不持锁，跟随者在该键的
    Condition 上阻塞。存储键为 ``(租户, 提交标识)``，跨租户天然隔离。
    """

    def __init__(
        self,
        retention: RetentionConfig | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._retention = retention or RetentionConfig()
        self._clock = clock
        self._lock = threading.Lock()
        self._conditions: dict[_StorageKey, threading.Condition] = {}
        self._waiters: dict[_StorageKey, int] = {}
        self._records: dict[_StorageKey, IdemRecord] = {}
        # 已淘汰/过期标识 -> 墓碑失效时间点：挡住静默二次执行
        self._tombstones: dict[_StorageKey, float] = {}
        # 每租户已完成结论的访问顺序（LRU，front 最久未访问）
        self._tenant_lru: dict[str, OrderedDict[str, None]] = {}
        # 每租户完成时间队列 (completed_at, key)：按完成顺序做 TTL 清扫
        self._completion: dict[str, deque[tuple[float, str]]] = {}

    # ---- 内部工具（均在持锁时调用） ----

    def _cond(self, skey: _StorageKey) -> threading.Condition:
        cv = self._conditions.get(skey)
        if cv is None:
            cv = threading.Condition(self._lock)
            self._conditions[skey] = cv
        return cv

    def _expire_tombstone_locked(
        self, skey: _StorageKey, now: float
    ) -> bool:
        """墓碑仍有保护作用返回 True；已过墓碑期则删除并返回 False。"""
        until = self._tombstones.get(skey)
        if until is None:
            return False
        if until <= now:
            self._tombstones.pop(skey, None)
            return False
        return True

    def _tombstone_locked(
        self, skey: _StorageKey, now: float
    ) -> None:
        """把一条记录转为墓碑：结论不可再回放，也不会被当新请求执行。"""
        tenant_id, key = skey
        self._records.pop(skey, None)
        lru = self._tenant_lru.get(tenant_id)
        if lru is not None:
            lru.pop(key, None)
        self._tombstones[skey] = now + self._retention.tombstone_ttl_seconds
        self._cond(skey).notify_all()

    def _evict_if_needed_locked(
        self, tenant_id: str, now: float
    ) -> None:
        """超过每租户容量上限时，从 LRU 最久未访问的已完成结论开始淘汰。

        处理中、仍有并发等待者的记录跳过；被淘汰者转为墓碑而不是直接删除，
        从而对随后到达的同标识重试给出明确的 RESULT_EXPIRED。
        """
        cap = self._retention.max_completed_records
        if cap is None:
            return
        lru = self._tenant_lru.get(tenant_id)
        while lru is not None and len(lru) > cap:
            victim: str | None = None
            for candidate in lru:
                skey = (tenant_id, candidate)
                rec = self._records.get(skey)
                if (
                    rec is not None
                    and rec.status is IdemStatus.COMPLETED
                    and self._waiters.get(skey, 0) == 0
                ):
                    victim = candidate
                    break
            if victim is None:
                return  # 全部不可淘汰（处理中/等待中），本轮保留
            lru.pop(victim)
            logger.info(
                "idempotency_key=%s tenant=%s decision=evict "
                "basis=capacity_lru", victim, tenant_id,
            )
            self._tombstone_locked((tenant_id, victim), now)

    def _sweep_ttl_locked(self, now: float) -> None:
        """按完成时间顺序把超期结论转为墓碑，并清掉过了保护期的墓碑。"""
        ttl = self._retention.result_ttl_seconds
        if ttl is not None:
            for tenant_id, queue in self._completion.items():
                while queue:
                    completed_at, key = queue[0]
                    if completed_at + ttl > now:
                        break
                    queue.popleft()
                    skey = (tenant_id, key)
                    rec = self._records.get(skey)
                    if (
                        rec is not None
                        and rec.status is IdemStatus.COMPLETED
                        and rec.completed_at == completed_at
                        and self._waiters.get(skey, 0) == 0
                    ):
                        logger.info(
                            "idempotency_key=%s tenant=%s decision=expire "
                            "basis=retention_ttl", key, tenant_id,
                        )
                        self._tombstone_locked(skey, now)
        for skey, until in list(self._tombstones.items()):
            if until <= now:
                self._tombstones.pop(skey, None)

    # ---- 协议实现 ----

    def acquire(
        self, tenant_id: str, key: str, payload_hash: str
    ) -> tuple[IdemRecord, AcquireOutcome]:
        skey = (tenant_id, key)
        with self._lock:
            now = self._clock()
            self._sweep_ttl_locked(now)
            if self._expire_tombstone_locked(skey, now):
                raise GovernanceError(
                    DenyReason.RESULT_EXPIRED,
                    f"提交标识 {key!r} 的处理结论已过期或被淘汰，"
                    "请使用新的提交标识重新发起",
                )
            existing = self._records.get(skey)
            if existing is None:
                record = IdemRecord(
                    key=key, tenant_id=tenant_id, payload_hash=payload_hash,
                    status=IdemStatus.INFLIGHT, last_accessed_at=now,
                )
                self._records[skey] = record
                return record, AcquireOutcome.LEADER
            if existing.status is IdemStatus.UNCERTAIN:
                raise GovernanceError(
                    DenyReason.DEPENDENCY_UNAVAILABLE,
                    "该提交的副作用可能已经发生，但结论暂时无法确认，"
                    "请勿重复提交，请稍后凭同一提交标识查询",
                )
            if (
                existing.status is IdemStatus.COMPLETED
                and existing.completed_at is not None
                and self._retention.result_ttl_seconds is not None
                and existing.completed_at + self._retention.result_ttl_seconds <= now
                and self._waiters.get(skey, 0) == 0
            ):
                logger.info(
                    "idempotency_key=%s tenant=%s decision=expire "
                    "basis=retention_ttl_lazy", key, tenant_id,
                )
                self._tombstone_locked(skey, now)
                raise GovernanceError(
                    DenyReason.RESULT_EXPIRED,
                    f"提交标识 {key!r} 的处理结论已过期，"
                    "请使用新的提交标识重新发起",
                )
            if existing.payload_hash != payload_hash:
                raise GovernanceError(
                    DenyReason.IDEMPOTENCY_CONFLICT,
                    f"提交标识 {key!r} 已携带不同载荷提交，拒绝语义冲突的请求",
                )
            existing.last_accessed_at = now
            if existing.status is IdemStatus.COMPLETED:
                lru = self._tenant_lru.get(tenant_id)
                if lru is not None and key in lru:
                    lru.move_to_end(key)
                return existing, AcquireOutcome.REPLAY
            return existing, AcquireOutcome.INFLIGHT

    def complete(self, tenant_id: str, key: str, result: Any) -> None:
        skey = (tenant_id, key)
        with self._lock:
            now = self._clock()
            record = self._records[skey]
            record.status = IdemStatus.COMPLETED
            record.result = result
            record.completed_at = now
            record.last_accessed_at = now
            lru = self._tenant_lru.setdefault(tenant_id, OrderedDict())
            lru[key] = None
            lru.move_to_end(key)
            self._completion.setdefault(tenant_id, deque()).append((now, key))
            self._sweep_ttl_locked(now)
            self._evict_if_needed_locked(tenant_id, now)
            self._cond(skey).notify_all()

    def fail(self, tenant_id: str, key: str) -> None:
        skey = (tenant_id, key)
        with self._lock:
            record = self._records.get(skey)
            if record is not None:
                record.status = IdemStatus.FAILED
            self._cond(skey).notify_all()

    def release(self, tenant_id: str, key: str) -> None:
        skey = (tenant_id, key)
        with self._lock:
            self._records.pop(skey, None)
            self._cond(skey).notify_all()

    def mark_uncertain(self, tenant_id: str, key: str) -> None:
        skey = (tenant_id, key)
        with self._lock:
            record = self._records.get(skey)
            if record is not None:
                record.status = IdemStatus.UNCERTAIN
            self._cond(skey).notify_all()

    def wait_until_settled(
        self,
        tenant_id: str,
        key: str,
        timeout: float | None = None,
    ) -> IdemRecord:
        """等待同键首请求落定（完成/失败/不确定/被释放）。"""
        skey = (tenant_id, key)
        with self._lock:
            cv = self._cond(skey)
            self._waiters[skey] = self._waiters.get(skey, 0) + 1
            try:
                record = self._records.get(skey)
                while record is not None and record.status is IdemStatus.INFLIGHT:
                    if not cv.wait(timeout=timeout):
                        raise StoreUnavailable(
                            f"等待提交标识 {key!r} 的首请求落定超时"
                        )
                    record = self._records.get(skey)
                if record is None:
                    # 首请求业务失败并释放了占位：允许调用方安全重试。
                    raise StoreUnavailable(
                        f"提交标识 {key!r} 的首请求执行失败，占位已释放"
                    )
                return record
            finally:
                self._waiters[skey] -= 1

    def query(self, tenant_id: str, key: str) -> QueryOutcome:
        """按租户隔离的只读查询：不执行业务、不占额度、不改变结算结论。"""
        skey = (tenant_id, key)
        with self._lock:
            now = self._clock()
            self._sweep_ttl_locked(now)
            if self._expire_tombstone_locked(skey, now):
                return QueryOutcome(QueryStatus.EXPIRED_OR_NOT_FOUND)
            record = self._records.get(skey)
            if record is None:
                return QueryOutcome(QueryStatus.EXPIRED_OR_NOT_FOUND)
            if record.status is IdemStatus.INFLIGHT:
                return QueryOutcome(QueryStatus.INFLIGHT)
            if record.status is IdemStatus.UNCERTAIN:
                return QueryOutcome(QueryStatus.UNCERTAIN)
            if record.status is IdemStatus.FAILED:
                return QueryOutcome(QueryStatus.EXPIRED_OR_NOT_FOUND)
            # COMPLETED：查询也算一次访问，刷新 LRU 位置（无业务副作用）。
            record.last_accessed_at = now
            lru = self._tenant_lru.get(tenant_id)
            if lru is not None and key in lru:
                lru.move_to_end(key)
            return QueryOutcome(QueryStatus.COMPLETED, record.result)

    def sweep(self) -> None:
        """显式触发超期/墓碑清扫（也可由后台周期任务调用）。"""
        with self._lock:
            self._sweep_ttl_locked(self._clock())

    def reset(self) -> None:
        with self._lock:
            self._conditions.clear()
            self._waiters.clear()
            self._records.clear()
            self._tombstones.clear()
            self._tenant_lru.clear()
            self._completion.clear()


class FailingIdempotencyStore:
    """测试用：acquire 即故障的存储（结算闸门保守拒绝）。"""

    def acquire(self, tenant_id, key, payload_hash):
        raise StoreUnavailable("idempotency backend unavailable")

    def complete(self, tenant_id, key, result):
        raise StoreUnavailable("idempotency backend unavailable")

    def fail(self, tenant_id, key): ...
    def release(self, tenant_id, key): ...
    def mark_uncertain(self, tenant_id, key): ...
    def wait_until_settled(self, tenant_id, key, timeout=None):
        raise StoreUnavailable("idempotency backend unavailable")

    def query(self, tenant_id, key):
        raise StoreUnavailable("idempotency backend unavailable")

    def sweep(self): ...
    def reset(self): ...


class CompleteFailingIdempotencyStore(InMemoryIdempotencyStore):
    """测试用：占位与查询正常，但 complete 落定失败 -> 进入 UNCERTAIN。"""

    def complete(self, tenant_id: str, key: str, result: Any) -> None:
        # 先把结论暂存到内存（基类落定），再模拟“对外确认失败”。
        super().complete(tenant_id, key, result)
        raise StoreUnavailable("idempotency backend unavailable on complete")

    def mark_uncertain(self, tenant_id: str, key: str) -> None:
        skey = (tenant_id, key)
        with self._lock:
            record = self._records.get(skey)
            if record is not None and record.status is IdemStatus.COMPLETED:
                # 副作用已发生但无法向调用方确认：撤销可回放结论。
                record.status = IdemStatus.UNCERTAIN
                record.result = None
                lru = self._tenant_lru.get(tenant_id)
                if lru is not None:
                    lru.pop(key, None)
            self._cond(skey).notify_all()


class IdempotencyService:
    """结算执行器：副作用一次、结论一致、仅首写占额度、冲突/过期明确拒绝。"""

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
        *,
        policy: WritePolicy | None = None,
        reserve_quota: Callable[[], None] | None = None,
    ) -> Any:
        # 未携带提交标识：强制标识的接口在此被明确拒绝（与其它失败可区分）；
        # 其余接口按普通请求执行——先占额度再产生副作用，不做结算保护。
        if not key:
            if policy is not None and policy.require_idempotency_key:
                logger.info(
                    "idempotency_key=<none> tenant=%s decision=deny "
                    "basis=idempotency_key_required", tenant_id,
                )
                raise GovernanceError(
                    DenyReason.MISSING_IDEMPOTENCY_KEY,
                    "该写接口要求携带提交标识 X-Idempotency-Key",
                )
            logger.info("idempotency_key=<none> tenant=%s decision=execute "
                        "basis=no_idempotency_key", tenant_id)
            if reserve_quota is not None:
                reserve_quota()
            return action()

        payload_hash = stable_payload_hash(payload)
        try:
            record, outcome = self._store.acquire(tenant_id, key, payload_hash)
        except GovernanceError:
            raise
        except StoreUnavailable as exc:
            logger.warning("idempotency_key=%s tenant=%s decision=deny "
                           "basis=idempotency_store_unavailable error=%s",
                           key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "结算服务暂不可用，已拒绝该写请求以避免重复副作用",
            ) from exc

        if outcome is AcquireOutcome.REPLAY:
            logger.info("idempotency_key=%s tenant=%s decision=replay "
                        "basis=duplicate_after_completion", key, tenant_id)
            return record.result
        if outcome is AcquireOutcome.INFLIGHT:
            return self._run_as_follower(key, tenant_id)
        return self._run_as_leader(
            key, tenant_id, action, reserve_quota=reserve_quota
        )

    def query(self, tenant_id: str, key: str) -> QueryOutcome:
        """只读查询某提交标识的处理结论（按租户隔离，不占额度）。"""
        try:
            return self._store.query(tenant_id, key)
        except StoreUnavailable as exc:
            logger.warning("idempotency_key=%s tenant=%s decision=query_unavailable "
                           "error=%s", key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "结算服务暂不可用，暂时无法查询该提交的处理结论",
            ) from exc

    def _run_as_leader(
        self,
        key: str,
        tenant_id: str,
        action: Callable[[], Any],
        reserve_quota: Callable[[], None] | None,
    ) -> Any:
        # 只有真正的首个写请求才占用额度；额度拒绝时释放占位，允许稍后重试。
        if reserve_quota is not None:
            try:
                reserve_quota()
            except GovernanceError:
                try:
                    self._store.release(tenant_id, key)
                except StoreUnavailable as exc:
                    logger.error(
                        "idempotency_key=%s tenant=%s release_after_quota_deny "
                        "failed error=%s", key, tenant_id, exc,
                    )
                logger.info("idempotency_key=%s tenant=%s decision=release "
                            "basis=quota_denied", key, tenant_id)
                raise
        logger.info("idempotency_key=%s tenant=%s decision=execute "
                    "basis=first_submission", key, tenant_id)
        try:
            result = action()
        except Exception:
            # 业务失败：标记并释放占位，允许调用方用同键安全重试（无副作用）。
            try:
                self._store.fail(tenant_id, key)
                self._store.release(tenant_id, key)
            except StoreUnavailable as exc:
                logger.error("idempotency_key=%s tenant=%s release_failed error=%s",
                             key, tenant_id, exc)
            logger.info("idempotency_key=%s tenant=%s decision=release "
                        "basis=business_failed", key, tenant_id)
            raise
        try:
            self._store.complete(tenant_id, key, result)
        except StoreUnavailable as exc:
            # 副作用已发生但无法落定：记录 UNCERTAIN，绝不允许重试造成重复。
            try:
                self._store.mark_uncertain(tenant_id, key)
            except StoreUnavailable as mark_exc:
                logger.error(
                    "idempotency_key=%s tenant=%s mark_uncertain_failed error=%s",
                    key, tenant_id, mark_exc,
                )
            logger.error("idempotency_key=%s tenant=%s complete_failed error=%s",
                         key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "请求已处理但结果暂时无法确认，请稍后凭同一提交标识查询",
            ) from exc
        logger.info("idempotency_key=%s tenant=%s decision=completed "
                    "basis=side_effect_committed", key, tenant_id)
        return result

    def _run_as_follower(self, key: str, tenant_id: str) -> Any:
        try:
            settled = self._store.wait_until_settled(tenant_id, key,
                                                     self._wait_timeout)
        except StoreUnavailable as exc:
            logger.warning("idempotency_key=%s tenant=%s decision=deny "
                           "basis=wait_settle_unavailable error=%s",
                           key, tenant_id, exc)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "重复请求的处理结果暂不可确认，请稍后凭同一提交标识重试或查询",
            ) from exc
        if settled.status is IdemStatus.FAILED:
            logger.info("idempotency_key=%s tenant=%s decision=deny "
                        "basis=leader_failed_retry_allowed", key, tenant_id)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "首次提交执行失败，请凭同一提交标识重试",
            )
        if settled.status is IdemStatus.UNCERTAIN:
            logger.info("idempotency_key=%s tenant=%s decision=deny "
                        "basis=side_effect_uncertain", key, tenant_id)
            raise GovernanceError(
                DenyReason.DEPENDENCY_UNAVAILABLE,
                "请求已处理但结果暂时无法确认，请稍后凭同一提交标识查询",
            )
        logger.info("idempotency_key=%s tenant=%s decision=replay "
                    "basis=concurrent_duplicate", key, tenant_id)
        return settled.result
