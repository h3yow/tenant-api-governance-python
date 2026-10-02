# 多租户 API 配额、幂等、重试结算与降级治理

面向多租户写 API 的治理层，让以下场景的行为 **可预期、可解释**：

- 并发请求：配额不会超卖、计数不会错乱；
- 重复提交 / 断线重连 / 重试风暴：同一提交标识只产生一次业务副作用，
  永远返回首次结论，且 **重试不再占额度**（即使额度已耗尽或已跨时间窗口）；
- **首次被额度拒绝也是一种落定结论**：同标识之后无论并发重试、断线重连、
  跨窗口还是额度已恢复，都原样回放同一个 429，绝不重新执行或再占额度；
- 依赖异常 / 策略调整：降级策略可按租户×接口配置并热更新，并发下裁决一致，
  底层错误不透出给调用方；
- 结论可查询、可淘汰：依赖恢复后可凭提交标识只读查询处理结论；结论保留有
  时长与容量上限，**保留配置可在运行中调整并立即生效**；过期/被淘汰的旧
  标识被明确拒绝，绝不静默二次执行。

## 1. 模块结构

```
governance/
  errors.py       # 统一拒绝原因 DenyReason 与 GovernanceError
  config.py       # 配额/写入策略/保留策略 + PolicyRegistry 原子热更新
  identity.py     # 凭据 -> 租户身份 + 权限边界（允许的接口集合）
  quota.py        # 租户×接口×固定窗口计数；存储协议 + 内存实现 + 降级裁决
  idempotency.py  # 结算存储：占位/等待/落定、TTL+LRU 保留、墓碑、只读查询
  middleware.py   # 判定链编排：身份 -> 权限 -> 策略快照 -> 结算（内含配额）
main.py           # FastAPI 示例：订单/支付接口 + 提交结论查询 + 保留配置热调整
tests/            # 79 个单测/集成测试（并发、跨窗口、依赖故障、过期淘汰、热更新）
```

## 2. 判定链（顺序确定）

1. **身份解析**：缺少凭据或凭据无法识别 → `missing_identity`（HTTP 401）。
2. **权限校验**：接口不在其权限边界内 → `access_denied`（403）。
3. **取策略快照**：整次请求只用同一份不可变配置（写入策略、额度、降级模式），
   因此并发请求不会一半走旧策略、一半走新策略（见第 6 节）。
4. **结算执行**：
   - 要求标识的接口缺标识 → `missing_idempotency_key`（**428**，与其它失败区分）；
   - 同标识重试/并发/断线重连 → 回放首次结论，**不占额度、不产生副作用**；
   - 仅真正第一次出现的写请求才占用配额：耗尽 → `quota_exhausted`（429），
     且该拒绝同样 *落定*（见第 3 节）；全新的提交标识在额度用完时仍照常
     被拒绝；
   - 同键不同载荷 → `idempotency_conflict`（409）；
   - 结论已过期/被淘汰 → `result_expired`（**410**），要求换新标识；
   - 结算存储故障 / 副作用已发生但结论无法确认 → `dependency_unavailable`（503）。

## 3. 重试结算语义（与旧版本的关键差异）

> ⚠️ **行为变更**：旧版本中“幂等重放请求仍占用调用额度”。新版本中
> **只有真正第一次出现的写请求占用额度**；重复提交、并发重试、断线重连
> 一律回放首次结论，不再消耗任何额度。

- **额度耗尽后的重试**：首次请求已落定，之后即使该租户该接口额度已经用完，
  同标识重试仍返回首次结论（而不是 429）；但 **全新的提交标识** 仍被 429
  正常拒绝。
- **跨时间窗口的重试**：首次提交与重试落在不同窗口时，重试回放首次结论，
  **不占用新窗口的额度**；新窗口额度完整保留给真正的新请求。
- **并发重复提交**：后到者在同键条件变量上等待，首请求落定后拿到同一个返回值，
  业务副作用严格一次（16 线程并发测试验证：副作用次数 = 1，结论完全一致）。
- **同键不同载荷**：规范化 JSON + SHA-256 比对，哈希不同立即
  `idempotency_conflict`，不会静默回放，也不会因为额度/窗口变化而改变结论；
  JSON 键顺序不同但语义相同视为同一载荷（回放）。
- **首次业务失败**：leader 标记 FAILED 并释放占位（无副作用），调用方可用
  同一键安全重试，重试成为新的 leader 并正常占额度。
- **配额闸门在结算之后**：leader 占位成功后才调用配额占用。

### 3.1 首次被拒（含额度拒绝）后的重试结算

> ⚠️ **行为变更**：旧版本中“额度拒绝后占位立即释放”，同一标识的重试会被
> 当成全新请求重新走配额闸门——跨窗口或额度恢复后会真的再执行一遍、再扣
> 一次额度；配额依赖故障时还会给出与首次不同的 503。新版本中
> **额度拒绝是落定结论（DENIED），与成功结论一样进入保留体系**。

- 首次提交因额度用完被拒（429 `quota_exhausted`）后，同一提交标识之后
  **不管是一批并发一起重试、断线重连再试、跨到下一个时间窗口、还是额度
  后来又有空余**，都原样回放同一个 429：不重新占额度、不产生业务副作用、
  不会变成其它失败（如 503）。
- 同批并发中，首请求还在配额判定时到达的跟随者在条件变量上等待，首请求
  落定后同样回放该拒绝结论——整批结论严格一致。
- 只有 **真正没见过的全新标识** 且额度用完时，才照常走配额闸门拒绝（该
  拒绝同样落定、可回放）。
- 被拒结论与成功结论一样受保留时长/容量上限约束：保留期内可回放、可查询
  （`denied`）；超期或被淘汰后返回 `result_expired`（410），提示调用方
  **换一个新的提交标识** 重新发起——只有新标识才会被当成全新请求执行。
- 配额 *依赖故障*（瞬时错误）不落结论：占位释放，允许稍后安全重试；
  只有确定的治理拒绝（额度用尽）才落定为可回放结论。

## 4. 无标识写入（X-Idempotency-Key 缺失）

- 默认接口：未携带提交标识时按普通请求执行——先占额度再产生副作用，**不做
  结算保护**，重复提交的风险由调用方承担（与旧版本一致）。
- **强制标识接口**：对不能接受重复副作用的接口，可按租户×接口配置
  `require_idempotency_key=True`。缺失标识时返回独立的
  `missing_idempotency_key`（HTTP **428 Precondition Required**），与额度、
  冲突、依赖故障等失败明确区分；该拒绝不产生副作用、不占额度。

## 5. 配额模型与保留规则

### 5.1 配额

- 配额键为 `(租户, 接口, 窗口编号)`，租户/接口互不串账；未配置规则则不限。
- 固定窗口编号 = `epoch // window_seconds`，窗口切换自动归零，边界行为确定。
- “判余量 → 递增”在存储锁内原子完成：200 并发 / 50 额度绝不超卖；被拒请求
  不递增、不虚耗。时钟可注入，测试可精确控制窗口边界。

### 5.2 结论保留与淘汰（`RetentionConfig`）

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `result_ttl_seconds` | `86400`（24h） | 已落定结论的保留时长，按 **最近一次访问**（提交/落定/回放/查询）起算的空闲过期；`None` 表示不按时间过期 |
| `max_completed_records` | `10000` | 每租户已落定结论容量上限；超出按 LRU 淘汰；`None` 表示不限 |
| `tombstone_ttl_seconds` | `86400`（24h） | 过期/被淘汰标识以“墓碑”继续挡住静默重放的时长 |

规则与保证：

- **超期或被淘汰 ≠ 删除后当新请求执行**：旧标识转为墓碑，之后再提交一律
  返回 `result_expired`（HTTP **410 Gone**），提示调用方换新标识；只有墓碑
  保护期也过去后，标识才彻底按“不存在”处理。
- **热数据不误清**：保留时长按最近访问起算，因此正在处理中、刚被提交或
  刚被查询/回放过的结论永远不会被时间清扫误清；容量淘汰走 LRU，同样只挑
  最久未访问的已落定结论，处理中与仍有并发等待者的记录一律跳过。
- **并发一致**：淘汰、超期清扫与并发重放在同一把锁内裁决，同一标识任何
  时刻最多对应一份结论；过期边界上同一批并发重试结论一致（要么统一回放、
  要么统一 `result_expired`），业务副作用最多一次、额度不被重复占用。
- 清扫在结论落定和访问时顺带触发（摊销成本），也可显式调用 `store.sweep()`
  交给后台周期任务。

### 5.3 保留配置的运行中调整

`result_ttl_seconds` 与 `max_completed_records` 支持 **不重启调整**：

- 编程接口：`InMemoryIdempotencyStore.update_retention(new_config)`；
- HTTP 接口：`PUT /governance/retention`（需携带具备写权限的凭据），未提供
  的字段沿用当前值，显式传 `null` 表示“不限”。

生效方式与并发一致性：

- 新配置在结算存储的同一把锁内 **原子替换**，并立即按新值做一次清扫/淘汰
  （例如调小容量上限会立刻把最久未访问的结论淘汰到新区间）——不会改完还
  按旧的过期时间、旧的上限在跑；
- 之后进入的任意一批并发请求统一按新配置判定，不会同一批里一半按旧值、
  一半按新值；
- 调整不会误清热数据：正在处理中、刚被提交或查询过的结论按第 5.2 节的
  保护规则保留（保留时长按最近访问起算，调小 TTL 不会立刻清掉刚被用过的
  结论）。

## 6. 写入策略与热更新

`WritePolicyRule(tenant_id, api, require_idempotency_key, degrade_mode)` 按
租户×接口配置；`degrade_mode=None` 时取全局 `GovernanceConfig.degrade_mode`
（默认 `FAIL_CLOSED`）。

通过 `PolicyRegistry.update(new_config)` 即可 **不重启** 调整策略。注册表返回
不可变快照：每个请求在判定链入口取一次快照，整次请求（以及该请求触发的配额
降级裁决）都基于同一版本——策略更新只影响更新之后进入的请求，已在处理中的
请求继续使用旧快照，因此同一批并发不会出现新旧策略混判。

## 7. 降级策略（存储故障时）

| 策略 | 配额闸门 | 结算闸门 |
| --- | --- | --- |
| `FAIL_CLOSED`（默认） | 拒绝 → 503 `dependency_unavailable` | 拒绝 → 503 |
| `FAIL_OPEN` | 放行配额闸门 | **仍然拒绝**（绝不放开） |
| `USE_CACHE` | 用最近已知窗口计数：未达上限放行，否则/冷缓存拒绝 | 拒绝 → 503 |

关键安全边界：**即使配置为 FAIL_OPEN，放开的也只是配额闸门**。结算存储无法
占位或无法确认首请求结论时一律保守拒绝，因此放行配置永远不会把重复副作用放
过去（`test_fail_open_degrade_never_allows_duplicate_side_effect` 覆盖）。

当副作用已经执行、但结论无法向调用方落定（如 complete 写失败）时，记录进入
**UNCERTAIN** 状态：重复提交被拒（避免第二次副作用），调用方应凭同一标识
查询，而不是重试。

## 8. 提交结论只读查询

`GET /submissions/{key}?api=/orders`（携带调用方凭据），由
`GovernanceMiddleware.query_settlement` 提供，**只读、不产生副作用、不占
额度**，并严格按租户隔离：一个租户的凭据查不到另一个租户同标识的结论；无
凭据 → 401。

| 返回 `status` | 含义 |
| --- | --- |
| `inflight` | 首请求仍在处理中 |
| `completed` | 已完成，`data` 为首次结论（逐字节一致） |
| `denied` | 首次提交被治理拒绝（如额度用尽），`data` 含原始拒绝原因与说明 |
| `expired_or_not_found` | 结论已过期/被淘汰，或该租户下不存在此标识 |
| `uncertain` | 副作用可能已发生，但结论暂时无法确认（请勿重试，稍后再查） |

## 9. 失败原因与 HTTP 映射

| `DenyReason` | HTTP | 含义 |
| --- | --- | --- |
| `missing_identity` | 401 | 凭据缺失/无效，无法解析租户 |
| `access_denied` | 403 | 已识别身份但越权访问接口 |
| `quota_exhausted` | 429 | 租户×接口当前窗口配额耗尽（新提交标识被拒后该结论落定，同标识重试原样回放） |
| `missing_idempotency_key` | **428** | 接口强制要求提交标识，但请求未携带 |
| `idempotency_conflict` | 409 | 同一提交标识携带了语义不同的载荷 |
| `result_expired` | **410** | 结论已过期/被淘汰，须换新标识重新提交 |
| `dependency_unavailable` | 503 | 依赖故障保守拒绝；或副作用已发生但结论暂不可确认 |

响应体统一为 `{"error": "<reason>", "message": "<可解释说明>"}`。

## 10. 兼容范围

- 治理层对外仍通过 `GovernanceMiddleware.handle(credentials, api,
  idempotency_key, payload, action)` 编排；旧调用点无需修改。
- `QuotaEnforcer` 既可接收静态 `GovernanceConfig`，也可接收 `PolicyRegistry`；
  未显式传入策略注册表时中间件自动复用配额判定器的配置来源。
- 内存结算存储的键由“仅提交标识”变为“(租户, 提交标识)”，跨租户彻底隔离；
  直接操作存储内部字典的工具应改用新的 `reset()` 方法。
- 主要行为差异：**同标识重试不再占用配额**；**首次额度拒绝落定为可回放
  结论**（同标识重试不再被当成新请求重新执行）；新增 428 / 410 两类拒绝；
  结算记录不再无限增长（默认 24h + 每租户 1 万条）；保留时长改为按最近
  访问起算的空闲过期，且保留/容量配置支持运行中调整。
- 内存实现适合单实例演示与测试；分布式部署应把 `IdempotencyStore` /
  `QuotaStore` 换成 Redis 等满足相同协议与原子性保证的实现。

## 11. 本地验证

前置：Python 3.14+、[uv](https://docs.astral.sh/uv/)。

```bash
uv sync

# 全部测试（79 个）；结尾按“业务 -> 用例”汇总通过情况；
# -s 打印治理日志（含 idempotency_key / tenant / decision / basis）
uv run python -m pytest -s

# 按业务场景只看某一组
uv run python -m pytest tests/test_quota_denied_replay.py   # 首次额度拒绝后的重试结算
uv run python -m pytest tests/test_retention_hot_update.py  # 保留/容量配置运行中调整
uv run python -m pytest tests/test_expiry_boundary.py       # 过期/淘汰边界并发重试
uv run python -m pytest tests/test_retry_settlement.py      # 重试结算、策略热更新、降级一致
uv run python -m pytest tests/test_retention_query.py       # TTL/LRU 淘汰、查询状态与租户隔离
uv run python -m pytest tests/test_api_retry.py             # 429回放/410/保留热调整的 HTTP 行为

# 启动示例服务
uv run fastapi dev main.py
```

手工验证：

```bash
# 强制标识接口缺标识 -> 428 missing_idempotency_key
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8000/payments \
  -H 'X-Auth-Token: token-t1' -H 'Content-Type: application/json' -d '{"item":"x"}'

# 正常下单 + 同键重复提交（一个订单、相同 order_id、第二次不占额度）
curl -s -X POST localhost:8000/orders -H 'X-Auth-Token: token-t1' \
  -H 'X-Idempotency-Key: demo-1' -d '{"item":"book"}'
curl -s -X POST localhost:8000/orders -H 'X-Auth-Token: token-t1' \
  -H 'X-Idempotency-Key: demo-1' -d '{"item":"book"}'

# 凭提交标识只读查询（按凭据的租户隔离）
curl -s localhost:8000/submissions/demo-1 -H 'X-Auth-Token: token-t1'

# 首次额度拒绝后同标识重试：t2 每分钟 2 次，用两个新键耗尽后，
# 第三个键首次提交得 429；之后无论重试多少次都原样回放同一个 429，
# 且可查询到 denied 结论
curl -s -X POST localhost:8000/orders -H 'X-Auth-Token: token-t2' -H 'X-Idempotency-Key: a' -d '{"item":"x"}'
curl -s -X POST localhost:8000/orders -H 'X-Auth-Token: token-t2' -H 'X-Idempotency-Key: b' -d '{"item":"x"}'
curl -s -X POST localhost:8000/orders -H 'X-Auth-Token: token-t2' -H 'X-Idempotency-Key: c' -d '{"item":"x"}'  # 429
curl -s -X POST localhost:8000/orders -H 'X-Auth-Token: token-t2' -H 'X-Idempotency-Key: c' -d '{"item":"x"}'  # 仍是同一个 429
curl -s localhost:8000/submissions/c -H 'X-Auth-Token: token-t2'   # status=denied

# 运行中调整保留/容量配置（立即生效，无需重启）
curl -s -X PUT localhost:8000/governance/retention \
  -H 'X-Auth-Token: token-t1' -H 'Content-Type: application/json' \
  -d '{"result_ttl_seconds": 3600, "max_completed_records": 100}'

# 同键不同载荷 -> 409；token-t3 越权 -> 403
```

示例凭据：`token-t1`（租户 t1，每分钟 3 次）、`token-t2`（t2，每分钟 2 次）、
`token-t3`（t3，仅 `/health`）。`/orders` 默认不强制标识，`/payments` 强制
要求 `X-Idempotency-Key`。

## 12. 日志可解释性

每条治理判定都打印结构化字段（幂等键、租户、`decision`、`basis`），例如：

```
governance.idempotency idempotency_key=k tenant=t1 decision=execute  basis=first_submission
governance.idempotency idempotency_key=k tenant=t1 decision=replay   basis=duplicate_after_completion
governance.idempotency idempotency_key=c tenant=t2 decision=deny     basis=quota_exhausted_recorded
governance.idempotency idempotency_key=c tenant=t2 decision=replay_deny basis=duplicate_after_denial deny_reason=quota_exhausted
governance.idempotency idempotency_key=k tenant=t1 decision=evict    basis=capacity_lru
governance.idempotency idempotency_key=k tenant=t1 decision=expire   basis=retention_ttl
governance.idempotency decision=retention_updated basis=runtime_config result_ttl_seconds=3600 max_completed_records=100 ...
governance.quota tenant=t1 api=/orders window=16 decision=deny basis=quota_exhausted
```
