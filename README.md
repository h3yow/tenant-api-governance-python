# 多租户 API 配额、幂等、重试结算与降级治理

面向多租户写 API 的治理层，让以下场景的行为 **可预期、可解释**：

- 并发请求：配额不会超卖、计数不会错乱；
- 重复提交 / 断线重连：同一提交标识只产生一次业务副作用，
  **重试一律回放首次结论，不再占用额度**；
- 额度耗尽、跨时间窗口后的重试：仍拿到与首次完全一致的结论；
- 结论过期 / 被淘汰：得到明确可区分的失败，绝不静默二次执行；
- 写入策略热更新、依赖异常：按配置确定性降级，并发下结论一致；
- 结论可按标识只读查询，按租户隔离。

## 1. 模块结构

```
governance/
  errors.py       # 统一拒绝原因 DenyReason 与 GovernanceError
  config.py       # 配额规则 QuotaRule、降级策略 DegradeMode、总配置
  policy.py       # 按租户×接口热更新的 WritePolicy、RetentionConfig 与 PolicyRegistry
  identity.py     # 凭据 -> 租户身份 + 权限边界（允许的接口集合）
  quota.py        # 租户×接口×固定窗口计数；存储协议 + 内存实现 + 降级裁决
  idempotency.py  # 占位/等待/落定；TTL+LRU 保留；过期墓碑；不可确认态；只读查询
  middleware.py   # 判定链编排：身份 -> 权限 -> 策略 -> 幂等（仅首请求占配额）
main.py           # FastAPI 示例：订单接口 + 结论查询 + 拒绝原因到 HTTP 映射
tests/            # 65 个单测/集成/HTTP 测试（并发、跨窗口、降级、过期、隔离）
```

## 2. 判定链（顺序确定）

1. **身份解析**：缺少凭据或凭据无法识别 → `missing_identity`（401）。
2. **权限校验**：接口不在权限边界内 → `access_denied`（403）。
3. **写入策略**：接口要求提交标识但请求缺失 →
   `idempotency_key_required`（428，与其他失败明确区分）。
4. **幂等占位**：重复 / 并发 / 断线重连直接等待或回放首次结论，
   **不再经过配额闸门**；只有真正第一次出现的写请求继续第 5 步。
5. **配额消耗（仅首请求）**：耗尽 → `quota_exhausted`（429）；
   存储故障 → 按该租户×接口当前生效的降级模式裁决。
6. **业务执行与落定**：载荷冲突 → `idempotency_conflict`（409）；
   结论已过期 → `result_expired`（410）；副作用可能已发生但无法确认 →
   `dependency_unavailable` / `result_unverifiable`。

所有底层依赖异常都在治理层被翻译成上述可区分原因，调用方永远不会看到
`connection refused` 之类的原始错误。

## 3. 重试结算语义（与旧行为的关键差异）

> **行为变更**：旧版本中"重复提交不再产生业务副作用，但仍占用调用额度"，
> 且重放走在配额检查之后——额度耗尽或跨窗口后重试会得到与首次不同的结论。
> 现在判定顺序调整为 **先幂等占位、后配额**，语义如下：

- **只有真正第一次出现的写请求占用额度**。同一提交标识的后续提交，
  无论是快速重复点击、并发重试，还是断线重连，都回放首次结论：
  - 首次成功 → 返回首次的同一份结果（如同一个 `order_id`）；
  - 首次与重试落在不同时间窗口 → 不占用新窗口额度，仍返回首次结果；
  - 重试时该租户该接口额度已耗尽 → 照常回放，**不会**返回 429。
- **全新的提交标识**在额度耗尽时仍然照常拒绝（`quota_exhausted`）。
- **同一标识携带语义不同的载荷**（规范化 JSON + SHA-256 不同）仍然是
  `idempotency_conflict`（409），不会变成回放，也不会以先到者为准。
  JSON 键顺序不同但语义相同视为同一载荷。
- 首请求**失败**（配额被拒、依赖故障、业务异常）时写成失败终态并携带首次
  原因：并发等待方拿到与首次一致的失败结论（如同为 `quota_exhausted`），
  不产生业务副作用；调用方随后可用**同一标识、同一载荷**安全重试，重试在
  存储锁内被原子回收为新的首请求（因此只占一次新额度、无重复副作用窗口）。

## 4. 配额模型

- **双维度约束**：配额键为 `(租户, 接口, 窗口编号)`，租户、接口各自独立；
  未配置规则的接口不限制。
- **固定时间窗口**：窗口编号 = `epoch // window_seconds`，窗口切换后从 0
  开始，不依赖惰性过期。
- **原子占用、拒绝不计**：`try_acquire` 在存储锁内完成 "判余量 → 递增"，
  并发下放行数严格等于额度；被拒请求不虚增计数。
- 时钟通过 `clock` 注入，测试可精确控制窗口边界。

## 5. 写入策略（按租户 × 接口，可热更新）

`PolicyRegistry` 保存每个 `(租户, 接口)` 的 `WritePolicy`：

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `require_idempotency_key` | `False` | 写请求是否必须携带 `X-Idempotency-Key`；缺失返回 428 `idempotency_key_required`，不占额度、不执行业务 |
| `degrade_mode` | `None`（沿用全局） | 配额存储故障时的降级模式，可覆盖全局 `GovernanceConfig.degrade_mode` |

- **热更新**：`update_write_policy(tenant, api, policy)` /
  `remove_write_policy(...)` / `update_retention(...)` 在锁内整体替换，
  不需要重启服务。
- **并发一致性**：每个请求在进入判定链时只读取一次策略快照；策略调整后
  的后续并发请求统一按新策略执行，不会同一批里一半旧策略一半新策略。
- 降级模式：

| 策略 | 行为 |
| --- | --- |
| `FAIL_CLOSED`（全局默认） | 配额存储故障时拒绝，返回 `dependency_unavailable` (503) |
| `FAIL_OPEN` | 放开配额闸门；**幂等保护不受影响，重复副作用仍然过不去** |
| `USE_CACHE` | 用最近已知窗口计数判断：未达上限放行，已达上限或冷缓存拒绝 |

幂等存储故障始终保守拒绝（即使配额侧是 FAIL_OPEN），因为无法确认结论时
放行写请求可能造成重复副作用。

## 6. 无提交标识的写入

- 默认（`require_idempotency_key=False`，**兼容旧行为**）：无标识请求按
  普通请求执行，**每次都占额度、每次都可能产生副作用**，重复风险由调用方
  自行承担。
- 对不能接受重复副作用的接口，把该 `(租户, 接口)` 策略置为
  `require_idempotency_key=True`：无标识请求在配额与业务之前被明确拒绝
  （428 `idempotency_key_required`）。

## 7. 结论保留、过期与淘汰

`RetentionConfig`（可热更新）控制已完成结论的增长：

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `ttl_seconds` | `3600`（示例服务为 86400） | 已完成结论的保留时长；`<=0` 表示不按时间过期 |
| `max_completed` | `10000` | 已完成结论容量上限；`<=0` 表示不限容量 |

- **超期**：已完成记录在被再次访问时若超过 TTL，翻成 `EXPIRED` 墓碑。
- **容量淘汰**：超过上限时按 **LRU（最久未访问）** 淘汰，回放/查询会刷新
  访问时间，因此刚被请求过的结论不会被清掉。
- **处理中（INFLIGHT）与不可确认（UNVERIFIABLE）记录永不淘汰**。
- **墓碑保证不静默二次执行**：超期或被淘汰的旧标识再次提交时，得到明确的
  `result_expired`（HTTP 410），提示调用方**更换新标识**重新发起；
  即使携带了不同载荷也是 410 而不是被当成新请求。全新标识不受影响。
- 过期判定与淘汰都在同一把存储锁内完成，并发下同一个标识只有一份结果、
  并发重放要么全部回放、要么全部得到 `result_expired`。
- 墓碑是极小记录（不含结果体），用于长期挡住旧标识复活；内存增长与
  "历史出现过的不同标识数量"同阶。

## 8. 按提交标识查询结论

`GET /submissions/{idempotency_key}`（需凭据；按调用方租户隔离）：

- **只读**：不产生业务副作用、不占用配额。
- **租户隔离**：记录键为 `(租户, 标识)`，一个租户的凭据查不到、也不会
  回放另一个租户同标识的结论。
- 返回状态（`status` 字段）：

| status | HTTP | 含义 |
| --- | --- | --- |
| `processing` | 200 | 首次请求仍在处理中 |
| `completed` | 200 | 已完成，响应体 `data` 为首次结论 |
| `failed` | 200 | 首次请求失败（配额/依赖/业务）；结论与首请求一致，可用同一标识重试 |
| `expired_or_unknown` | 404 | 已过期 / 被淘汰 / 从未见过（不区分，避免泄露存在性） |
| `unverifiable` | 200 | 副作用可能已经发生，但结论暂时无法确认；切勿换载荷重试 |

存储故障时查询本身返回 503 `dependency_unavailable`，可稍后凭同一标识再查。

## 9. 失败原因与 HTTP 映射

| `DenyReason` | HTTP | 含义 |
| --- | --- | --- |
| `missing_identity` | 401 | 凭据缺失/无效 |
| `access_denied` | 403 | 越权访问接口 / 查询 |
| `quota_exhausted` | 429 | 全新写请求命中当前窗口配额上限 |
| `idempotency_conflict` | 409 | 同一标识携带语义不同的载荷 |
| `dependency_unavailable` | 503 | 依赖故障按策略拒绝；或副作用已发生但结论无法确认 |
| `idempotency_key_required` | 428 | 接口强制要求提交标识但请求缺失 |
| `result_expired` | 410 | 标识结论已过期/被淘汰，需换新标识 |
| `result_unverifiable` | 409 | 副作用可能已发生，结论暂不可确认 |

写失败响应体统一为 `{"error": "<reason>", "message": "<说明>"}`。

## 10. 兼容范围

- 判定链内部顺序由"配额 → 幂等"调整为"幂等占位 →（仅首请求）配额"，
  对外错误码保持兼容；身份、权限、载荷冲突、并发重复提交等既有行为不变。
- **行为差异**（有意为之）：携带提交标识的重复/重试请求不再占用额度，且在
  额度耗尽、跨窗口后仍回放首次结论——这是本次新增的重试结算保证。
- 不携带提交标识的请求维持旧行为：每次执行、每次占额度。
- 保留策略默认 TTL 3600s / 容量 10000；接入方应据此选择标识生命周期，
  并对 410 `result_expired` 做"换新标识重试"处理。

## 11. 本地验证

前置：Python 3.14+、[uv](https://docs.astral.sh/uv/)。

```bash
uv sync

# 全部测试（65 个）
uv run python -m pytest

# 只看重试结算新行为
uv run python -m pytest tests/test_retry_settlement.py
uv run python -m pytest tests/test_api.py

# 启动示例服务
uv run fastapi dev main.py
```

手工验证：

```bash
# 1) 正常下单 + 同键重放（只创建一个订单，order_id 相同）
curl -s -X POST localhost:8000/orders \
  -H 'X-Auth-Token: token-t1' -H 'X-Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' -d '{"item":"book"}'
curl -s -X POST localhost:8000/orders \
  -H 'X-Auth-Token: token-t1' -H 'X-Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' -d '{"item":"book"}'

# 2) 查询结论（completed）；换租户查同标识 -> 404
curl -s localhost:8000/submissions/demo-1 -H 'X-Auth-Token: token-t1'
curl -s localhost:8000/submissions/demo-1 -H 'X-Auth-Token: token-t2'

# 3) 同键不同载荷 -> 409 idempotency_conflict
curl -s -X POST localhost:8000/orders \
  -H 'X-Auth-Token: token-t1' -H 'X-Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' -d '{"item":"phone"}'

# 4) 运行时把 t1 的 /orders 改为强制提交标识；之后无标识请求 -> 428
curl -s -X POST \
  'localhost:8000/admin/policies/write?tenant_id=t1&api=/orders&require_idempotency_key=true'
curl -s -X POST localhost:8000/orders \
  -H 'X-Auth-Token: token-t1' -H 'Content-Type: application/json' -d '{"item":"x"}'
```

示例凭据：`token-t1`（t1，每分钟 3 次）、`token-t2`（t2，每分钟 2 次）、
`token-t3`（t3，仅 `/health`，用于验证越权）。运行时可通过
`POLICIES.update_write_policy(...)` 热更新强制标识/降级策略，通过
`POLICIES.update_retention(...)` 热更新保留时长与容量。

## 12. 日志可解释性

每条治理判定都打印结构化字段：

```
governance.idempotency idempotency_key=k tenant=t1 decision=execute basis=first_submission
governance.idempotency idempotency_key=k tenant=t1 decision=replay  basis=duplicate_after_completion
governance.idempotency idempotency_key=old tenant=t1 decision=deny   basis=ttl_expired
governance.idempotency idempotency_key=old tenant=t1 decision=evict  basis=lru_capacity_exceeded
governance.chain idempotency_key=<none> tenant=t1 api=/orders decision=deny basis=idempotency_key_required
governance.quota tenant=t1 api=/orders window=16 used=2 limit=3 decision=deny basis=quota_exhausted
```

`basis` 给出判定依据（首提交 / 重复回放 / 并发跟随 / TTL 过期 /
LRU 淘汰 / 缺标识 / 配额授予或耗尽 / 具体降级模式），`tenant`、
`idempotency_key` 标识请求归属。
