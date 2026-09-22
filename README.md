# 多租户 API 配额、幂等与降级治理

面向多租户写 API 的治理层，让以下三类场景的行为 **可预期、可解释**：

- 并发请求：配额不会超卖、计数不会错乱；
- 重复提交：同一幂等键只产生一次业务副作用，返回一致结论；
- 依赖异常 / 时间窗口切换：按可配置策略确定性降级，底层错误不透出给调用方。

## 1. 模块结构

```
governance/
  errors.py       # 统一拒绝原因 DenyReason 与 GovernanceError
  config.py       # 配额规则 QuotaRule、降级策略 DegradeMode、总配置
  identity.py     # 凭据 -> 租户身份 + 权限边界（允许的接口集合）
  quota.py        # 租户×接口×固定窗口计数；存储协议 + 内存实现 + 降级裁决
  idempotency.py  # 幂等键占位/等待/落定；载荷一致性校验；存储协议+内存实现
  middleware.py   # 判定链编排：身份 -> 权限 -> 配额 -> 幂等执行业务
main.py           # FastAPI 示例：订单接口 + 拒绝原因到 HTTP 状态码映射
tests/            # 35 个单测/集成测试（并发、跨窗口、依赖故障）
```

## 2. 判定链（顺序确定）

1. **身份解析**：缺少凭据或凭据无法识别 → `missing_identity`（HTTP 401）。
2. **权限校验**：身份已解析但请求的接口不在其权限边界内 → `access_denied`（403）。
3. **配额消耗**：见第 3 节，耗尽 → `quota_exhausted`（429）；存储故障 → 按降级策略裁决。
4. **幂等执行**：见第 4 节，载荷冲突 → `idempotency_conflict`（409）；存储不可用 → `dependency_unavailable`（503）。

所有底层依赖异常都在治理层被翻译成上述可区分原因，调用方永远不会看到
`connection refused` 之类的原始错误。

## 3. 配额模型

- **双维度约束**：配额键为 `(租户, 接口, 窗口编号)`。不同租户互不串账，
  同一租户对不同接口各自独立计额；未配置规则的接口不限制。
- **固定时间窗口**：窗口编号 = `epoch // window_seconds`。窗口切换后计数
  自动从 0 开始（由 `tests/test_quota.py::test_quota_resets_at_window_boundary`
  覆盖），不依赖惰性过期，因此窗口边界处行为确定。
- **原子占用、拒绝不计**：`try_acquire` 在存储锁内完成 "判余量 → 递增"，
  并发下放行数严格等于额度（200 并发 / 50 额度的测试验证绝不超卖）；
  被拒绝的请求不递增，不虚耗配额。
- 时钟通过 `clock` 注入，测试可以精确控制窗口边界。

## 4. 幂等规则

写请求通过请求头 `X-Idempotency-Key` 携带幂等键（配合规范化后的载荷哈希）：

- **首次提交（leader）**：占位（INFLIGHT）→ 执行业务 → 落定结果（COMPLETED）。
- **完成后重复提交**：直接回放首请求结果，业务动作不再执行。
- **并发重复提交**：后到者在同键的条件变量上等待，首请求落定后拿到
  *同一个* 返回值。测试用 16 线程并发验证：副作用次数 = 1，16 份结论完全一致。
- **同键不同载荷**：载荷做规范化 JSON + SHA-256 比较；哈希不同立即返回
  `idempotency_conflict`，**不会静默以先到者为准**。JSON 键顺序不同但
  语义相同视为同一载荷（回放，不算冲突）。
- **业务失败**：leader 标记 FAILED 并释放占位，调用方可用同一键安全重试，
  不会留下半成品状态。
- **未携带幂等键**：按普通请求执行，不做幂等保护（调用方自行承担重复风险）。

> 注意：幂等重放请求仍会经过身份/权限/配额检查。即"重复提交不再产生业务
> 副作用"，但仍占用该租户该接口的调用额度（重复点击本身也是调用量）。

## 5. 降级策略

配额存储在 `increment` 时发生故障（连接失败、超时、等待落定超时等），
由 `DegradeMode` 配置统一裁决，并发下策略一致：

| 策略 | 行为 | 适用场景 |
| --- | --- | --- |
| `FAIL_CLOSED`（默认） | 拒绝，返回 `dependency_unavailable` (503) | 安全优先，避免故障期超卖 |
| `FAIL_OPEN` | 放行配额闸门 | 可用性优先；幂等保护仍然生效，副作用仍只一次 |
| `USE_CACHE` | 用最近一次已知窗口计数判断：未达上限放行，已达上限或冷缓存未知则拒绝 | 可容忍轻微滞后 |

幂等存储故障（无法占位或无法确认首请求结论）**一律保守拒绝**为
`dependency_unavailable`，因为无法确认状态时放行写请求可能导致重复副作用。

## 6. 失败原因与 HTTP 映射

| `DenyReason` | HTTP | 含义 |
| --- | --- | --- |
| `missing_identity` | 401 | 凭据缺失/无效，无法解析租户 |
| `access_denied` | 403 | 已识别身份但越权访问接口 |
| `quota_exhausted` | 429 | 租户×接口当前窗口配额耗尽 |
| `idempotency_conflict` | 409 | 同一幂等键携带了语义不同的载荷 |
| `dependency_unavailable` | 503 | 底层依赖故障，按降级策略拒绝（不暴露细节） |

响应体统一为 `{"error": "<reason>", "message": "<可解释说明>"}`。

## 7. 本地验证

前置：Python 3.14+、[uv](https://docs.astral.sh/uv/)。

```bash
# 安装依赖（含 pytest 开发依赖）
uv sync

# 运行全部测试；-s 打印治理日志（含 idempotency_key / tenant / basis）
uv run python -m pytest -s

# 只看某类场景
uv run python -m pytest tests/test_quota.py        # 跨窗口、并发不超卖、降级
uv run python -m pytest tests/test_idempotency.py  # 并发重复、载荷冲突、失败重试

# 启动示例服务
uv run fastapi dev main.py
```

手工验证（服务启动后）：

```bash
# 正常下单 + 同键重复提交（只创建一个订单，返回相同 order_id）
curl -s -X POST localhost:8000/orders \
  -H 'X-Auth-Token: token-t1' -H 'X-Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' -d '{"item":"book"}'
curl -s -X POST localhost:8000/orders \
  -H 'X-Auth-Token: token-t1' -H 'X-Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' -d '{"item":"book"}'

# 同键不同载荷 -> 409 idempotency_conflict
curl -s -X POST localhost:8000/orders \
  -H 'X-Auth-Token: token-t1' -H 'X-Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' -d '{"item":"phone"}'

# 无凭据 -> 401；越权（token-t3 无 /orders 权限）-> 403；
# token-t2 连续 3 次不同键下单 -> 第 3 次 429 quota_exhausted
```

示例凭据：`token-t1`（租户 t1，每分钟 3 次）、`token-t2`（t2，每分钟 2 次）、
`token-t3`（t3，仅有 `/health` 权限，用于验证越权拒绝）。

## 8. 日志可解释性

每条治理判定都打印结构化字段，单测日志中可直接核对：

```
governance.idempotency idempotency_key=log-key tenant=t-log decision=execute basis=first_submission
governance.idempotency idempotency_key=log-key tenant=t-log decision=replay  basis=duplicate_after_completion
governance.quota tenant=t1 api=/orders window=16 used=2 limit=3 decision=deny basis=quota_exhausted
governance.quota tenant=t1 api=/orders decision=deny basis=degrade_fail_closed error=...
```

`basis` 给出判定依据（首提交 / 重复回放 / 并发跟随 / 配额授予或耗尽 /
具体降级模式），`tenant`、`idempotency_key` 标识请求归属。
