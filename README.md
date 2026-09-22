# 多租户 API 配额、幂等与降级治理

在 FastAPI 服务基础上提供一套面向多租户 API 的治理能力，使**并发请求、重复提交、依赖异常**
下的行为可预期、可解释。所有治理判定均输出结构化日志，包含**租户标识、幂等键、接口与判定依据**。

## 1. 目录结构

```
app/
  errors.py            错误码分类与异常（依赖异常被屏蔽，不透出底层细节）
  models.py            Principal / ApiScope / 判定结果等领域模型
  identity.py          令牌身份解析 + 接口级权限边界
  quota.py             租户+接口双维度配额、固定窗口、三种依赖降级策略
  idempotency.py       写请求幂等：占坑/等待回放/载荷冲突/陈旧接管/两种降级
  governance.py        治理引擎：串联鉴权 → 幂等 → 配额 → 业务处理器
  dependencies.py      演示数据与依赖装配（令牌目录、限额、窗口）
  main.py              HTTP 入口 + 本地验证用 /admin 管理端点
tests/                 45 个用例：鉴权分类、并发幂等、跨窗口配额、依赖故障、超时接管
```

## 2. 配额模型

- **双维度约束**：计数键为 `(tenant_id, api, window_start)`。同一租户不同接口、
  同一接口不同租户互不影响；不同租户即使持有相同角色也各自独立计数。
- **固定时间窗口**：`window_start = floor(now / window_seconds) * window_seconds`。
  跨窗口边界后计数归零，新窗口重新计满限额；默认 `window_seconds=60`。
- **预占语义**：请求进入业务处理前原子自增（存储侧加锁），并发下计数严格
  `1..N` 不重不漏（见 `test_concurrent_consumers_never_overcount_but_are_ordered`）。
- **只读接口**默认不计配额（`QuotaPolicy.read_quota=False`，可配置开启）。
- **回放不计费**：幂等重放直接返回首次结论，不再次占用配额，也不产生副作用。
- **限额配置**：按接口配置 `limits={"orders.write": 3}`，未命中用 `default_limit`。

## 3. 幂等规则

写请求（`scope.write=True`）**必须**携带 `Idempotency-Key` 请求头，缺失返回
`400 idempotency_key_required`。

1. **载荷指纹**：对 JSON 载荷做键排序规范化后取 SHA-256 前 32 位；
   键序不同但语义相同的载荷指纹一致。
2. **占坑执行**：以 `put_if_absent` 原子插入 `in_flight` 记录，只有占坑成功者
   （`FIRST`）会执行业务处理器一次。
3. **并发/重复提交**：后来者在一个超时窗口内轮询；首请求完成后得到 `REPLAY`，
   返回与首次**完全一致的结论**（成功结果或首次的业务拒绝错误都被回放），
   HTTP 首次 `201`、回放 `200`，回放体带 `meta.idempotent_replay=true`。
4. **同键不同载荷**：指纹不一致立即返回 `422 idempotency_conflict`，
   **明确拒绝，不静默以先到者为准**；首次结论保持完好可继续回放。
5. **进行中冲突**：首请求在超时窗口内未完成，等待者返回 `409 idempotency_inflight`，
   客户端可稍后用同一键重试。
6. **陈旧接管**：in-flight 记录超过 `in_flight_timeout + 0.05s` 宽限仍未完成
   （模拟首请求进程崩溃），重试方可接管；崩溃方迟到的完成会被
   compare-and-swap 丢弃，不会覆盖接管者结论，保证最终结论唯一。
7. **失败回滚**：配额耗尽或处理器抛出未预期异常时释放幂等占坑（`abandon`），
   客户端可用同一键安全重试；业务规则拒绝（`GovernanceError`）则作为
   确定结论保存并对重复提交回放。

## 4. 降级规则（依赖异常与窗口切换）

底层存储异常统一包装为 `DependencyError`，**绝不透出给调用方**；对外只返回
可区分的治理错误（`503 dependency_unavailable`）或明确标注的降级结果
（`meta.degraded=true` + `reason`）。策略均可在装配期和运行时配置。

### 4.1 配额 `QuotaFailMode`

| 模式 | 存储故障时行为 |
| --- | --- |
| `closed`（默认） | 拒绝：`503`，说明 fail-closed 策略，屏蔽底层原因 |
| `open` | 降级放行：`DEGRADED_ALLOW`，响应明确标注 `fail_mode=open` |
| `last_known` | 依据本地缓存的“最近已知计数”：同窗口未满放行/已满拒绝；**缓存窗口早于当前窗口（窗口切换）时，新窗口视为全新而放行**；缓存为空时拒绝。并发请求读到同一份缓存，判定一致 |

### 4.2 幂等 `IdemFailMode`

| 模式 | 存储故障时行为 |
| --- | --- |
| `closed`（默认） | 拒绝写请求：`503`，不暴露底层异常 |
| `local_dedupe` | 降级为**进程内去重**：同进程内同键仍只产生一次副作用、可回放、冲突仍拒绝，记录与响应均标注 `degraded`（语义边界：跨进程不保证，原因写入 `reason`） |

## 5. 失败原因分类（错误码 → HTTP）

| 错误码 | HTTP | 触发场景 |
| --- | --- | --- |
| `unauthenticated` | 401 | 令牌缺失/未知，无法解析租户 |
| `forbidden` | 403 | 已认证但角色不包含目标接口权限 |
| `idempotency_key_required` | 400 | 写请求缺少幂等键 |
| `idempotency_conflict` | 422 | 幂等键复用但载荷语义不同 |
| `idempotency_inflight` | 409 | 同键首请求仍在执行，等待超时 |
| `quota_exhausted` | 429 | 租户+接口维度配额耗尽（含 last_known 降级拒绝） |
| `dependency_unavailable` | 503 | 底层依赖异常且策略为 fail-closed |
| `internal_error` | 500 | 未预期异常（占坑已释放，可用同键重试） |

## 6. 本地验证

### 6.1 运行测试

```bash
uv sync
uv run pytest -v          # 45 个用例，判定日志输出在 Captured stderr/log
```

测试覆盖：

- **并发重复提交**：12 线程同键并发，断言业务副作用恰好 1 次、1 次首发 + 11 次回放、
  结论一致（`tests/test_idempotency_concurrency.py`）；依赖故障 + local_dedupe 下同样验证。
- **配额跨窗口边界**：用注入时钟验证窗口滚动后计数重置、边界时刻（59s→60s）归属、
  16 线程 100 次请求计数严格有序（`tests/test_quota.py`）。
- **依赖故障**：三种配额降级、两种幂等降级、last_known 在同窗口/窗口切换下的确定性、
  并发下降级判定一致、底层异常文本不出现在响应中（`tests/test_degradation.py`）。
- **失败原因分类**：401/403/400/409/422/429/503 均有断言。
- 单测日志通过统一入口打印 `event=... tenant=... idem_key=... api=... decision=... reason=...`。

### 6.2 启动服务手工演示

```bash
uv run fastapi dev main.py
```

另开终端：

```bash
TOKEN="token-tenant-a-writer"

# 1) 首次下单 201；重复提交（载荷键序不同也视为同一请求）200 回放
curl -s -X POST localhost:8000/api/orders \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" -d '{"item":"book","quantity":1}'
curl -s -X POST localhost:8000/api/orders \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" -d '{"quantity":1,"item":"book"}'

# 2) 同键不同载荷 -> 422 idempotency_conflict
curl -i -X POST localhost:8000/api/orders \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" -d '{"item":"phone","quantity":1}'

# 3) 默认每租户 orders.write 限额 3；第 4 次 -> 429
for i in 2 3 4 5; do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8000/api/orders \
    -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: q-$i" \
    -H "Content-Type: application/json" -d '{"item":"pen"}'
done

# 4) 依赖故障 + 降级切换（仅本地管理端点）
curl -s -X POST localhost:8000/admin/policy \
  -H "Content-Type: application/json" -d '{"quota_fail_mode":"open"}'
curl -s -X POST localhost:8000/admin/faults \
  -H "Content-Type: application/json" -d '{"quota_store":true}'
curl -s -X POST localhost:8000/api/orders \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: deg-1" \
  -H "Content-Type: application/json" -d '{"item":"x"}'
# 响应 meta.degraded=true，reason 说明 fail_mode=open

# 5) 复位
curl -s -X POST localhost:8000/admin/reset
```

### 6.3 演示身份

| 令牌 | 租户 | 权限 |
| --- | --- | --- |
| `token-tenant-a-writer` | tenant-a | orders.read / orders.write |
| `token-tenant-b-writer` | tenant-b | orders.read / orders.write |
| `token-tenant-a-reader` | tenant-a | orders.read |
