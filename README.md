# 联合目录补采采集服务

联合目录管理员对单馆书目进行补采的 OAI-PMH 采集服务。核心保证：**翻页失败绝不让半批记录入库**，且**大批次在 256 MiB 内存上限内一次采完**（65,536 条 × 4 KiB `dc:description`、每页 256 条实测峰值 RSS 约 75 MiB）。

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| FastAPI 接收截止日，首次另填起始日 | `POST /api/v1/harvest`，请求体 `until` 必填、`from` 选填 |
| 之后从上次成功采集的截止日接着采，起止日都包含 | `from` 留空时取 `harvest_progress.last_until` 作为起始日（含当天） |
| OAI-PMH ListRecords 按日期拉取 oai_dc | 首请求 `verb=ListRecords&metadataPrefix=oai_dc&from=...&until=...` |
| 续页用 resumptionToken 替代日期和格式参数 | 续页请求只带 `verb` + `resumptionToken`；样例源对混用参数返回 `badArgument` |
| 令牌失效丢弃已取页，按原区间重抓一次 | 捕获 `badResumptionToken` 后删除本次暂存文件，用原始 from/until 重新拉取；只重抓一次 |
| 大批次内存有界（256 MiB 内采完 65,536 条） | 翻页时把记录流式写入数据库旁的 JSON Lines 暂存文件，内存只驻留当前页；落库阶段逐条读回 |
| 按标识更新 SQLite 书目 | `bib_records.identifier` 主键 upsert，重复采集幂等 |
| 遇删除标记就撤下 | header `status=deleted` 的记录置为 `withdrawn`（库中不存在则写墓碑） |
| 整批采完才提交书目变更和截止日 | 单事务 `BEGIN IMMEDIATE`，全部页取完才写库并提交；异常整体回滚；提交前目录查询仍返回原目录 |
| 无匹配也推进截止日 | `noRecordsMatch` 不写书目，同事务推进 `last_until` |
| Docker Compose 启动限内存服务和本地分页样例源 | `docker compose up --build` 启动 `harvester-api`（`mem_limit: 256m`）+ `oai-source`（65,536 条样例） |
| Swagger 发起采集并查询目录和进度 | http://localhost:8000/docs |

## 目录结构

```
app/                  FastAPI 采集服务
  main.py             路由（采集/目录/进度/运行历史/健康检查）
  harvester.py        采集编排：翻页、磁盘暂存、令牌失效重抓、整批事务
  oai_client.py       OAI-PMH 2.0 客户端（httpx + xml.etree）
  db.py               SQLite 表结构与事务操作
  schemas.py          Pydantic 请求/响应模型
  Dockerfile
sample-oai/           本地分页样例 OAI-PMH 源（纯标准库）
  server.py           记录数/页大小/描述字节数均可配；默认 27 条（rec-013 删除），
                      compose 中为 65,536 条、每页 256 条、每条 4 KiB dc:description
  Dockerfile
tests/                pytest 端到端测试（16 个用例，含大批次与回滚）
scripts/              辅助脚本（故障回滚演示、全规模内存验证）
docker-compose.yml
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 采集服务 API / Swagger：http://localhost:8000/docs （容器内存上限 256 MiB）
- 样例 OAI 源：http://localhost:8080/oai?verb=Identify
  （65,536 条记录，datestamp 分布在 2026-10-01 起的 90 天内，每页 256 条，每条 4 KiB `dc:description`）
- SQLite 数据保存在命名卷 `harvester-data`（容器内 `/data/harvester.db`），采集暂存文件也落在该卷、用完即删

## 操作流程（Swagger 或 curl）

### 1. 大批补采：一次采完 65,536 条积压书目

```bash
curl -X POST http://localhost:8000/api/v1/harvest \
  -H 'Content-Type: application/json' \
  -d '{"from": "2026-10-01", "until": "2026-12-31"}'
```

响应（约 20~30 秒，256 页全部取完后一次性提交）：

```json
{
  "status": "success",
  "from_date": "2026-10-01",
  "until_date": "2026-12-31",
  "pages": 256, "fetched": 65536,
  "inserted": 65536, "updated": 0, "withdrawn": 0,
  "retries": 0, "run_id": 1
}
```

采集过程中查询 `/api/v1/catalog` 始终返回原目录；提交瞬间 65,536 条与截止日一起可见。

### 2. 后续采集：只填截止日，自动从上次截止日（含）续采

```bash
curl -X POST http://localhost:8000/api/v1/harvest \
  -H 'Content-Type: application/json' \
  -d '{"until": "2027-01-15"}'
# 实际区间：2026-12-31 ~ 2027-01-15（last_until 含当天，避免边界漏采）
```

### 3. 查询目录与进度

```bash
curl 'http://localhost:8000/api/v1/catalog?status=active'   # 在架（默认）
curl 'http://localhost:8000/api/v1/catalog?status=all'      # 含已撤下
curl 'http://localhost:8000/api/v1/catalog/oai:sample.lib:rec-001'
curl 'http://localhost:8000/api/v1/progress'                # 上次成功截止日
curl 'http://localhost:8000/api/v1/runs'                    # 采集运行历史
```

### 4. 无匹配也推进截止日

对没有任何记录的区间发起采集，返回 `fetched=0` 且 `last_until` 照常推进。

## 令牌失效重抓的验证方法

样例源提供故障注入端点：

```bash
# 令下一次 resumptionToken 请求返回 badResumptionToken
curl -X POST http://localhost:8080/admin/fail-next
curl http://localhost:8080/admin/state

# 第 1 页取到后续页令牌失效 -> 丢弃已取页 -> 原区间重抓成功
curl -X POST http://localhost:8000/api/v1/harvest \
  -H 'Content-Type: application/json' \
  -d '{"from": "2026-10-01", "until": "2026-12-31"}'
# 响应中 retries=1；运行历史与服务日志可查
curl -X POST http://localhost:8080/admin/reset   # 清除故障开关
```

若重抓过程中令牌**再次**失效（不可恢复），接口返回 `502`，本批所有书目变更回滚、
截止日保持不变、暂存文件删除，`harvest_runs` 中记录一条 `failed` 历史——任何半批数据都不会进库。

## 内存与一致性设计

- **翻页阶段只往磁盘写**：每页记录解析后追加到数据库旁的 `harvest-spool-*.jsonl`
  暂存文件即释放，内存驻留不超过当前页（256 条 × 约 4.5 KiB ≈ 1.2 MB）；
- **整批取完才落库**：在单个 `BEGIN IMMEDIATE` 事务内逐条从暂存文件读出，
  完成 upsert、撤下、截止日推进、运行历史更新后一次 `commit`；
  任意时刻内存中只驻留一条记录，SQLite 页缓存有界；
- 事务中任何一步失败都会 rollback，库状态回到采集前，暂存文件删除；
- 采集全程不向 `bib_records` / `harvest_progress` 写任何数据，
  提交前目录与进度查询始终返回原值（WAL 模式下读不被阻塞）；
- 进程内互斥锁保证同一时刻只有一个采集任务，并发请求返回 `409`。

实测（`scripts/verify_big_harvest.py`，样例源与采集端同进程、比容器部署更严苛）：
65,536 条 × 4 KiB 描述、256 页，峰值 RSS 约 95 MiB；Docker 双容器部署下
`harvester-api` 实测峰值约 75 MiB，均远低于 256 MiB 上限。

## 本地开发（无 Docker）

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# 终端 1：样例 OAI 源（默认 27 条小数据集；大批次见下方环境变量）
python3 sample-oai/server.py

# 终端 2：API
APP_DB_PATH=./data/harvester.db OAI_ENDPOINT=http://127.0.0.1:8080/oai \
  uvicorn app.main:app --reload

# 测试
pytest

# 全规模内存验证（65,536 条 × 4 KiB，断言峰值 RSS < 256 MiB）
python3 scripts/verify_big_harvest.py

# 故障回滚演示（令牌持续失效 -> 整批回滚）
python3 scripts/test_rollback.py
```

## 环境变量

采集服务：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `OAI_ENDPOINT` | `http://oai-source:8080/oai` | OAI-PMH 端点 |
| `APP_DB_PATH` | `/data/harvester.db` | SQLite 数据库路径（暂存文件放在同目录） |

样例 OAI 源：

| 变量 | 默认值 | compose 中 | 说明 |
| --- | --- | --- | --- |
| `RECORD_COUNT` | `27` | `65536` | 样例记录总数 |
| `PAGE_SIZE` | `10` | `256` | 每页记录数 |
| `DESCRIPTION_BYTES` | `0` | `4096` | 每条 `dc:description` 字节数（渲染时生成，源自身内存有界） |
| `DELETED_INDEX` | `13` | `0` | 第几条带删除标记（1 起）；0 表示无删除记录 |
| `DATE_SPAN_DAYS` | `0` | `90` | datestamp 分布窗口（循环取模）；0 表示每天一条顺延 |
| `TOKEN_TTL` | `3600` | `3600` | resumptionToken 有效期秒数 |
| `PORT` | `8080` | `8080` | 监听端口 |
