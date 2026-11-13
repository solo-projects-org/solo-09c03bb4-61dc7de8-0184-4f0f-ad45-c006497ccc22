# SQL 差异最小复现平台（FastAPI + DuckDB）

数仓工程师改写报告汇总 SQL 后结果不一致时，用本平台把**精简复现数据**交给维护者：
上传「报告 / 检验项」两份 CSV，声明整数或文本列、主键及一条报告号外键，
再填两条**仅查询这两张表**的确定性 `SELECT`；平台自动删行缩减，产出可下载的最小复现包。

## 核心语义（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 空字段作 NULL | CSV 中空白单元格（含纯空白）统一导入为 `NULL`，建表与候选数据均如此 |
| 仅两表、确定性 SELECT | 静态检查：只允许 `SELECT`/`WITH`，禁止写操作/多语句/`ATTACH` 等；拦截 `random/uuid/now/current_*` 等不确定函数；`FROM/JOIN` 只允许 `report`、`item` |
| 整数或文本列、主键、报告号外键 | 上传时逐列声明 `INTEGER/TEXT` 与主键（可联合）；校验整数格式、主键非空且唯一；外键声明 `item.列 -> report.列` 并校验类型一致，孤儿行仅告警 |
| 无序多重集比较，重复行照计 | 工作进程对两查询结果分别做 `Counter`（每行计数），比较多重集而非集合；列结构不同也视为不同 |
| 两查询均成功且结果不同才采用候选 | 任一查询报错 → 拒绝候选；均成功但多重集相等 → 拒绝；两者都成功且不同 → 采用 |
| 自动删行缩减，删报告联删检验项 | 每轮按主键升序枚举删除单元：① 删一份报告并联删其全部检验项；② 删任一检验项单行。接受一个单元后立即重新开始一轮，保证确定性 |
| 完成标准 | 一整轮中再删任一单行或任一报告联删组都无法保留差异（全部被拒）才标记 `COMPLETE` |
| 超时不能判无差异 | 候选超时只记 `timeouts` 并跳过；若整轮穷尽但含超时，标记 `INCOMPLETE`（未完成），绝不标记无差异，可放宽超时后重新启动 |
| 取消/超时停止实际计算 | 工作进程在独立进程组运行（`python -I` 隔离），取消或超时即 `SIGKILL` 整个进程组；确认取消后不再更换最近验证包 |
| 同时只运行一批 | 全局单批锁，运行中再提交返回 `409` |
| 最近验证包可下载，取消/重启后保留 | 每采用一个候选就原子切换 `packages/current.zip`（符号链接原子替换）；所有状态与数据落盘到 `REPRO_DATA_DIR`，重启后作业恢复为 `INTERRUPTED` 且包仍可下载 |

### 复现包内容（`current.zip`）

```
data/report.csv   data/item.csv   # 当前最小数据（空字段为 NULL）
schema.json                       # 列类型、主键、报告号外键、NULL 规则
queries.sql                       # 两条确定性 SELECT
manifest.json                     # 比较方式、缩减计数、最近验证结果
README.txt
```

## 目录结构

```
app/
  config.py       路径、禁止关键字/不确定函数清单
  validation.py   CSV 解析、列/主键/外键/SQL 静态校验
  worker.py       隔离工作进程：建 DuckDB 表、跑两查询、多重集比较
  runner.py       作业状态机、子进程调度、超时/取消、贪心缩减、打包、重启恢复
  main.py         FastAPI 路由
  web/index.html  网页操作入口（上传、声明、进度、日志、取消、下载）
sample/           本地差异样例（INNER JOIN 改写为 LEFT JOIN 的差异）
tests/test_e2e.py 17 个端到端测试
scripts/run-local.sh
Dockerfile  docker-compose.yml
```

## 方式一：Docker Compose（推荐交付）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

数据卷 `repro-data` 挂载到容器 `/data`，容器重启或取消后作业与最近验证包均保留。

## 方式二：本地直接运行

```bash
# 依赖安装到项目内 .venv（无 docker/pip 时可用 python3 -m pip install --user 起步）
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

./scripts/run-local.sh           # 默认 http://localhost:8000，数据在 ./data
```

网页上可直接点「载入本地差异样例」一键体验；或手动上传 `sample/` 下两份 CSV，
列定义/SQL 见 `sample/sample-job.json`。

## 主要 HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/jobs` | multipart 上传两份 CSV + 列定义 JSON + 主键 JSON + 外键 + 两条 SQL |
| POST | `/api/sample` | 用内置样例建作业并自动开始 |
| GET | `/api/jobs` / `/api/jobs/{id}` | 作业列表 / 状态（含验证、采用、拒绝、超时计数） |
| GET | `/api/jobs/{id}/events` | 结构化事件日志 |
| POST | `/api/jobs/{id}/start` | 启动/恢复（`QUEUED/INCOMPLETE/ERROR/INTERRUPTED` 可启动） |
| POST | `/api/jobs/{id}/cancel` | 请求取消，边界处杀进程组 |
| GET | `/api/jobs/{id}/package` | 下载最近验证通过的 zip（无包时 409） |

## 测试

```bash
.venv/bin/python -I -m pytest tests/ -q
```

覆盖：完整缩减与最小包、无差异/查询失败拒绝、重复行多重集、空字段 NULL、
超时→未完成→放宽后恢复、取消杀进程且不换包、单批 409、重启恢复、
SQL 静态护栏、主键/整数/外键校验、孤儿外键告警等 17 个场景。

## 安全说明

- 工作进程以 `python -I` 启动（隔离模式，不读取当前目录与用户 site-packages），
  仅从磁盘读取该作业自己的 CSV 与 `spec.json`；每个候选使用独立临时目录与内存 DuckDB。
- 用户 SQL 只做只读查询，静态拦截写操作与不确定函数，语义错误由 DuckDB 抛出并记为候选失败。
