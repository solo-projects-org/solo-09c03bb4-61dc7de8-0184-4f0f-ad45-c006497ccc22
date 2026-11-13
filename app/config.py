"""全局配置与路径常量。"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# 数据根目录（任务包、状态全部落盘，重启后仍可下载/恢复）
DATA_DIR = Path(os.environ.get("REPRO_DATA_DIR", "/data")).resolve()
JOBS_DIR = DATA_DIR / "jobs"

# 工作进程解释器选择顺序：
# 1) 环境变量 REPRO_PYTHON 显式指定；
# 2) 项目内 .venv（本地开发，-I 隔离模式下也能导入 duckdb）；
# 3) 当前解释器 sys.executable（Docker 镜像内依赖装在系统 site-packages）。
_VENV_PY = Path(__file__).resolve().parent.parent / ".venv" / "bin" / "python"
if os.environ.get("REPRO_PYTHON"):
    WORKER_PYTHON = os.environ["REPRO_PYTHON"]
elif _VENV_PY.exists():
    WORKER_PYTHON = str(_VENV_PY)
else:
    WORKER_PYTHON = sys.executable
WORKER_SCRIPT = str(Path(__file__).resolve().parent / "worker.py")

TABLES = ("report", "item")
ALLOWED_TYPES = ("INTEGER", "TEXT")

# 确定性要求：禁止使用 DuckDB 中结果不确定的函数/特性。
# 命中即拒绝（按词边界匹配，不区分大小写）。
NON_DETERMINISTIC_TOKENS = (
    "random",
    "uuid",
    "now",
    "current_timestamp",
    "current_date",
    "current_time",
    "get_current_timestamp",
    "epoch_ms",
    "epoch_us",
    "epoch_ns",
    "transaction_timestamp",
    "statement_timestamp",
    "clock_timestamp",
    "systemtime",
)

# 只允许查询两张表，禁止任何写操作/事务/附加数据库等。
FORBIDDEN_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "attach",
    "detach",
    "copy",
    "merge",
    "call",
    "set",
    "use",
    "begin",
    "commit",
    "rollback",
    "grant",
    "revoke",
    "checkpoint",
    "load",
    "install",
    "export",
    "import",
    "pragma",
    "prepare",
    "execute",
    "vacuum",
)


def ensure_dirs() -> None:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
