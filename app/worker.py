"""隔离工作进程：在独立 DuckDB 实例中执行两条 SELECT 并按无序多重集比较。

以 `python -I worker.py <job_dir> <input_dir>` 方式启动：
- 只读取磁盘上的 CSV / spec.json，不接收任何来自父进程的可执行输入；
- 结果以 JSON 写入 <job_dir>/_last_result.json；
- 父进程负责超时与取消（杀整个进程组），本进程不自行吞超时。
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

# -I 隔离模式不自动把脚本目录加入 sys.path，这里显式加入仓库根以按包导入
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

from app.validation import quote_ident  # noqa: E402


def _load_table(con: duckdb.DuckDBPyConnection, table: str, input_dir: Path,
                spec: dict) -> None:
    cols = ", ".join(
        f"{quote_ident(c['name'])} {c['type']}" for c in spec["columns"]
    )
    con.execute(f"CREATE TABLE {quote_ident(table)} ({cols})")
    csv_path = input_dir / f"{table}.csv"
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        idx = {c["name"]: header.index(c["name"]) for c in spec["columns"]}
        ordered = [idx[c["name"]] for c in spec["columns"]]
        rows = []
        for row in reader:
            if len(row) == 1 and row[0] == "":
                continue
            out = []
            for c, i in zip(spec["columns"], ordered):
                raw = row[i]
                if raw.strip() == "":
                    out.append(None)
                elif c["type"] == "INTEGER":
                    out.append(int(raw.strip()))
                else:
                    out.append(raw)
            rows.append(out)
    placeholders = ", ".join("?" for _ in spec["columns"])
    if rows:
        con.executemany(
            f"INSERT INTO {quote_ident(table)} VALUES ({placeholders})", rows
        )


def _norm_value(v):
    """转成可哈希且类型安全的比较键（区分 int/float/str/bool/None）。"""
    if v is None:
        return ("n", None)
    if isinstance(v, bool):
        return ("b", int(v))
    if isinstance(v, int):
        return ("i", v)
    if isinstance(v, float):
        return ("f", v)
    if isinstance(v, (bytes, bytearray)):
        return ("bl", bytes(v).hex())
    return ("s", str(v))


def _run_query(con, sql: str) -> dict:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    fetched = cur.fetchall()
    # 无序多重集：每行一个计数键，重复行照计
    bag: dict = {}
    for row in fetched:
        key = tuple(_norm_value(v) for v in row)
        bag[key] = bag.get(key, 0) + 1
    return {"columns": cols, "row_count": len(fetched), "bag": bag}


def main(job_dir: Path, input_dir: Path) -> int:
    spec = json.loads((job_dir / "spec.json").read_text(encoding="utf-8"))
    result: dict = {"ok": False}
    try:
        con = duckdb.connect(database=":memory:")
        try:
            _load_table(con, "report", input_dir, spec["report"])
            _load_table(con, "item", input_dir, spec["item"])
            q1 = _run_query(con, spec["sql1"])
            q2 = _run_query(con, spec["sql2"])
        finally:
            con.close()
        same = q1["bag"] == q2["bag"]
        # 列结构差异也视为结果不同（正常同语义查询列应一致）
        same = same and q1["columns"] == q2["columns"]
        result.update(
            ok=True,
            same=same,
            different=not same,
            q1={"columns": q1["columns"], "row_count": q1["row_count"]},
            q2={"columns": q2["columns"], "row_count": q2["row_count"]},
        )
    except Exception as exc:  # SQL 语义错误等：候选失败，但绝非“无差异”
        result.update(ok=False, error_type=type(exc).__name__, error=str(exc))
    (job_dir / "_last_result.json").write_text(
        json.dumps(result, ensure_ascii=False), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: python -I worker.py <job_dir> <input_dir>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(Path(sys.argv[1]), Path(sys.argv[2])))
