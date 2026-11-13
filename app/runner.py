"""缩减运行器：作业状态机、子进程执行、贪心删行与可下载包管理。

并发模型：
- 全局同时只运行一批（一个作业的缩减）；提交时若忙则 409；
- 取消通过 threading.Event 通知，运行器在每次轮询间隙检查并杀掉进程组；
- 所有状态/数据落盘，进程重启后作业可恢复（中断态可重新启动）。
"""
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import uuid
import zipfile
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .config import JOBS_DIR, WORKER_PYTHON, WORKER_SCRIPT, ensure_dirs
from .validation import JobSpec, TableSpec, quote_ident

# ---------- 状态常量 ----------
QUEUED = "QUEUED"
RUNNING = "RUNNING"            # 初始验证或缩减进行中
COMPLETE = "COMPLETE"          # 穷尽：任何单行/联删组都无法保留差异
INCOMPLETE = "INCOMPLETE"      # 最后一轮存在超时，无法判定无差异
CANCELLED = "CANCELLED"
ERROR = "ERROR"                # 工作进程异常等
INTERRUPTED = "INTERRUPTED"    # 服务重启导致的中断，可恢复
REJECTED = "REJECTED"          # 初始验证未通过（查询失败/结果无差异）

TERMINAL = {COMPLETE, INCOMPLETE, CANCELLED, ERROR, REJECTED}
RESUMABLE = {INCOMPLETE, ERROR, INTERRUPTED, QUEUED}

_lock = threading.RLock()
_jobs: dict[str, dict] = {}
_cancel_events: dict[str, threading.Event] = {}
_runner_busy = threading.Event()       # set = 有一批正在运行
_runner_thread: Optional[threading.Thread] = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def job_dir(job_id: str) -> Path:
    return JOBS_DIR / job_id


def _state_path(job_id: str) -> Path:
    return job_dir(job_id) / "state.json"


def _spec_path(job_id: str) -> Path:
    return job_dir(job_id) / "spec.json"


# ---------- 持久化 ----------
def _write_json_atomic(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _load_state(job_id: str) -> dict:
    with _state_path(job_id).open(encoding="utf-8") as fh:
        return json.load(fh)


def _save_state(job_id: str, state: dict) -> None:
    state["updated_at"] = _now()
    _write_json_atomic(_state_path(job_id), state)
    _jobs[job_id] = state


def _event(job_id: str, level: str, message: str, **extra) -> None:
    entry = {"ts": _now(), "level": level, "message": message, **extra}
    with (job_dir(job_id) / "events.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    state = _jobs.get(job_id)
    if state is not None:
        state.setdefault("recent_events", []).append(entry)
        state["recent_events"] = state["recent_events"][-200:]


def read_events(job_id: str, limit: int = 300) -> list[dict]:
    path = job_dir(job_id) / "events.jsonl"
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        lines = fh.readlines()[-limit:]
    return [json.loads(line) for line in lines]


def list_jobs() -> list[dict]:
    with _lock:
        return [_public_state(s) for s in sorted(
            _jobs.values(), key=lambda s: s.get("created_at", ""), reverse=True)]


def get_state(job_id: str) -> Optional[dict]:
    with _lock:
        s = _jobs.get(job_id)
        return _public_state(s) if s else None


def _public_state(state: dict) -> dict:
    s = dict(state)
    s.pop("recent_events", None)
    s["busy"] = _runner_busy.is_set()
    s["download_available"] = current_package_path(s["job_id"]) is not None
    return s


# ---------- CSV / 包 ----------
def _write_table_csv(path: Path, spec: TableSpec) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(spec.colnames)
        for row in spec.rows:
            writer.writerow(["" if v is None else v for v in row])


def _read_table_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = [r for r in reader if not (len(r) == 1 and r[0] == "")]
    return header, rows


def _write_candidate_csvs(input_dir: Path, spec: JobSpec,
                          kept_report: set[tuple], kept_item: set[tuple],
                          report_keys: list[tuple], item_keys: list[tuple]) -> None:
    """按当前保留主键集合写出候选 CSV（保持原始行顺序）。"""
    input_dir.mkdir(parents=True, exist_ok=True)
    r_idx = {k: i for i, k in enumerate(report_keys)}
    i_idx = {k: i for i, k in enumerate(item_keys)}
    r_order = sorted((r_idx[k] for k in kept_report))
    i_order = sorted((i_idx[k] for k in kept_item))
    for table, keep_order in (("report", r_order), ("item", i_order)):
        ts = spec.report if table == "report" else spec.item
        with (input_dir / f"{table}.csv").open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(ts.colnames)
            for i in keep_order:
                w.writerow(["" if v is None else v for v in ts.rows[i]])


def _build_zip(job_id: str, input_dir: Path, label: str) -> Path:
    """将数据、列定义和 SQL 打成可交给维护者的 zip（原子替换 current）。"""
    state = _jobs[job_id]
    spec = json.loads(_spec_path(job_id).read_text(encoding="utf-8"))
    pkg_dir = job_dir(job_id) / "packages"
    pkg_dir.mkdir(parents=True, exist_ok=True)
    target = pkg_dir / f"{label}.zip"
    fd, tmp_name = tempfile.mkstemp(dir=str(pkg_dir), suffix=".zip.tmp")
    os.close(fd)
    try:
        with zipfile.ZipFile(tmp_name, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(input_dir / "report.csv", "data/report.csv")
            zf.write(input_dir / "item.csv", "data/item.csv")
            zf.writestr(
                "schema.json",
                json.dumps(
                    {
                        "tables": {
                            "report": spec["report"],
                            "item": spec["item"],
                        },
                        "foreign_key": {
                            "table": "item",
                            "column": spec["fk_column"],
                            "references": {"table": "report", "column": spec["fk_ref_column"]},
                        },
                        "null_rule": "CSV 空字段（含纯空白）一律作为 NULL",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )
            zf.writestr(
                "queries.sql",
                "-- 查询一（原始/改写前）\n" + spec["sql1"] + ";\n\n"
                "-- 查询二（改写后）\n" + spec["sql2"] + ";\n",
            )
            zf.writestr(
                "manifest.json",
                json.dumps(
                    {
                        "job_id": job_id,
                        "label": label,
                        "created_at": _now(),
                        "comparison": "无序多重集（重复行照计），两查询均成功且结果不同",
                        "reduction_status": state["status"],
                        "attempts": state["attempts"],
                        "accepted": state["accepted"],
                        "rejected": state["rejected"],
                        "timeouts": state["timeouts"],
                        "last_result": state.get("last_result"),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )
            zf.writestr(
                "README.txt",
                "SQL 差异最小复现包\n"
                "==================\n"
                "data/      两份 CSV（空字段为 NULL）\n"
                "schema.json 列类型、主键与报告号外键定义\n"
                "queries.sql 两条确定性 SELECT（仅访问 report/item）\n"
                "manifest.json 比较方式与缩减计数\n"
                "在 DuckDB 中按 schema.json 建表导入 data/ 后分别执行两条查询，\n"
                "结果按无序多重集比较应保持不同。\n",
            )
        os.replace(tmp_name, target)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    # 原子切换 current 符号链接
    cur = pkg_dir / "current.zip"
    tmp_link = pkg_dir / "current.zip.new"
    if tmp_link.exists() or tmp_link.is_symlink():
        tmp_link.unlink()
    os.symlink(f"{label}.zip", tmp_link)
    os.replace(tmp_link, cur)
    return target


def current_package_path(job_id: str) -> Optional[Path]:
    cur = job_dir(job_id) / "packages" / "current.zip"
    if cur.is_symlink() or cur.exists():
        try:
            real = cur.resolve(strict=True)
            return real if real.exists() else None
        except OSError:
            return None
    return None


# ---------- 子进程执行 ----------
def run_attempt(job_id: str, input_dir: Path, timeout: float,
                cancel: threading.Event) -> dict:
    """运行一次候选验证。返回 dict：status in success/timeout/error/cancelled。"""
    jd = job_dir(job_id)
    result_file = jd / "_last_result.json"
    if result_file.exists():
        result_file.unlink()
    cmd = [WORKER_PYTHON, "-I", WORKER_SCRIPT, str(jd), str(input_dir)]
    # 新进程组，取消/超时时整组杀掉，确保实际计算停止
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True, text=True,
    )
    deadline = time.monotonic() + timeout
    while True:
        try:
            rc = proc.wait(timeout=0.2)
            break
        except subprocess.TimeoutExpired:
            if cancel.is_set():
                _kill_group(proc)
                return {"status": "cancelled"}
            if time.monotonic() >= deadline:
                _kill_group(proc)
                # 超时不能判为无差异：仅记录，绝不接受/否定差异
                return {"status": "timeout"}
    if cancel.is_set():
        _kill_group(proc)
        return {"status": "cancelled"}
    if rc != 0 or not result_file.exists():
        err = proc.stderr.read()[-2000:] if proc.stderr else ""
        return {"status": "error", "error": f"工作进程退出码 {rc}: {err}"}
    payload = json.loads(result_file.read_text(encoding="utf-8"))
    if not payload.get("ok"):
        return {"status": "error",
                "error": f"{payload.get('error_type')}: {payload.get('error')}"}
    return {"status": "success", **payload}


def _kill_group(proc: subprocess.Popen) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


# ---------- 作业创建 ----------
def create_job(spec: JobSpec, warnings: list[str]) -> str:
    job_id = time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8]
    jd = job_dir(job_id)
    (jd / "input").mkdir(parents=True)
    (jd / "packages").mkdir()
    _write_table_csv(jd / "input" / "report.csv", spec.report)
    _write_table_csv(jd / "input" / "item.csv", spec.item)
    spec_payload = {
        "report": {
            "columns": [asdict(c) for c in spec.report.columns],
            "primary_key": spec.report.primary_key,
        },
        "item": {
            "columns": [asdict(c) for c in spec.item.columns],
            "primary_key": spec.item.primary_key,
        },
        "fk_column": spec.fk_column,
        "fk_ref_column": spec.fk_ref_column,
        "sql1": spec.sql1,
        "sql2": spec.sql2,
        "timeout_seconds": spec.timeout_seconds,
    }
    _write_json_atomic(jd / "spec.json", spec_payload)
    state = {
        "job_id": job_id,
        "status": QUEUED,
        "created_at": _now(),
        "updated_at": _now(),
        "warnings": warnings,
        "attempts": 0,
        "accepted": 0,
        "rejected": 0,
        "timeouts": 0,
        "errors": 0,
        "pass_index": 0,
        "report_rows": len(spec.report.rows),
        "item_rows": len(spec.item.rows),
        "current_report_rows": len(spec.report.rows),
        "current_item_rows": len(spec.item.rows),
        "last_attempt": None,
        "last_result": None,
        "recent_events": [],
    }
    _write_json_atomic(_state_path(job_id), state)
    with _lock:
        _jobs[job_id] = state
    _event(job_id, "info", f"作业已创建：报告 {len(spec.report.rows)} 行，检验项 {len(spec.item.rows)} 行")
    for w in warnings:
        _event(job_id, "warn", w)
    return job_id


# ---------- 缩减主循环 ----------
def request_cancel(job_id: str) -> bool:
    with _lock:
        state = _jobs.get(job_id)
        if not state or state["status"] in TERMINAL:
            return False
        state["cancel_requested"] = True
        _save_state(job_id, state)
        ev = _cancel_events.get(job_id)
        if ev:
            ev.set()
    _event(job_id, "warn", "收到取消请求，将在当前验证边界停止实际计算")
    return True


def start_job(job_id: str) -> None:
    global _runner_thread
    with _lock:
        state = _jobs.get(job_id)
        if not state:
            raise KeyError(job_id)
        if state["status"] not in RESUMABLE:
            raise ValueError(f"作业状态 {state['status']} 不可启动")
        if _runner_busy.is_set():
            raise RuntimeError("已有一批缩减正在运行，同一时刻只运行一批")
        state["status"] = QUEUED
        state.pop("cancel_requested", None)
        _save_state(job_id, state)
        ev = threading.Event()
        _cancel_events[job_id] = ev
        _runner_busy.set()
        t = threading.Thread(target=_run_safe, args=(job_id, ev), daemon=True)
        _runner_thread = t
        t.start()


def _run_safe(job_id: str, cancel: threading.Event) -> None:
    try:
        _run(job_id, cancel)
    except Exception as exc:  # 兜底：异常进入 ERROR，不吞掉
        with _lock:
            state = _jobs.get(job_id)
            if state and state["status"] not in TERMINAL:
                state["status"] = ERROR
                state["fatal_error"] = repr(exc)
                _save_state(job_id, state)
        _event(job_id, "error", f"运行器异常: {exc!r}")
    finally:
        _runner_busy.clear()
        _cancel_events.pop(job_id, None)


def _pk_keys(spec: TableSpec) -> list[tuple]:
    col_index = {name: i for i, name in enumerate(spec.colnames)}
    idxs = [col_index[p] for p in spec.primary_key]
    return [tuple(row[i] for i in idxs) for row in spec.rows]


def _load_rows_into_spec(ts: TableSpec, csv_path: Path) -> TableSpec:
    """按列定义从磁盘 CSV 重新载入数据行（空字段作 NULL，INTEGER 转 int）。"""
    header, str_rows = _read_table_csv(csv_path)
    pos = [header.index(c.name) for c in ts.columns]
    rows: list[list[object]] = []
    for srow in str_rows:
        typed: list[object] = []
        for c, p in zip(ts.columns, pos):
            raw = srow[p]
            if raw.strip() == "":
                typed.append(None)
            elif c.type == "INTEGER":
                typed.append(int(raw.strip()))
            else:
                typed.append(raw)
        rows.append(typed)
    ts.rows = rows
    return ts


def _build_units(spec: JobSpec, kept_report: set[tuple], kept_item: set[tuple],
                 report_keys: list[tuple], item_keys: list[tuple]) -> list[dict]:
    """构建本轮尝试单元：报告联删组 + 检验项单行，全部按主键升序，保证确定性。"""
    fk_i = spec.item.colnames.index(spec.fk_column)
    ref_i = spec.report.colnames.index(spec.fk_ref_column)
    # report 主键值 -> 该报告当前仍保留的检验项主键
    groups: dict = {}
    item_rows_by_key = dict(zip(item_keys, spec.item.rows))
    for ik in sorted(kept_item):
        fk_val = item_rows_by_key[ik][fk_i]
        groups.setdefault(fk_val, []).append(ik)
    units: list[dict] = []
    report_rows_by_key = dict(zip(report_keys, spec.report.rows))
    for rk in sorted(kept_report):
        ref_val = report_rows_by_key[rk][ref_i]
        child_items = sorted(groups.get(ref_val, []))
        units.append({
            "kind": "report_group",
            "report_key": rk,
            "item_keys": child_items,
            "label": f"删除报告 {dict(zip(spec.report.primary_key, rk))}"
                     + (f" 并联删 {len(child_items)} 条检验项" if child_items else ""),
        })
    for ik in sorted(kept_item):
        units.append({
            "kind": "item_row",
            "item_key": ik,
            "label": f"删除检验项 {dict(zip(spec.item.primary_key, ik))}",
        })
    return units


def _run(job_id: str, cancel: threading.Event) -> None:
    jd = job_dir(job_id)
    spec_dict = json.loads(_spec_path(jd).read_text(encoding="utf-8"))
    from .validation import ColumnSpec, TableSpec  # 局部导入避免循环
    def _ts(t):
        return TableSpec(
            columns=[ColumnSpec(**c) for c in t["columns"]],
            primary_key=list(t["primary_key"]),
        )
    spec = JobSpec(
        report=_ts(spec_dict["report"]), item=_ts(spec_dict["item"]),
        fk_column=spec_dict["fk_column"], fk_ref_column=spec_dict["fk_ref_column"],
        sql1=spec_dict["sql1"], sql2=spec_dict["sql2"],
        timeout_seconds=spec_dict["timeout_seconds"],
    )
    # spec.json 不含数据行，从磁盘 CSV 重新载入（重启恢复路径同样依赖）
    _load_rows_into_spec(spec.report, jd / "input" / "report.csv")
    _load_rows_into_spec(spec.item, jd / "input" / "item.csv")
    # 初始验证：完整数据上两条查询都必须成功且结果不同
    with _lock:
        state = _load_state(job_id)
        state["status"] = RUNNING
        _save_state(job_id, state)
    _event(job_id, "info", "开始初始验证：完整数据上执行两条查询")
    with tempfile.TemporaryDirectory(dir=str(jd), prefix="try-") as tmp:
        tmp_in = Path(tmp) / "input"
        shutil.copytree(jd / "input", tmp_in)
        attempt = run_attempt(job_id, tmp_in, spec.timeout_seconds, cancel)
    with _lock:
        state = _load_state(job_id)
        state["attempts"] += 1
        state["last_attempt"] = attempt["status"]
    if attempt["status"] == "cancelled":
        state["status"] = CANCELLED
        _save_state(job_id, state)
        _event(job_id, "warn", "初始验证前被取消，未更换包")
        return
    if attempt["status"] == "timeout":
        state["timeouts"] += 1
        state["status"] = INCOMPLETE
        _save_state(job_id, state)
        _event(job_id, "error", "初始验证超时：超时不代表无差异，不能判为无差异，标记为未完成")
        return
    if attempt["status"] == "error":
        state["errors"] += 1
        state["status"] = REJECTED
        state["fatal_error"] = attempt["error"]
        _save_state(job_id, state)
        _event(job_id, "error", f"初始验证查询失败，拒绝建作业候选：{attempt['error']}")
        return
    if not attempt.get("different"):
        state["status"] = REJECTED
        state["last_result"] = {k: attempt[k] for k in ("q1", "q2", "same")}
        _save_state(job_id, state)
        _event(job_id, "error", "两条查询结果相同（多重集相等），不存在差异，作业不可采用")
        return
    # 初始差异成立 -> 生成第一个可下载包
    state["last_result"] = {k: attempt[k] for k in ("q1", "q2", "same")}
    state["accepted"] += 1
    _save_state(job_id, state)
    pkg = _build_zip(job_id, jd / "input", "initial")
    _event(job_id, "info",
           f"初始验证通过：Q1 {attempt['q1']['row_count']} 行 / Q2 {attempt['q2']['row_count']} 行，"
           f"结果不同，已生成可下载包 {pkg.name}")

    # 缩减：保留集合（主键元组）
    report_keys = _pk_keys(spec.report)
    item_keys = _pk_keys(spec.item)
    kept_report: set[tuple] = set(report_keys)
    kept_item: set[tuple] = set(item_keys)

    while True:
        if cancel.is_set():
            with _lock:
                state = _load_state(job_id)
                state["status"] = CANCELLED
                _save_state(job_id, state)
            _event(job_id, "warn", "确认取消，停止缩减；最近验证过的包保留且不再更换")
            return
        with _lock:
            state = _load_state(job_id)
            state["status"] = RUNNING
            state["pass_index"] += 1
            pass_no = state["pass_index"]
            _save_state(job_id, state)
        units = _build_units(spec, kept_report, kept_item, report_keys, item_keys)
        if not units:
            # 两表均已删空仍保留差异：自然穷尽
            state = _load_state(job_id)
            state["status"] = COMPLETE
            state["note"] = "已删至空表仍保持差异"
            _save_state(job_id, state)
            _event(job_id, "info", "两表均已删空且差异仍存在，缩减完成")
            return
        _event(job_id, "info",
               f"第 {pass_no} 轮开始：{len(kept_report)} 报告 / {len(kept_item)} 检验项，"
               f"{len(units)} 个删除单元")
        pass_had_timeout = False
        pass_accepted = False
        for ui, unit in enumerate(units, start=1):
            if cancel.is_set():
                with _lock:
                    state = _load_state(job_id)
                    state["status"] = CANCELLED
                    _save_state(job_id, state)
                _event(job_id, "warn", "确认取消，停止实际计算；保留当前最近验证包")
                return
            cand_report = set(kept_report)
            cand_item = set(kept_item)
            if unit["kind"] == "report_group":
                cand_report.discard(unit["report_key"])
                for ik in unit["item_keys"]:
                    cand_item.discard(ik)
            else:
                cand_item.discard(unit["item_key"])
            with tempfile.TemporaryDirectory(dir=str(jd), prefix="try-") as tmp:
                tmp_in = Path(tmp) / "input"
                _write_candidate_csvs(
                    tmp_in, spec, cand_report, cand_item, report_keys, item_keys)
                attempt = run_attempt(job_id, tmp_in, spec.timeout_seconds, cancel)
            with _lock:
                state = _load_state(job_id)
                state["attempts"] += 1
                state["last_attempt"] = attempt["status"]
                prefix = f"[{ui}/{len(units)}] {unit['label']}"
                if attempt["status"] == "cancelled":
                    state["status"] = CANCELLED
                    _save_state(job_id, state)
                    _event(job_id, "warn", f"{prefix}：取消生效，进程组已终止，不更换包")
                    return
                if attempt["status"] == "timeout":
                    state["timeouts"] += 1
                    pass_had_timeout = True
                    _save_state(job_id, state)
                    _event(job_id, "warn",
                           f"{prefix}：验证超时，跳过该候选（超时不代表无差异）")
                    continue
                if attempt["status"] == "error":
                    state["errors"] += 1
                    state["rejected"] += 1
                    _save_state(job_id, state)
                    _event(job_id, "info", f"{prefix}：查询失败，拒绝 {attempt['error']}")
                    continue
                if not attempt.get("different"):
                    state["rejected"] += 1
                    state["last_result"] = {k: attempt[k] for k in ("q1", "q2", "same")}
                    _save_state(job_id, state)
                    _event(job_id, "info",
                           f"{prefix}：两查询均成功但结果相同，保留该行/组")
                    continue
                # 接受：两查询均成功且结果不同
                kept_report, kept_item = cand_report, cand_item
                state["accepted"] += 1
                state["current_report_rows"] = len(kept_report)
                state["current_item_rows"] = len(kept_item)
                state["last_result"] = {k: attempt[k] for k in ("q1", "q2", "same")}
                _save_state(job_id, state)
            # 物化接受后的数据集并原子更换最近验证包
            accepted_dir = _csv_dir_from_sets(
                jd, spec, kept_report, kept_item, report_keys, item_keys)
            pkg = _build_zip(job_id, accepted_dir, f"pass{pass_no}-u{ui}")
            pass_accepted = True
            _event(job_id, "info",
                   f"{prefix}：差异保留，采用候选（报告 {len(kept_report)} / 检验项 {len(kept_item)}），"
                   f"已更新可下载包 {pkg.name}")
            # 本轮快照已失效，重新开始一轮
            break
        else:
            # for 正常结束 = 整轮没有任何接受
            state = _load_state(job_id)
            if pass_had_timeout:
                state["status"] = INCOMPLETE
                _save_state(job_id, state)
                _event(job_id, "error",
                       "本轮穷尽所有可行单元但存在超时：无法确认无差异，标记未完成，可稍后重试")
            else:
                state["status"] = COMPLETE
                _save_state(job_id, state)
                _event(job_id, "info",
                       "缩减完成：再删任一单行或任一报告联删组都无法保留差异")
            return
        if pass_accepted:
            continue


def _csv_dir_from_sets(jd: Path, spec: JobSpec, kept_report, kept_item,
                       report_keys, item_keys) -> Path:
    """把接受的候选集物化成稳定目录用于打包。"""
    out = jd / "input"
    _write_candidate_csvs(out, spec, kept_report, kept_item, report_keys, item_keys)
    return out


# ---------- 重启恢复 ----------
def recover_on_startup() -> None:
    ensure_dirs()
    for path in sorted(JOBS_DIR.iterdir()):
        if not path.is_dir():
            continue
        sp = path / "state.json"
        if not sp.exists():
            continue
        state = json.loads(sp.read_text(encoding="utf-8"))
        _jobs[state["job_id"]] = state
        if state["status"] not in TERMINAL:
            if state.get("cancel_requested"):
                state["status"] = CANCELLED
            else:
                state["status"] = INTERRUPTED
            _save_state(state["job_id"], state)
            _event(state["job_id"], "warn",
                   "服务重启中断了实际计算，作业标记为 INTERRUPTED，可重新启动；"
                   "最近验证过的包仍可下载")
