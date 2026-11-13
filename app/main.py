"""FastAPI 入口：上传建作业、启动/取消/状态、下载最近验证包。"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import runner
from .config import ensure_dirs
from .validation import SpecError, validate_job_payload

WEB_DIR = Path(__file__).resolve().parent / "web"
SAMPLE_DIR = Path(__file__).resolve().parent.parent / "sample"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_dirs()
    runner.recover_on_startup()
    yield


app = FastAPI(title="SQL 差异最小复现平台", version="1.0.0", lifespan=lifespan)


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "busy": runner._runner_busy.is_set()}


@app.post("/api/jobs")
async def create_job(
    report_csv: UploadFile = File(...),
    item_csv: UploadFile = File(...),
    report_columns: str = Form(...),
    item_columns: str = Form(...),
    report_pk: str = Form(...),
    item_pk: str = Form(...),
    fk_column: str = Form(...),
    fk_ref_column: str = Form(...),
    sql1: str = Form(...),
    sql2: str = Form(...),
    timeout_seconds: float = Form(30.0),
    autostart: bool = Form(True),
):
    if runner._runner_busy.is_set():
        raise HTTPException(409, "已有一批缩减正在运行，同一时刻只运行一批；请等待或取消后再提交")
    try:
        r_cols = _parse_json_list(report_columns, "report_columns")
        i_cols = _parse_json_list(item_columns, "item_columns")
        r_pk = _parse_json_list(report_pk, "report_pk", expect_str=True)
        i_pk = _parse_json_list(item_pk, "item_pk", expect_str=True)
    except SpecError as exc:
        raise HTTPException(400, str(exc)) from exc
    report_raw = await report_csv.read()
    item_raw = await item_csv.read()
    try:
        spec, fk_info = validate_job_payload(
            report_raw, item_raw, r_cols, i_cols, r_pk, i_pk,
            fk_column, fk_ref_column, sql1, sql2, timeout_seconds,
        )
    except SpecError as exc:
        raise HTTPException(400, f"输入校验失败: {exc}") from exc
    job_id = runner.create_job(spec, fk_info["warnings"])
    if autostart:
        try:
            runner.start_job(job_id)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
    return {"job_id": job_id, "warnings": fk_info["warnings"],
            "status_url": f"/api/jobs/{job_id}"}


def _parse_json_list(raw: str, field: str, expect_str: bool = False) -> list:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SpecError(f"{field} 不是合法 JSON: {exc}") from exc
    if not isinstance(value, list) or not value:
        raise SpecError(f"{field} 必须是非空 JSON 数组")
    if expect_str and not all(isinstance(x, str) and x.strip() for x in value):
        raise SpecError(f"{field} 必须是字符串数组")
    return value


@app.get("/api/jobs")
def jobs() -> JSONResponse:
    return JSONResponse(runner.list_jobs())


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str) -> dict:
    state = runner.get_state(job_id)
    if not state:
        raise HTTPException(404, "作业不存在")
    return state


@app.get("/api/jobs/{job_id}/events")
def job_events(job_id: str) -> dict:
    if not runner.get_state(job_id):
        raise HTTPException(404, "作业不存在")
    return {"events": runner.read_events(job_id)}


@app.post("/api/jobs/{job_id}/start")
def job_start(job_id: str) -> dict:
    try:
        runner.start_job(job_id)
    except KeyError as exc:
        raise HTTPException(404, "作业不存在") from exc
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"status": "started"}


@app.post("/api/jobs/{job_id}/cancel")
def job_cancel(job_id: str) -> dict:
    if not runner.get_state(job_id):
        raise HTTPException(404, "作业不存在")
    ok = runner.request_cancel(job_id)
    if not ok:
        raise HTTPException(409, "作业已结束，无法取消")
    return {"status": "cancel_requested"}


@app.get("/api/jobs/{job_id}/package")
def job_package(job_id: str):
    if not runner.get_state(job_id):
        raise HTTPException(404, "作业不存在")
    pkg = runner.current_package_path(job_id)
    if not pkg:
        raise HTTPException(409, "尚无验证通过的包（初始差异未确认前不可下载）")
    return FileResponse(
        str(pkg), filename=pkg.name,
        media_type="application/zip",
        headers={"Cache-Control": "no-store"},
    )


@app.post("/api/sample")
def load_sample():
    """载入本地差异样例并自动开始缩减，便于一键体验。"""
    if runner._runner_busy.is_set():
        raise HTTPException(409, "已有一批缩减正在运行，同一时刻只运行一批")
    payload = json.loads((SAMPLE_DIR / "sample-job.json").read_text(encoding="utf-8"))
    report_raw = (SAMPLE_DIR / "report.csv").read_bytes()
    item_raw = (SAMPLE_DIR / "item.csv").read_bytes()
    try:
        spec, fk_info = validate_job_payload(
            report_raw, item_raw,
            payload["report_columns"], payload["item_columns"],
            payload["report_pk"], payload["item_pk"],
            payload["fk_column"], payload["fk_ref_column"],
            payload["sql1"], payload["sql2"], payload["timeout_seconds"],
        )
    except SpecError as exc:
        raise HTTPException(400, str(exc)) from exc
    job_id = runner.create_job(spec, fk_info["warnings"])
    runner.start_job(job_id)
    return {"job_id": job_id, "status_url": f"/api/jobs/{job_id}"}


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse((WEB_DIR / "index.html").read_text(encoding="utf-8"))


app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")
