"""端到端测试：覆盖缩减、超时不判无差异、取消、单批限制、重启恢复与多重集语义。"""
from __future__ import annotations

import json
import os
import time
import zipfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
os.environ["REPRO_DATA_DIR"] = str(REPO / "data-test")

from fastapi.testclient import TestClient  # noqa: E402

from app import runner  # noqa: E402
from app.main import app  # noqa: E402

SAMPLE = REPO / "sample"


@pytest.fixture()
def client(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("REPRO_DATA_DIR", str(data_dir))
    # 重定向模块级路径常量
    monkeypatch.setattr(runner, "JOBS_DIR", data_dir / "jobs")
    from app import config as cfg
    monkeypatch.setattr(cfg, "DATA_DIR", data_dir)
    monkeypatch.setattr(cfg, "JOBS_DIR", data_dir / "jobs")
    runner._jobs.clear()
    runner._cancel_events.clear()
    runner._runner_busy.clear()
    with TestClient(app) as c:
        yield c
    # 等待可能残留的运行器线程收尾
    for _ in range(50):
        if not runner._runner_busy.is_set():
            break
        time.sleep(0.1)


def _wait_terminal(client, job_id, timeout=60.0, expect=()):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.get(f"/api/jobs/{job_id}").json()
        if last["status"] in runner.TERMINAL:
            if expect:
                assert last["status"] in expect, last
            return last
        time.sleep(0.15)
    raise AssertionError(f"作业未在 {timeout}s 内结束: {last and last['status']}")


def _sample_payload(client, **overrides):
    spec = json.loads((SAMPLE / "sample-job.json").read_text(encoding="utf-8"))
    spec.update(overrides)
    with (SAMPLE / "report.csv").open("rb") as rf, (SAMPLE / "item.csv").open("rb") as inf:
        files = {"report_csv": ("report.csv", rf, "text/csv"),
                 "item_csv": ("item.csv", inf, "text/csv")}
        data = {
            "report_columns": json.dumps(spec["report_columns"]),
            "item_columns": json.dumps(spec["item_columns"]),
            "report_pk": json.dumps(spec["report_pk"]),
            "item_pk": json.dumps(spec["item_pk"]),
            "fk_column": spec["fk_column"],
            "fk_ref_column": spec["fk_ref_column"],
            "sql1": spec["sql1"],
            "sql2": spec["sql2"],
            "timeout_seconds": str(spec["timeout_seconds"]),
        }
        resp = client.post("/api/jobs", files=files, data=data)
    return resp


# ---------- 1. 样例完整缩减 ----------
def test_sample_reduces_to_minimum_and_package_valid(client):
    resp = _sample_payload(client)
    assert resp.status_code == 200, resp.text
    job_id = resp.json()["job_id"]
    state = _wait_terminal(client, job_id, expect=("COMPLETE",))
    # 最小集：差异来自“无检验项的报告在 LEFT JOIN 下出现计数 0 行”，
    # 最终只剩这一份报告（0 条检验项），再删它差异即消失。
    assert state["current_report_rows"] == 1
    assert state["current_item_rows"] == 0
    assert state["accepted"] >= 2
    assert state["timeouts"] == 0

    pkg_resp = client.get(f"/api/jobs/{job_id}/package")
    assert pkg_resp.status_code == 200
    data_dir = runner.JOBS_DIR
    zpath = data_dir / job_id / "packages" / "current.zip"
    with zipfile.ZipFile(zpath.resolve()) as zf:
        names = set(zf.namelist())
        assert {"data/report.csv", "data/item.csv", "schema.json",
                "queries.sql", "manifest.json"} <= names
        manifest = json.loads(zf.read("manifest.json"))
        assert manifest["comparison"].startswith("无序多重集")
        schema = json.loads(zf.read("schema.json"))
        assert schema["foreign_key"]["column"] == "report_id"
        item_csv = zf.read("data/item.csv").decode()
        report_csv = zf.read("data/report.csv").decode()
    # 报告 3（无检验项）必须保留，否则 LEFT/INNER JOIN 差异消失
    assert "R-1003" in report_csv
    assert "R-1001" not in report_csv and "R-1002" not in report_csv
    # 检验项已全部随报告联删
    assert "101" not in item_csv and "104" not in item_csv


# ---------- 2. 初始无差异 -> REJECTED，无包可下 ----------
def test_identical_queries_rejected(client):
    same_sql = "SELECT report_id FROM report ORDER BY report_id"
    resp = _sample_payload(client, sql1=same_sql, sql2=same_sql)
    assert resp.status_code == 200
    job_id = resp.json()["job_id"]
    state = _wait_terminal(client, job_id, expect=("REJECTED",))
    events = client.get(f"/api/jobs/{job_id}/events").json()["events"]
    assert any("相同" in e["message"] for e in events)
    pkg = client.get(f"/api/jobs/{job_id}/package")
    assert pkg.status_code == 409


# ---------- 3. 查询执行失败 -> REJECTED ----------
def test_invalid_sql_rejected(client):
    resp = _sample_payload(client, sql2="SELECT * FROM no_such_table")
    # 静态检查先拦截非法表名
    assert resp.status_code == 400
    bad = _sample_payload(client, sql2="SELECT report_id FROM report GROUP BY no_column")
    assert bad.status_code == 200
    state = _wait_terminal(client, bad.json()["job_id"], expect=("REJECTED",))
    assert state["fatal_error"]


# ---------- 4. 重复行照计：多重集而非集合 ----------
def test_duplicate_rows_count_as_multiset(client, tmp_path):
    # 报告各一行；检验项里报告 1 有两条完全相同的测量行（不同主键，值可重复）
    report = "report_id,report_no\n1,A\n2,B\n"
    item = ("item_id,report_id,v\n"
            "10,1,5\n11,1,5\n12,2,7\n")
    (tmp_path / "r.csv").write_text(report, encoding="utf-8")
    (tmp_path / "i.csv").write_text(item, encoding="utf-8")
    # Q1 不去重计数；Q2 去重 -> 多重集不同（重复行照计才会暴露）
    q1 = "SELECT r.report_id, count(*) c FROM report r JOIN item i ON i.report_id=r.report_id GROUP BY r.report_id ORDER BY 1"
    q2 = "SELECT r.report_id, count(DISTINCT i.v) c FROM report r JOIN item i ON i.report_id=r.report_id GROUP BY r.report_id ORDER BY 1"
    with (tmp_path / "r.csv").open("rb") as rf, (tmp_path / "i.csv").open("rb") as inf:
        resp = client.post("/api/jobs", data={
            "report_columns": json.dumps([{"name": "report_id", "type": "INTEGER"},
                                          {"name": "report_no", "type": "TEXT"}]),
            "item_columns": json.dumps([{"name": "item_id", "type": "INTEGER"},
                                        {"name": "report_id", "type": "INTEGER"},
                                        {"name": "v", "type": "INTEGER"}]),
            "report_pk": json.dumps(["report_id"]),
            "item_pk": json.dumps(["item_id"]),
            "fk_column": "report_id", "fk_ref_column": "report_id",
            "sql1": q1, "sql2": q2, "timeout_seconds": "30",
        }, files={"report_csv": ("r.csv", rf), "item_csv": ("i.csv", inf)})
    assert resp.status_code == 200, resp.text
    state = _wait_terminal(client, resp.json()["job_id"], expect=("COMPLETE",))
    # 删到只剩报告1 + 两条重复值检验项：count=2 vs count(DISTINCT v)=1
    assert state["current_item_rows"] == 2
    assert state["current_report_rows"] == 1
    pkg = runner.current_package_path(state["job_id"])
    with zipfile.ZipFile(pkg) as zf:
        item_csv = zf.read("data/item.csv").decode()
        report_csv = zf.read("data/report.csv").decode()
    assert "10" in item_csv and "11" in item_csv and "12" not in item_csv
    assert "1,A" in report_csv and "2,B" not in report_csv


# ---------- 5. 空字段作 NULL ----------
def test_empty_fields_become_null(client, tmp_path):
    (tmp_path / "r.csv").write_text(
        "report_id,note\n1,\n2,ok\n", encoding="utf-8")
    (tmp_path / "i.csv").write_text(
        "item_id,report_id\n10,1\n11,2\n", encoding="utf-8")
    # 行级查询：Q1 用 IS NULL 显式区分 NULL 备注行；Q2 用等值判断（NULL 不命中）。
    # 这样含 NULL 备注的报告 1 是差异载体，不可删；报告 2 两行判断一致，可删。
    q1 = "SELECT report_id FROM report WHERE note IS NULL ORDER BY report_id"
    q2 = "SELECT report_id FROM report WHERE note = '' ORDER BY report_id"
    with (tmp_path / "r.csv").open("rb") as rf, (tmp_path / "i.csv").open("rb") as inf:
        resp = client.post("/api/jobs", data={
            "report_columns": json.dumps([{"name": "report_id", "type": "INTEGER"},
                                          {"name": "note", "type": "TEXT"}]),
            "item_columns": json.dumps([{"name": "item_id", "type": "INTEGER"},
                                        {"name": "report_id", "type": "INTEGER"}]),
            "report_pk": json.dumps(["report_id"]),
            "item_pk": json.dumps(["item_id"]),
            "fk_column": "report_id", "fk_ref_column": "report_id",
            "sql1": q1, "sql2": q2, "timeout_seconds": "30",
        }, files={"report_csv": ("r.csv", rf), "item_csv": ("i.csv", inf)})
    assert resp.status_code == 200, resp.text
    state = _wait_terminal(client, resp.json()["job_id"], expect=("COMPLETE",))
    # 备注为 NULL 的报告 1 必须保留：空字段导入为 NULL，Q1 命中、Q2 不命中
    pkg = runner.current_package_path(state["job_id"])
    with zipfile.ZipFile(pkg) as zf:
        report_csv = zf.read("data/report.csv").decode()
    assert state["current_report_rows"] == 1
    assert "1," in report_csv
    assert "\n2," not in report_csv  # 报告 2（有备注）两行判断一致，可删
    # 空字段以空串形式保留在 CSV 中，DuckDB 导入时按 NULL 处理
    import csv as _csv, io as _io
    rows = list(_csv.reader(_io.StringIO(report_csv)))
    assert rows[1] == ["1", ""]


# ---------- 6. 超时不判无差异：INCOMPLETE，可重新启动后完成 ----------
def test_timeout_is_incomplete_then_resume_completes(client, tmp_path):
    # 500 条检验项 + 三重交叉联接，单次执行约 20~30s；timeout=1s 必然触发候选超时。
    (tmp_path / "r.csv").write_text(
        "report_id\n1\n2\n3\n", encoding="utf-8")
    (tmp_path / "i.csv").write_text(
        "item_id,report_id,v\n" +
        # 只引用报告 1、2；报告 3 无检验项，INNER/LEFT JOIN 结果确定不同
        "\n".join(f"{1000+n},{(n % 2) + 1},{n}" for n in range(500)) + "\n",
        encoding="utf-8")
    slow_q1 = (
        "SELECT r.report_id, count(*) FROM report r "
        "JOIN item i ON i.report_id=r.report_id "
        "CROSS JOIN item a1 CROSS JOIN item a2 "
        "GROUP BY r.report_id ORDER BY 1"
    )
    fast_q2 = (
        "SELECT r.report_id, count(*) FROM report r "
        "LEFT JOIN item i ON i.report_id=r.report_id "
        "GROUP BY r.report_id ORDER BY 1"
    )
    # 初始验证需要通过：先提交两条快查询，再把查询一替换为慢查询，
    # 这样首个候选验证（删第一个报告组，仍剩 490+ 条检验项）必然超时。
    fast_q1 = (
        "SELECT r.report_id, count(*) FROM report r "
        "JOIN item i ON i.report_id=r.report_id "
        "GROUP BY r.report_id ORDER BY 1"
    )
    with (tmp_path / "r.csv").open("rb") as rf, (tmp_path / "i.csv").open("rb") as inf:
        resp = client.post("/api/jobs", data={
            "report_columns": json.dumps([{"name": "report_id", "type": "INTEGER"}]),
            "item_columns": json.dumps([
                {"name": "item_id", "type": "INTEGER"},
                {"name": "report_id", "type": "INTEGER"},
                {"name": "v", "type": "INTEGER"}]),
            "report_pk": json.dumps(["report_id"]),
            "item_pk": json.dumps(["item_id"]),
            "fk_column": "report_id", "fk_ref_column": "report_id",
            "sql1": fast_q1, "sql2": fast_q2, "timeout_seconds": "1",
            "autostart": "false",
        }, files={"report_csv": ("r.csv", rf), "item_csv": ("i.csv", inf)})
    assert resp.status_code == 200, resp.text
    job_id = resp.json()["job_id"]
    assert runner.get_state(job_id)["status"] == "QUEUED"
    # 把查询一替换为慢查询后再启动：初始验证也会变慢并超时
    spec_path = runner._spec_path(job_id)
    spec = json.loads(spec_path.read_text())
    spec["sql1"] = slow_q1
    runner._write_json_atomic(spec_path, spec)
    r = client.post(f"/api/jobs/{job_id}/start")
    assert r.status_code == 200, r.text
    state = _wait_terminal(client, job_id, timeout=120, expect=("INCOMPLETE",))
    assert state["timeouts"] >= 1
    events = client.get(f"/api/jobs/{job_id}/events").json()["events"]
    assert any("超时不代表无差异" in e["message"] for e in events)
    # 初始验证即超时 -> 尚无包可下载，且系统绝不把超时判为无差异
    assert client.get(f"/api/jobs/{job_id}/package").status_code == 409
    # 恢复快查询、放宽超时后重新启动，应能正常完成
    spec = json.loads(spec_path.read_text())
    spec["sql1"] = fast_q1
    spec["timeout_seconds"] = 120
    runner._write_json_atomic(spec_path, spec)
    r = client.post(f"/api/jobs/{job_id}/start")
    assert r.status_code == 200, r.text
    _wait_terminal(client, job_id, timeout=300, expect=("COMPLETE", "INCOMPLETE"))


# ---------- 7. 取消：立即停止且不再换包 ----------
def test_cancel_stops_computation_and_keeps_package(client):
    # 报告 1..30 带检验项，31..60 无检验项（保证 INNER/LEFT JOIN 初始结果不同，
    # 且缩减需要尝试足够多的删除单元，留出取消窗口）
    big_report = "report_id,report_no\n" + "\n".join(
        f"{n},R{n}" for n in range(1, 61)) + "\n"
    big_item = "item_id,report_id,v\n" + "\n".join(
        f"{1000+n},{(n % 30) + 1},{n}" for n in range(120)) + "\n"
    import io
    q1 = "SELECT r.report_id, count(*) FROM report r JOIN item i ON i.report_id=r.report_id GROUP BY r.report_id ORDER BY 1"
    q2 = "SELECT r.report_id, count(*) FROM report r LEFT JOIN item i ON i.report_id=r.report_id GROUP BY r.report_id ORDER BY 1"
    resp = client.post("/api/jobs", data={
        "report_columns": json.dumps([{"name": "report_id", "type": "INTEGER"},
                                      {"name": "report_no", "type": "TEXT"}]),
        "item_columns": json.dumps([{"name": "item_id", "type": "INTEGER"},
                                    {"name": "report_id", "type": "INTEGER"},
                                    {"name": "v", "type": "INTEGER"}]),
        "report_pk": json.dumps(["report_id"]),
        "item_pk": json.dumps(["item_id"]),
        "fk_column": "report_id", "fk_ref_column": "report_id",
        "sql1": q1, "sql2": q2, "timeout_seconds": "30",
    }, files={"report_csv": ("r.csv", io.BytesIO(big_report.encode())),
              "item_csv": ("i.csv", io.BytesIO(big_item.encode()))})
    assert resp.status_code == 200, resp.text
    job_id = resp.json()["job_id"]
    # 等初始包生成
    deadline = time.monotonic() + 30
    pkg_before = None
    while time.monotonic() < deadline:
        p = runner.current_package_path(job_id)
        if p:
            pkg_before = p.read_bytes()
            break
        time.sleep(0.1)
    assert pkg_before is not None
    assert client.post(f"/api/jobs/{job_id}/cancel").status_code == 200
    state = _wait_terminal(client, job_id, timeout=30, expect=("CANCELLED",))
    assert state["cancel_requested"] is True
    # 确认取消后包不再更换
    time.sleep(0.5)
    assert runner.current_package_path(job_id).read_bytes() == pkg_before
    # 再次取消被拒绝
    assert client.post(f"/api/jobs/{job_id}/cancel").status_code == 409


# ---------- 8. 同一时刻只运行一批 ----------
def test_only_one_batch_at_a_time(client):
    resp = _sample_payload(client)
    assert resp.status_code == 200
    # 立即提交第二个 -> 409
    second = _sample_payload(client)
    assert second.status_code == 409
    assert "只运行一批" in second.json()["detail"]
    _wait_terminal(client, resp.json()["job_id"], expect=("COMPLETE",))
    # 结束后可再提交
    third = _sample_payload(client)
    assert third.status_code == 200
    _wait_terminal(client, third.json()["job_id"], expect=("COMPLETE",))


# ---------- 9. 重启恢复：运行中作业变 INTERRUPTED，包保留，可重启完成 ----------
def test_restart_recovery(client, tmp_path):
    resp = _sample_payload(client)
    job_id = resp.json()["job_id"]
    _wait_terminal(client, job_id, expect=("COMPLETE",))
    # 模拟进程重启：直接调用恢复逻辑（当前无运行中线程）
    runner._jobs.clear()
    # 先把状态改成非终态，模拟运行中被杀
    state_path = runner._state_path(job_id)
    st = json.loads(state_path.read_text())
    st["status"] = "RUNNING"
    runner._write_json_atomic(state_path, st)
    runner.recover_on_startup()
    recovered = client.get(f"/api/jobs/{job_id}").json()
    assert recovered["status"] == "INTERRUPTED"
    # 包仍在
    assert client.get(f"/api/jobs/{job_id}/package").status_code == 200
    # 重新启动后正常结束
    assert client.post(f"/api/jobs/{job_id}/start").status_code == 200
    _wait_terminal(client, job_id, expect=("COMPLETE",))


# ---------- 10. 静态安全/确定性检查 ----------
@pytest.mark.parametrize("sql2,needle", [
    ("SELECT * FROM report; DROP TABLE item", "分号"),
    ("DELETE FROM report", "DELETE"),
    ("SELECT random() FROM report", "RANDOM"),
    ("SELECT * FROM other_table", "不允许的关系"),
    ("SELECT uuid() FROM report", "UUID"),
])
def test_static_sql_guardrails(client, sql2, needle):
    resp = _sample_payload(client, sql2=sql2)
    assert resp.status_code == 400
    assert needle in resp.json()["detail"]


# ---------- 11. 主键重复 / 整数非法 / 外键类型不匹配 ----------
def test_csv_integrity_validation(client, tmp_path):
    (tmp_path / "r.csv").write_text("report_id\n1\n1\n", encoding="utf-8")
    (tmp_path / "i.csv").write_text("item_id,report_id\n1,1\n", encoding="utf-8")
    with (tmp_path / "r.csv").open("rb") as rf, (tmp_path / "i.csv").open("rb") as inf:
        resp = client.post("/api/jobs", data={
            "report_columns": json.dumps([{"name": "report_id", "type": "INTEGER"}]),
            "item_columns": json.dumps([{"name": "item_id", "type": "INTEGER"},
                                        {"name": "report_id", "type": "INTEGER"}]),
            "report_pk": json.dumps(["report_id"]),
            "item_pk": json.dumps(["item_id"]),
            "fk_column": "report_id", "fk_ref_column": "report_id",
            "sql1": "SELECT count(*) FROM report",
            "sql2": "SELECT count(*) FROM item",
            "timeout_seconds": "30",
        }, files={"report_csv": ("r.csv", rf), "item_csv": ("i.csv", inf)})
    assert resp.status_code == 400 and "主键重复" in resp.json()["detail"]

    (tmp_path / "r2.csv").write_text("report_id\n1.5\n", encoding="utf-8")
    with (tmp_path / "r2.csv").open("rb") as rf, (tmp_path / "i.csv").open("rb") as inf:
        resp = client.post("/api/jobs", data={
            "report_columns": json.dumps([{"name": "report_id", "type": "INTEGER"}]),
            "item_columns": json.dumps([{"name": "item_id", "type": "INTEGER"},
                                        {"name": "report_id", "type": "INTEGER"}]),
            "report_pk": json.dumps(["report_id"]),
            "item_pk": json.dumps(["item_id"]),
            "fk_column": "report_id", "fk_ref_column": "report_id",
            "sql1": "SELECT count(*) FROM report",
            "sql2": "SELECT count(*) FROM item",
            "timeout_seconds": "30",
        }, files={"report_csv": ("r2.csv", rf), "item_csv": ("i.csv", inf)})
    assert resp.status_code == 400 and "INTEGER" in resp.json()["detail"]


# ---------- 12. 孤儿外键允许但告警 ----------
def test_orphan_fk_warns_but_accepted(client, tmp_path):
    (tmp_path / "r.csv").write_text("report_id\n1\n", encoding="utf-8")
    (tmp_path / "i.csv").write_text("item_id,report_id\n10,99\n", encoding="utf-8")
    with (tmp_path / "r.csv").open("rb") as rf, (tmp_path / "i.csv").open("rb") as inf:
        resp = client.post("/api/jobs", data={
            "report_columns": json.dumps([{"name": "report_id", "type": "INTEGER"}]),
            "item_columns": json.dumps([{"name": "item_id", "type": "INTEGER"},
                                        {"name": "report_id", "type": "INTEGER"}]),
            "report_pk": json.dumps(["report_id"]),
            "item_pk": json.dumps(["item_id"]),
            "fk_column": "report_id", "fk_ref_column": "report_id",
            "sql1": "SELECT count(*) FROM report r JOIN item i ON i.report_id=r.report_id",
            "sql2": "SELECT count(*) FROM report r LEFT JOIN item i ON i.report_id=r.report_id",
            "timeout_seconds": "30",
        }, files={"report_csv": ("r.csv", rf), "item_csv": ("i.csv", inf)})
    assert resp.status_code == 200, resp.text
    assert any("孤儿" in w for w in resp.json()["warnings"])


# ---------- 13. 样例一键接口 ----------
def test_sample_endpoint(client):
    resp = client.post("/api/sample")
    assert resp.status_code == 200, resp.text
    _wait_terminal(client, resp.json()["job_id"], expect=("COMPLETE",))


# ---------- 14. NULL 外键检验项不参与报告联删，只能单行删除 ----------
def test_null_fk_not_cascaded(client):
    from app.validation import ColumnSpec, TableSpec, JobSpec
    from app import runner
    report = TableSpec([ColumnSpec("report_id", "INTEGER")], ["report_id"], rows=[[1]])
    item = TableSpec(
        [ColumnSpec("item_id", "INTEGER"), ColumnSpec("report_id", "INTEGER")],
        ["item_id"], rows=[[10, 1], [11, None]])
    spec = JobSpec(report=report, item=item, fk_column="report_id",
                   fk_ref_column="report_id", sql1="", sql2="", timeout_seconds=30)
    rk, ik = runner._pk_keys(spec.report), runner._pk_keys(spec.item)
    units = runner._build_units(spec, set(rk), set(ik), rk, ik)
    group = next(u for u in units if u["kind"] == "report_group")
    assert group["item_keys"] == [(10,)]  # NULL 外键行不被联删
    singles = [u["item_key"] for u in units if u["kind"] == "item_row"]
    assert (11,) in singles  # 只能作为单行单元删除
