"""CSV 解析与列定义/主键/外键校验（纯标准库，不执行任何用户 SQL）。"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from typing import Optional

from .config import ALLOWED_TYPES, FORBIDDEN_KEYWORDS, NON_DETERMINISTIC_TOKENS, TABLES

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_INT_RE = re.compile(r"^[+-]?\d+$")
_WORD_RE_TMPL = r"(?<![A-Za-z0-9_])(?:{tokens})(?![A-Za-z0-9_])"


class SpecError(ValueError):
    """用户输入（CSV / 列定义 / SQL 静态形式）不合法。"""


@dataclass
class ColumnSpec:
    name: str
    type: str  # INTEGER | TEXT


@dataclass
class TableSpec:
    columns: list[ColumnSpec]
    primary_key: list[str]
    rows: list[list[Optional[object]]] = field(default_factory=list)

    @property
    def colnames(self) -> list[str]:
        return [c.name for c in self.columns]


@dataclass
class JobSpec:
    report: TableSpec
    item: TableSpec
    # item 表上的报告号外键：(item 列名, report 主键列名)
    fk_column: str
    fk_ref_column: str
    sql1: str
    sql2: str
    timeout_seconds: float


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_ident(name: str) -> str:
    """供建表/写数据使用的安全标识符引用。"""
    if not _IDENT_RE.match(name):
        raise SpecError(f"非法列名: {name!r}（仅允许字母/下划线开头的字母数字下划线组合）")
    return _quote_ident(name)


def read_csv_rows(raw: bytes, source: str) -> tuple[list[str], list[list[str]]]:
    """读取 CSV，返回表头与字符串行。空字段统一为空串（后续按 NULL 处理）。"""
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SpecError(f"{source} 不是有效的 UTF-8 文本: {exc}") from exc
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise SpecError(f"{source} 为空，缺少表头")
    header = [h.strip() for h in header]
    if not header or any(h == "" for h in header):
        raise SpecError(f"{source} 表头存在空列名: {header}")
    if len(set(header)) != len(header):
        raise SpecError(f"{source} 表头存在重复列名: {header}")
    rows: list[list[str]] = []
    for line_no, row in enumerate(reader, start=2):
        # 跳过完全空白的行（CSV 里常见的尾部空行）
        if len(row) == 1 and row[0] == "":
            continue
        if len(row) != len(header):
            raise SpecError(
                f"{source} 第 {line_no} 行字段数 {len(row)} 与表头 {len(header)} 不一致"
            )
        rows.append(row)
    return header, rows


def _coerce_value(raw: str, col_type: str, table: str, col: str, line_no: int):
    # 空字段（含纯空白）一律作 NULL
    if raw.strip() == "":
        return None
    if col_type == "INTEGER":
        if not _INT_RE.match(raw.strip()):
            raise SpecError(
                f"{table} 第 {line_no} 行列 {col!r} 声明为 INTEGER，"
                f"但值 {raw!r} 不是整数"
            )
        return int(raw.strip())
    return raw  # TEXT 原样保留（含首尾空格；空白串视为 NULL）


def build_table_spec(
    raw: bytes,
    source: str,
    columns: list[dict],
    primary_key: list[str],
) -> TableSpec:
    header, str_rows = read_csv_rows(raw, source)
    header_set = set(header)
    if not columns:
        raise SpecError(f"{source} 未声明任何列")
    col_specs: list[ColumnSpec] = []
    for c in columns:
        name = (c.get("name") or "").strip()
        ctype = (c.get("type") or "").strip().upper()
        if name not in header_set:
            raise SpecError(f"{source} 声明的列 {name!r} 不在 CSV 表头 {header} 中")
        if ctype not in ALLOWED_TYPES:
            raise SpecError(f"{source} 列 {name!r} 类型 {ctype!r} 不支持，仅允许 INTEGER/TEXT")
        col_specs.append(ColumnSpec(name=name, type=ctype))
    if len({c.name for c in col_specs}) != len(col_specs):
        raise SpecError(f"{source} 列声明存在重复")
    pk = [p.strip() for p in primary_key if p.strip()]
    if not pk:
        raise SpecError(f"{source} 必须声明主键")
    declared = {c.name for c in col_specs}
    for p in pk:
        if p not in declared:
            raise SpecError(f"{source} 主键列 {p!r} 未在列定义中声明")

    idx = {c.name: header.index(c.name) for c in col_specs}
    pk_positions = [next(i for i, c in enumerate(col_specs) if c.name == p) for p in pk]
    typed_rows: list[list[object]] = []
    seen_pk: set[tuple] = set()
    for line_no, srow in enumerate(str_rows, start=2):
        typed: list[object] = []
        for cs in col_specs:
            typed.append(_coerce_value(srow[idx[cs.name]], cs.type, source, cs.name, line_no))
        pk_values = tuple(typed[pos] for pos in pk_positions)
        if any(v is None for v in pk_values):
            raise SpecError(f"{source} 第 {line_no} 行主键存在 NULL，主键不可为空")
        if pk_values in seen_pk:
            raise SpecError(f"{source} 主键重复: {dict(zip(pk, pk_values))}")
        seen_pk.add(pk_values)
        typed_rows.append(typed)
    return TableSpec(columns=col_specs, primary_key=pk, rows=typed_rows)


def validate_fk(report: TableSpec, item: TableSpec, fk_column: str,
                fk_ref_column: str) -> dict:
    if fk_column not in item.colnames:
        raise SpecError(f"外键列 {fk_column!r} 不在检验项表列中")
    if fk_ref_column not in report.colnames:
        raise SpecError(f"外键引用列 {fk_ref_column!r} 不在报告表列中")
    fk_type = next(c.type for c in item.columns if c.name == fk_column)
    ref_type = next(c.type for c in report.columns if c.name == fk_ref_column)
    if fk_type != ref_type:
        raise SpecError(
            f"外键类型不匹配: item.{fk_column}({fk_type}) -> "
            f"report.{fk_ref_column}({ref_type})"
        )
    # 外键是“报告号”，语义上应引用报告主键（单列）；若引用的不是主键列则警告但放行
    warnings = []
    if fk_ref_column not in report.primary_key or len(report.primary_key) != 1:
        warnings.append(
            f"外键引用列 report.{fk_ref_column} 不是报告表的单列主键，"
            f"联删将按该列的值匹配执行"
        )
    # 统计孤儿行（不强制引用完整性，仅提示）
    ref_idx = report.colnames.index(fk_ref_column)
    ref_values = {r[ref_idx] for r in report.rows}
    fk_idx = item.colnames.index(fk_column)
    orphans = sum(1 for r in item.rows if r[fk_idx] is not None and r[fk_idx] not in ref_values)
    null_fk = sum(1 for r in item.rows if r[fk_idx] is None)
    if orphans:
        warnings.append(f"检验项中有 {orphans} 行的外键值在报告表中不存在（孤儿行，仍保留）")
    if null_fk:
        warnings.append(f"检验项中有 {null_fk} 行外键为空（联删报告时不会被带走）")
    return {"warnings": warnings, "orphans": orphans, "null_fk": null_fk}


def _strip_one_select(sql: str, label: str) -> str:
    s = (sql or "").strip()
    if not s:
        raise SpecError(f"{label} 为空")
    if s.endswith(";"):
        s = s[:-1].strip()
    if ";" in s:
        raise SpecError(f"{label} 只允许一条 SELECT 语句（检测到多余的分号）")
    return s


def static_check_sql(sql: str, label: str) -> str:
    """对 SQL 做静态安全/确定性检查；语义合法性交给 DuckDB 工作进程验证。"""
    s = _strip_one_select(sql, label)
    low = s.lower()
    for kw in FORBIDDEN_KEYWORDS:
        if re.search(_WORD_RE_TMPL.format(tokens=re.escape(kw)), low):
            raise SpecError(f"{label} 包含禁止的关键字 {kw.upper()}，只允许只读 SELECT")
    for tok in NON_DETERMINISTIC_TOKENS:
        if re.search(_WORD_RE_TMPL.format(tokens=re.escape(tok)), low):
            raise SpecError(f"{label} 使用了不确定函数/特性 {tok.upper()}，查询必须确定可复现")
    if not (low.startswith("select") or low.startswith("with")):
        raise SpecError(f"{label} 必须是 SELECT（或 WITH ... SELECT）查询")
    # 只能引用两张允许的表：扫描 from/join 后的标识符，拒绝其它表名/表函数。
    for m in re.finditer(r"(?is)\b(?:from|join)\s+([\"'\[`]?[A-Za-z_][A-Za-z0-9_]*)", s):
        name = m.group(1).strip("\"'[`")
        if name.lower() not in TABLES:
            raise SpecError(
                f"{label} 引用了不允许的关系 {name!r}，查询只能访问 report / item 两张表"
            )
    return s


def validate_job_payload(
    report_csv: bytes,
    item_csv: bytes,
    report_columns: list[dict],
    item_columns: list[dict],
    report_pk: list[str],
    item_pk: list[str],
    fk_column: str,
    fk_ref_column: str,
    sql1: str,
    sql2: str,
    timeout_seconds: float,
) -> tuple[JobSpec, dict]:
    report = build_table_spec(report_csv, "报告表 report.csv", report_columns, report_pk)
    item = build_table_spec(item_csv, "检验项表 item.csv", item_columns, item_pk)
    fk_info = validate_fk(report, item, fk_column.strip(), fk_ref_column.strip())
    q1 = static_check_sql(sql1, "查询一")
    q2 = static_check_sql(sql2, "查询二")
    if not (1 <= float(timeout_seconds) <= 3600):
        raise SpecError("单条候选验证超时时间需在 1~3600 秒之间")
    spec = JobSpec(
        report=report,
        item=item,
        fk_column=fk_column.strip(),
        fk_ref_column=fk_ref_column.strip(),
        sql1=q1,
        sql2=q2,
        timeout_seconds=float(timeout_seconds),
    )
    return spec, fk_info
