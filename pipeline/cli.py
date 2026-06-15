"""CLI 命令行界面"""
import os
import sys
import json
import click
from tabulate import tabulate

from .service import (
    PipelineService, BatchServiceError, BatchLockedError,
    SchemeError, SchemeConflictError, SchemeImportResult, SchemeCloneResult,
    SchemeDeriveResult, DryRunResult, DryRunRisk, SwitchSchemeResult,
    TicketError, TicketConflictError, TicketImportResult
)
from . import snapshot as snap
from .config import get_default_config, load_config

OK = "[OK]"
ERR = "[ERROR]"
WARN = "[!]"


def _configure_terminal_encoding():
    """配置终端编码容错，避免 Windows PowerShell GBK 下 UnicodeEncodeError。

    根因处理：将 stdout/stderr 用 errors='replace' 包装，遇到不可编码字符时
    用替代符而非抛出异常崩溃。同时对可预期的特殊字符使用 ASCII 友好形式。
    """
    if sys.platform == "win32":
        if sys.stdout.encoding and "utf" not in sys.stdout.encoding.lower():
            sys.stdout.reconfigure(errors="replace")
        if sys.stderr.encoding and "utf" not in sys.stderr.encoding.lower():
            sys.stderr.reconfigure(errors="replace")


def _get_service(db_path: str = None) -> PipelineService:
    return PipelineService(db_path)


def _print_table(rows, headers=None):
    if not rows:
        click.echo("(无数据)")
        return
    click.echo(tabulate(rows, headers=headers or "keys", tablefmt="simple"))


def _print_switch_result(sw: SwitchSchemeResult, label: str, is_dry_run: bool):
    """统一输出 scheme switch 结果（含 dry-run 和执行结果）。"""
    dr = sw.dry_run
    click.echo(f"=== 方案切换 {'[预检]' if is_dry_run else ''}：{label} ===")

    if dr:
        if dr.batch_id:
            click.echo(f"  目标批次:   #{dr.batch_id}" + (f" '{dr.batch_name}'" if dr.batch_name else ""))
            if dr.batch_locked:
                click.echo(f"             {WARN} 批次已锁定")
        if dr.current_scheme_id:
            click.echo(f"  当前方案:   #{dr.current_scheme_id}" +
                       (f" '{dr.current_scheme_name}'" if dr.current_scheme_name else "") +
                       (f" (v{dr.current_scheme_version})" if dr.current_scheme_version else ""))
            if dr.current_config_version:
                click.echo(f"  当前配置:   v{dr.current_config_version}")

        if sw.switch_type == SwitchSchemeResult.SWITCH_TYPE_ROLLBACK:
            if dr.scheme_id:
                click.echo(f"  回滚到方案: #{dr.scheme_id}" +
                           (f" '{dr.scheme_name}'" if dr.scheme_name else ""))
        else:
            if dr.scheme_id:
                click.echo(f"  待应用方案: #{dr.scheme_id}" +
                           (f" '{dr.scheme_name}'" if dr.scheme_name else "") +
                           (f" (v{dr.scheme_version})" if dr.scheme_version else ""))
            if dr.source_scheme_id:
                click.echo(f"  源方案:     #{dr.source_scheme_id}" +
                           (f" '{dr.source_scheme_name}'" if dr.source_scheme_name else ""))
            if dr.new_scheme_name:
                click.echo(f"  新方案名:   '{dr.new_scheme_name}'")

        if dr.new_config_version:
            click.echo(f"  新配置版本: v{dr.new_config_version}")

        click.echo()

        if not is_dry_run and not dr.can_proceed:
            click.echo(f"{ERR} 预检未通过，执行终止", err=True)
        elif is_dry_run and dr.can_proceed:
            click.echo(f"{OK} 预检通过，可以继续执行")
        elif is_dry_run and not dr.can_proceed:
            click.echo(f"{ERR} 预检未通过，无法继续执行", err=True)
        elif sw.success:
            click.echo(f"{OK} 切换成功")

        click.echo(f"  风险数量: {len(dr.risks)}")
        if dr.risks:
            click.echo("\n--- 风险详情 ---")
            for i, risk in enumerate(dr.risks, 1):
                severity_label = "阻止" if risk.severity == DryRunRisk.SEVERITY_BLOCKER else "警告"
                click.echo(f"  {i}. [{severity_label}] {risk.risk_type}: {risk.message}")
                if risk.details:
                    for k, v in risk.details.items():
                        click.echo(f"     {k}: {v}")

        if dr.config_diff and dr.can_proceed:
            click.echo("\n--- 配置变更预览 ---")
            cd = dr.config_diff
            if cd.get("version_change"):
                click.echo(f"  版本变化: v{cd['version_change']['old']} → v{cd['version_change']['new']}")
            if cd.get("added"):
                click.echo(f"  新增字段 ({len(cd['added'])}):")
                for k, v in cd["added"].items():
                    click.echo(f"    + {k} = {v}")
            if cd.get("modified"):
                click.echo(f"  修改字段 ({len(cd['modified'])}):")
                for k, v in cd["modified"].items():
                    click.echo(f"    ~ {k}: {v['old']} → {v['new']}")
            if cd.get("removed"):
                click.echo(f"  删除字段 ({len(cd['removed'])}):")
                for k, v in cd["removed"].items():
                    click.echo(f"    - {k} = {v}")

    if not is_dry_run and sw.success:
        if sw.rollback_result:
            rb = sw.rollback_result
            click.echo()
            click.echo(f"{OK} 配置回滚成功")
            click.echo(f"  原版本: v{rb.previous_config_version}")
            click.echo(f"  新版本: v{rb.new_config_version}")
            if rb.previous_scheme_id:
                click.echo(f"  回滚到方案: #{rb.previous_scheme_id}" +
                           (f" '{rb.previous_scheme_name}'" if rb.previous_scheme_name else ""))
        click.echo("  请执行 process 命令以使用新配置重跑该批次。")

    if sw.message:
        click.echo()
        if sw.success:
            click.echo(f"  提示: {sw.message}")
        else:
            click.echo(f"  原因: {sw.message}", err=True)

    click.echo()
    if not (sw.success or (is_dry_run and dr and dr.can_proceed)):
        sys.exit(1)


def _print_dry_run_enhanced(result: DryRunResult):
    """输出增强版 dry-run 信息。"""
    click.echo(f"=== Dry-Run 检查结果 ===")
    if result.batch_id:
        click.echo(f"  目标批次:   #{result.batch_id}" +
                   (f" '{result.batch_name}'" if result.batch_name else ""))
        if result.batch_locked:
            click.echo(f"             {WARN} 批次已锁定")
    if result.current_scheme_id:
        click.echo(f"  当前方案:   #{result.current_scheme_id}" +
                   (f" '{result.current_scheme_name}'" if result.current_scheme_name else "") +
                   (f" (v{result.current_scheme_version})" if result.current_scheme_version else ""))
    if result.current_config_version:
        click.echo(f"  当前配置:   v{result.current_config_version}")
    if result.scheme_id:
        click.echo(f"  待应用方案: #{result.scheme_id}" +
                   (f" '{result.scheme_name}'" if result.scheme_name else "") +
                   (f" (v{result.scheme_version})" if result.scheme_version else ""))
    if result.source_scheme_id:
        click.echo(f"  源方案:     #{result.source_scheme_id}" +
                   (f" '{result.source_scheme_name}'" if result.source_scheme_name else ""))
    if result.new_scheme_name:
        click.echo(f"  新方案名:   '{result.new_scheme_name}'")
    if result.new_config_version:
        click.echo(f"  新配置版本: v{result.new_config_version}")
    click.echo()

    if result.can_proceed:
        click.echo(f"{OK} 检查通过，可以继续执行")
    else:
        click.echo(f"{ERR} 检查未通过，无法继续执行", err=True)

    click.echo(f"  风险数量: {len(result.risks)}")
    if result.risks:
        click.echo("\n--- 风险详情 ---")
        for i, risk in enumerate(result.risks, 1):
            severity_label = "阻止" if risk.severity == DryRunRisk.SEVERITY_BLOCKER else "警告"
            click.echo(f"  {i}. [{severity_label}] {risk.risk_type}: {risk.message}")
            if risk.details:
                for k, v in risk.details.items():
                    click.echo(f"     {k}: {v}")

    if result.config_diff and result.can_proceed:
        click.echo("\n--- 配置变更预览 ---")
        cd = result.config_diff
        if cd.get("version_change"):
            click.echo(f"  版本变化: v{cd['version_change']['old']} → v{cd['version_change']['new']}")
        if cd.get("added"):
            click.echo(f"  新增字段 ({len(cd['added'])}):")
            for k, v in cd["added"].items():
                click.echo(f"    + {k} = {v}")
        if cd.get("modified"):
            click.echo(f"  修改字段 ({len(cd['modified'])}):")
            for k, v in cd["modified"].items():
                click.echo(f"    ~ {k}: {v['old']} → {v['new']}")
        if cd.get("removed"):
            click.echo(f"  删除字段 ({len(cd['removed'])}):")
            for k, v in cd["removed"].items():
                click.echo(f"    - {k} = {v}")

    click.echo()
    if not result.can_proceed:
        sys.exit(1)


@click.group()
@click.option("--db", "db_path", default=None, help="数据库文件路径")
@click.pass_context
def cli(ctx, db_path):
    """实验数据处理流水线 CLI"""
    _configure_terminal_encoding()
    ctx.ensure_object(dict)
    ctx.obj["db_path"] = db_path


# ========== 批次管理 ==========

@cli.command("create")
@click.argument("name")
@click.argument("csv_path", type=click.Path(exists=True))
@click.option("--config", "config_path", type=click.Path(exists=True), help="自定义配置文件路径")
@click.pass_context
def create_batch(ctx, name, csv_path, config_path):
    """创建新数据批次"""
    try:
        cfg = load_config(config_path) if config_path else get_default_config()
        svc = _get_service(ctx.obj.get("db_path"))
        batch_id = svc.create_batch(name, csv_path, cfg)
        click.echo(f"{OK} 批次创建成功: ID={batch_id}, 名称='{name}'")
    except (BatchServiceError, FileNotFoundError) as e:
        click.echo(f"{ERR} 错误: {e}", err=True)
        sys.exit(1)


@cli.command("list")
@click.pass_context
def list_batches(ctx):
    """列出所有批次"""
    svc = _get_service(ctx.obj.get("db_path"))
    batches = svc.list_batches()
    if not batches:
        click.echo("(暂无批次)")
        return
    rows = []
    for b in batches:
        scheme_info = f"#{b['current_scheme_id']}" if b.get("current_scheme_id") else "-"
        rows.append({
            "ID": b["id"],
            "名称": b["name"],
            "状态": b["status"],
            "锁定": "是" if b["locked"] else "否",
            "配置版本": b["config_version"],
            "当前方案": scheme_info,
            "源文件": os.path.basename(b["source_file"]),
            "更新时间": b["updated_at"][:19]
        })
    _print_table(rows)


@cli.command("show")
@click.argument("batch_id", type=int)
@click.pass_context
def show_batch(ctx, batch_id):
    """显示批次详情"""
    svc = _get_service(ctx.obj.get("db_path"))
    batch = svc.get_batch(batch_id)
    if not batch:
        click.echo(f"{ERR} 批次不存在: {batch_id}", err=True)
        sys.exit(1)

    click.echo(f"=== 批次 #{batch['id']} ===")
    click.echo(f"  名称:       {batch['name']}")
    click.echo(f"  状态:       {batch['status']}")
    click.echo(f"  锁定:       {'是' if batch['locked'] else '否'}")
    click.echo(f"  配置版本:   {batch['config_version']}")
    if batch.get("current_scheme_id"):
        click.echo(f"  当前方案:   #{batch['current_scheme_id']} {batch.get('current_scheme_name') or ''}")
    else:
        click.echo(f"  当前方案:   (无)")
    click.echo(f"  源文件:     {batch['source_file']}")
    click.echo(f"  创建时间:   {batch['created_at']}")
    click.echo(f"  更新时间:   {batch['updated_at']}")
    if batch.get("error_message"):
        click.echo(f"  错误信息:   {batch['error_message']}")

    cfg = json.loads(batch["config_json"])
    click.echo(f"\n  当前配置 (v{cfg.get('version', 1)}):")
    click.echo(json.dumps(cfg, indent=4, ensure_ascii=False))


# ========== 处理与重跑 ==========

@cli.command("process")
@click.argument("batch_id", type=int)
@click.pass_context
def process_batch(ctx, batch_id):
    """处理批次 / 重跑。未锁定批次创建新运行记录；锁定批次拒绝执行。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        run_id, run_number = svc.process_batch(batch_id)
        click.echo(f"{OK} 批次 {batch_id} 处理完成")
        click.echo(f"  运行 ID: {run_id}")
        click.echo(f"  运行次数: #{run_number}")
    except BatchLockedError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)
    except BatchServiceError as e:
        click.echo(f"{ERR} 错误: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"{ERR} 处理失败: {e}", err=True)
        sys.exit(1)


# ========== 锁定管理 ==========

@cli.command("lock")
@click.argument("batch_id", type=int)
@click.pass_context
def lock_batch(ctx, batch_id):
    """锁定批次（防止被覆盖）"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        svc.lock_batch(batch_id)
        click.echo(f"{OK} 批次 {batch_id} 已锁定")
    except BatchServiceError as e:
        click.echo(f"{ERR} 错误: {e}", err=True)
        sys.exit(1)


@cli.command("unlock")
@click.argument("batch_id", type=int)
@click.pass_context
def unlock_batch(ctx, batch_id):
    """解锁批次"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        svc.unlock_batch(batch_id)
        click.echo(f"{OK} 批次 {batch_id} 已解锁")
    except BatchServiceError as e:
        click.echo(f"{ERR} 错误: {e}", err=True)
        sys.exit(1)


# ========== 配置/阈值修改 ==========

@cli.command("set-threshold")
@click.argument("batch_id", type=int)
@click.option("--zscore", type=float, default=None, help="Z-score 阈值")
@click.option("--iqr", type=float, default=None, help="IQR 倍数")
@click.pass_context
def set_threshold(ctx, batch_id, zscore, iqr):
    """修改异常检测阈值（自动递增配置版本）"""
    if zscore is None and iqr is None:
        click.echo(f"{ERR} 请至少指定 --zscore 或 --iqr 之一", err=True)
        sys.exit(1)
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        new_cfg = svc.set_threshold(batch_id, zscore, iqr)
        click.echo(f"{OK} 阈值已更新，配置版本升至 v{new_cfg['version']}")
        if zscore is not None:
            click.echo(f"  zscore_threshold = {zscore}")
        if iqr is not None:
            click.echo(f"  iqr_multiplier = {iqr}")
    except BatchLockedError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)
    except BatchServiceError as e:
        click.echo(f"{ERR} 错误: {e}", err=True)
        sys.exit(1)


# ========== 运行历史 ==========

@cli.command("history")
@click.argument("batch_id", type=int)
@click.pass_context
def show_history(ctx, batch_id):
    """查看批次运行历史"""
    svc = _get_service(ctx.obj.get("db_path"))
    runs = svc.list_runs(batch_id)
    if not runs:
        click.echo("(暂无运行记录)")
        return
    rows = []
    for r in runs:
        rows.append({
            "RunID": r["id"],
            "#": r["run_number"],
            "状态": r["status"],
            "配置版本": r["config_version"],
            "处理行数": r["rows_processed"],
            "错误行数": r["rows_errors"],
            "开始": r["started_at"][:19],
            "结束": r["finished_at"][:19] if r["finished_at"] else "-",
            "错误": r["error_message"] or ""
        })
    _print_table(rows)


@cli.command("run-show")
@click.argument("run_id", type=int)
@click.option("--metrics", is_flag=True, help="显示指标")
@click.option("--errors", is_flag=True, help="显示行级错误")
@click.option("--anomalies", is_flag=True, help="显示异常点")
@click.pass_context
def show_run(ctx, run_id, metrics, errors, anomalies):
    """查看某次运行详情"""
    svc = _get_service(ctx.obj.get("db_path"))
    run = svc.get_run(run_id)
    if not run:
        click.echo(f"{ERR} 运行不存在: {run_id}", err=True)
        sys.exit(1)

    click.echo(f"=== 运行 #{run['id']} (批次 #{run['batch_id']}, 第 {run['run_number']} 次) ===")
    click.echo(f"  状态:       {run['status']}")
    click.echo(f"  配置版本:   {run['config_version']}")
    click.echo(f"  处理行数:   {run['rows_processed']}")
    click.echo(f"  错误行数:   {run['rows_errors']}")
    click.echo(f"  开始:       {run['started_at']}")
    click.echo(f"  结束:       {run['finished_at'] or '-'}")
    if run.get("error_message"):
        click.echo(f"  错误信息:   {run['error_message']}")

    show_all = not (metrics or errors or anomalies)

    if show_all or metrics:
        click.echo("\n--- 指标 ---")
        m_list = svc.get_run_metrics(run_id)
        if m_list:
            rows = [{"传感器": m["sensor_name"], "指标": m["metric_name"], "值": round(m["metric_value"], 6)} for m in m_list]
            _print_table(rows)
        else:
            click.echo("(无)")

    if show_all or errors:
        click.echo("\n--- 行级错误 ---")
        e_list = svc.get_run_errors(run_id)
        if e_list:
            rows = [{"行号": e["row_number"], "类型": e["error_type"], "详情": e["error_detail"]} for e in e_list]
            _print_table(rows)
        else:
            click.echo("(无)")

    if show_all or anomalies:
        click.echo("\n--- 异常点 ---")
        a_list = svc.get_run_anomalies(run_id)
        if a_list:
            rows = [
                {"传感器": a["sensor_name"], "行号": a["row_number"],
                 "时间": a["timestamp"][:19] if a["timestamp"] else "-",
                 "值": round(a["value"], 4), "类型": a["anomaly_type"]}
                for a in a_list
            ]
            _print_table(rows)
        else:
            click.echo("(无)")


# ========== 导出 ==========

@cli.command("export")
@click.argument("batch_id", type=int)
@click.option("--output", "-o", required=True, type=click.Path(), help="输出目录")
@click.option("--run-id", type=int, default=None, help="指定运行 ID（默认最新）")
@click.option("--metrics", is_flag=True, help="导出指标")
@click.option("--errors", is_flag=True, help="导出行级错误")
@click.option("--anomalies", is_flag=True, help="导出异常点")
@click.pass_context
def export_data(ctx, batch_id, output, run_id, metrics, errors, anomalies):
    """导出数据（指标 / 错误 / 异常点）"""
    svc = _get_service(ctx.obj.get("db_path"))
    export_all = not (metrics or errors or anomalies)
    os.makedirs(output, exist_ok=True)

    try:
        if export_all or metrics:
            path = svc.export_metrics(batch_id, os.path.join(output, f"batch_{batch_id}_metrics.csv"), run_id)
            click.echo(f"{OK} 指标已导出: {path}")
        if export_all or errors:
            path = svc.export_errors(batch_id, os.path.join(output, f"batch_{batch_id}_errors.csv"), run_id)
            click.echo(f"{OK} 错误已导出: {path}")
        if export_all or anomalies:
            path = svc.export_anomalies(batch_id, os.path.join(output, f"batch_{batch_id}_anomalies.csv"), run_id)
            click.echo(f"{OK} 异常点已导出: {path}")
    except BatchServiceError as e:
        click.echo(f"{ERR} 错误: {e}", err=True)
        sys.exit(1)


@cli.command("exports")
@click.argument("batch_id", type=int)
@click.pass_context
def list_exports(ctx, batch_id):
    """查看批次导出历史"""
    svc = _get_service(ctx.obj.get("db_path"))
    exports = svc.list_exports(batch_id)
    if not exports:
        click.echo("(暂无导出记录)")
        return
    rows = []
    for e in exports:
        rows.append({
            "ID": e["id"],
            "RunID": e["run_id"],
            "类型": e["export_type"],
            "路径": e["export_path"],
            "时间": e["exported_at"][:19]
        })
    _print_table(rows)


# ========== 分析方案管理 ==========

@cli.group()
def scheme():
    """分析方案管理（保存/加载/导入/导出/列出方案）"""
    pass


@scheme.command("save")
@click.argument("name")
@click.option("--batch-id", type=int, default=None, help="从该批次提取当前配置作为方案")
@click.option("--config", "config_path", type=click.Path(exists=True), default=None, help="从 JSON 文件读取配置")
@click.option("--description", default=None, help="方案描述")
@click.pass_context
def scheme_save(ctx, name, batch_id, config_path, description):
    """保存分析方案。从 --batch-id 批次配置 或 --config 文件中读取"""
    from .config import load_config
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        if config_path:
            cfg = load_config(config_path)
            sid = svc.save_scheme(name, cfg, description)
        elif batch_id is not None:
            sid = svc.save_scheme(name, description=description, batch_id=batch_id)
        else:
            click.echo(f"{ERR} 必须指定 --batch-id 或 --config 之一", err=True)
            sys.exit(1)
        click.echo(f"{OK} 方案已保存: ID={sid}, 名称='{name}'")
    except SchemeConflictError as e:
        click.echo(f"{ERR} 冲突({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        sys.exit(1)
    except SchemeError as e:
        click.echo(f"{ERR} 方案错误: {e}", err=True)
        sys.exit(1)


@scheme.command("list")
@click.pass_context
def scheme_list(ctx):
    """列出所有分析方案"""
    svc = _get_service(ctx.obj.get("db_path"))
    schemes = svc.list_schemes()
    if not schemes:
        click.echo("(暂无方案)")
        return
    rows = []
    for s in schemes:
        rows.append({
            "ID": s["id"],
            "名称": s["name"],
            "版本": s["scheme_version"],
            "来源": f"#{s['source_scheme_id']}" if s.get("source_scheme_id") else "原始",
            "描述": s.get("description") or "",
            "创建": s["created_at"][:19],
            "更新": s["updated_at"][:19]
        })
    _print_table(rows)


@scheme.command("show")
@click.argument("scheme_id", type=int)
@click.pass_context
def scheme_show(ctx, scheme_id):
    """显示方案详情"""
    svc = _get_service(ctx.obj.get("db_path"))
    s = svc.get_scheme(scheme_id)
    if not s:
        click.echo(f"{ERR} 方案不存在: {scheme_id}", err=True)
        sys.exit(1)
    click.echo(f"=== 方案 #{s['id']} ===")
    click.echo(f"  名称:       {s['name']}")
    click.echo(f"  描述:       {s.get('description') or '(无)'}")
    click.echo(f"  版本:       {s['scheme_version']}")
    source_sid = s.get("source_scheme_id")
    if source_sid:
        click.echo(f"  派生来源:   方案 #{source_sid}")
    else:
        click.echo(f"  派生来源:   (原始方案)")
    click.echo(f"  创建时间:   {s['created_at']}")
    click.echo(f"  更新时间:   {s['updated_at']}")
    click.echo("\n  配置内容:")
    click.echo(json.dumps(s["config"], indent=4, ensure_ascii=False))


@scheme.command("apply")
@click.argument("scheme_id", type=int)
@click.argument("batch_id", type=int)
@click.option("--dry-run", is_flag=True, help="仅执行预检，不实际修改")
@click.pass_context
def scheme_apply(ctx, scheme_id, batch_id, dry_run):
    """将方案配置应用到未锁定批次（不自动重跑）。

    使用 --dry-run 可仅执行预检，不实际修改。完整的预检→执行流水推荐使用 switch 命令。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        sw = svc.switch_scheme(
            SwitchSchemeResult.SWITCH_TYPE_APPLY,
            batch_id,
            scheme_id=scheme_id,
            dry_run_only=dry_run
        )
        _print_switch_result(sw, "apply", dry_run)
    except BatchLockedError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)
    except SchemeError as e:
        click.echo(f"{ERR} 方案错误: {e}", err=True)
        sys.exit(1)
    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


@scheme.command("export")
@click.argument("scheme_id", type=int)
@click.option("--output", "-o", required=True, type=click.Path(), help="输出 JSON 文件路径")
@click.pass_context
def scheme_export(ctx, scheme_id, output):
    """将方案导出为 JSON 文件"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        path = svc.export_scheme_to_file(scheme_id, output)
        click.echo(f"{OK} 方案已导出: {path}")
    except SchemeError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@scheme.command("import")
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--on-conflict", type=click.Choice(["overwrite", "rename", "skip", "ask"]), default="ask",
              help="冲突处理策略: overwrite=覆盖, rename=自动重命名, skip=跳过, ask=报错等待用户选择")
@click.option("--new-name", default=None, help="重命名时使用的新名称（仅 --on-conflict rename 时）")
@click.pass_context
def scheme_import(ctx, file_path, on_conflict, new_name):
    """从 JSON 文件导入方案"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        strategy = None if on_conflict == "ask" else on_conflict
        result = svc.import_scheme_from_file(file_path, strategy, new_name)
        if result.success:
            action_label = {
                None: "导入",
                "overwrite": "覆盖导入",
                "rename": "重命名导入"
            }.get(result.action, "导入")
            click.echo(f"{OK} {action_label}成功: ID={result.scheme_id}, {result.message}")
            if result.original_name and result.original_name != result.final_name:
                click.echo(f"  原始名称:   {result.original_name}")
                click.echo(f"  落地名称:   {result.final_name}")
            if result.original_id:
                click.echo(f"  原始ID:     {result.original_id}")
            if result.imported_from:
                click.echo(f"  导入来源:   {result.imported_from}")
        else:
            click.echo(f"  已跳过: {result.message}")
            if result.original_name:
                click.echo(f"  原始名称:   {result.original_name}")
    except SchemeConflictError as e:
        click.echo(f"{ERR} 导入冲突 ({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        click.echo("  提示: 使用 --on-conflict overwrite/rename/skip 自动处理", err=True)
        sys.exit(1)
    except SchemeError as e:
        click.echo(f"{ERR} 方案错误: {e}", err=True)
        sys.exit(1)


@scheme.command("import-apply")
@click.argument("file_path", type=click.Path(exists=True))
@click.argument("batch_id", type=int)
@click.option("--on-conflict", type=click.Choice(["overwrite", "rename", "skip"]), default="rename",
              help="冲突处理策略: overwrite=覆盖, rename=自动重命名(默认), skip=跳过")
@click.option("--new-name", default=None, help="重命名时使用的新名称")
@click.pass_context
def scheme_import_apply(ctx, file_path, batch_id, on_conflict, new_name):
    """从 JSON 文件导入方案并立即应用到批次（一步完成导入+应用链路）。

    完整流程：导出 JSON → 外部修改 → import-apply 导入并应用 → 必要时 rollback。
    导入冲突按 --on-conflict 策略处理（默认 rename）。
    成功后终端输出：方案ID、名称、原始名称(若重命名)、原始ID、导入来源、批次ID、配置版本变化。
    导入和应用均记录审计日志，rollback 可撤销。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.import_and_apply_scheme(file_path, batch_id, on_conflict, new_name)
        ir = result["import_result"]
        if not ir.success:
            click.echo(f"{ERR} 导入失败: {ir.message}", err=True)
            sys.exit(1)

        new_cfg = result["apply_config"]
        click.echo(f"{OK} 方案导入并应用成功")
        click.echo(f"  方案ID:     {ir.scheme_id}")
        click.echo(f"  方案名称:   {ir.final_name}")
        if ir.original_name and ir.original_name != ir.final_name:
            click.echo(f"  原始名称:   {ir.original_name}")
        if ir.original_id:
            click.echo(f"  原始ID:     {ir.original_id}")
        if ir.imported_from:
            click.echo(f"  导入来源:   {ir.imported_from}")
        click.echo(f"  应用批次:   #{batch_id}")
        if new_cfg:
            click.echo(f"  配置版本:   v{new_cfg['version']}")
        click.echo("  请执行 process 命令以使用新配置重跑该批次。")
    except BatchLockedError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)
    except SchemeConflictError as e:
        click.echo(f"{ERR} 导入冲突 ({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        sys.exit(1)
    except SchemeError as e:
        click.echo(f"{ERR} 方案错误: {e}", err=True)
        sys.exit(1)
    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


@scheme.command("last-change")
@click.argument("batch_id", type=int)
@click.pass_context
def scheme_last_change(ctx, batch_id):
    """查看批次最近一次方案变更结果。

    输出包含：批次状态、当前方案、版本变化、操作类型、触发方式、
    失败原因、配置差异、回滚信息、方案来源追溯（original_id/imported_from）。
    数据持久化在审计日志中，重启后仍可查询。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        info = svc.get_latest_scheme_change(batch_id)

        click.echo(f"=== 批次 #{info['batch_id']} 方案变更结果 ===")
        click.echo(f"  批次名称:   {info['batch_name']}")
        click.echo(f"  批次状态:   {info['batch_status']}" +
                   (" (已锁定)" if info['batch_locked'] else ""))
        if info.get("current_scheme_id"):
            click.echo(f"  当前方案:   #{info['current_scheme_id']} {info['current_scheme_name'] or ''}")
        else:
            click.echo(f"  当前方案:   (无)")
        click.echo(f"  当前配置:   v{info['current_config_version']}")

        lc = info.get("latest_change")
        if not lc:
            click.echo(f"\n  {info.get('message', '该批次尚无方案变更记录')}")
            return

        click.echo(f"\n--- 最近一次变更 ---")
        action_labels = {
            "apply": "应用", "clone_apply": "克隆应用",
            "derive_apply": "派生应用", "import": "导入",
            "import_apply": "导入应用", "rollback": "回滚",
            "direct_modify": "直接修改"
        }
        result_labels = {"success": "成功", "failed": "失败", "blocked": "阻止"}
        trigger_labels = {"cli": "CLI", "api": "API", "import": "导入"}

        click.echo(f"  操作:       {action_labels.get(lc['action'], lc['action'])}")
        click.echo(f"  触发方式:   {trigger_labels.get(lc['trigger_method'], lc['trigger_method'])}")
        click.echo(f"  结果:       {result_labels.get(lc['result'], lc['result'])}")
        if lc.get("scheme_id"):
            click.echo(f"  方案:       #{lc['scheme_id']}" +
                       (f" '{lc['scheme_name']}'" if lc.get("scheme_name") else ""))
        if lc.get("source_scheme_id"):
            click.echo(f"  来源方案:   #{lc['source_scheme_id']}")
        if lc.get("version_change"):
            vc = lc["version_change"]
            click.echo(f"  版本变化:   v{vc['old']} -> v{vc['new']}")
        if lc.get("error_message"):
            click.echo(f"  失败原因:   {lc['error_message']}")
        click.echo(f"  发生时间:   {lc['created_at'][:19]}")

        sd = info.get("scheme_detail")
        if sd:
            click.echo(f"\n--- 方案详情 ---")
            click.echo(f"  方案ID:     {sd['scheme_id']}")
            click.echo(f"  方案名称:   {sd['scheme_name']}")
            click.echo(f"  方案版本:   {sd['scheme_version']}")
            if sd.get("source_scheme_id"):
                click.echo(f"  派生来源:   #{sd['source_scheme_id']}")
            if sd.get("original_id"):
                click.echo(f"  原始ID:     {sd['original_id']}")
            if sd.get("imported_from"):
                click.echo(f"  导入来源:   {sd['imported_from']}")

        rb = info.get("rollback_info")
        if rb:
            click.echo(f"\n--- 回滚信息 ---")
            click.echo(f"  回滚自历史: #{rb['rolled_back_from_history_id']}")
            if rb.get("rolled_back_to_scheme_id"):
                click.echo(f"  回滚到方案: #{rb['rolled_back_to_scheme_id']}" +
                           (f" '{rb['rolled_back_to_scheme_name']}'" if rb.get("rolled_back_to_scheme_name") else ""))

        cd = lc.get("config_diff") if lc else None
        if cd:
            has_changes = cd.get("added") or cd.get("modified") or cd.get("removed")
            if has_changes:
                click.echo(f"\n--- 配置差异 ---")
                if cd.get("added"):
                    click.echo(f"  新增 ({len(cd['added'])}):")
                    for k, v in cd["added"].items():
                        click.echo(f"    + {k} = {v}")
                if cd.get("modified"):
                    click.echo(f"  修改 ({len(cd['modified'])}):")
                    for k, v in cd["modified"].items():
                        click.echo(f"    ~ {k}: {v['old']} -> {v['new']}")
                if cd.get("removed"):
                    click.echo(f"  删除 ({len(cd['removed'])}):")
                    for k, v in cd["removed"].items():
                        click.echo(f"    - {k} = {v}")

    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


@scheme.command("delete")
@click.argument("scheme_id", type=int)
@click.pass_context
def scheme_delete(ctx, scheme_id):
    """删除方案"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        svc.delete_scheme(scheme_id)
        click.echo(f"{OK} 方案 {scheme_id} 已删除")
    except SchemeError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@scheme.command("clone")
@click.argument("source_scheme_id", type=int)
@click.argument("new_name")
@click.option("--description", default=None, help="新方案描述（不指定则沿用源方案）")
@click.pass_context
def scheme_clone(ctx, source_scheme_id, new_name, description):
    """基于已有方案克隆出新方案，可改名称和描述。

    同名时直接报错（name_exists），不会自动覆盖或重命名。
    克隆成功后终端输出源方案 ID/名称 和 新方案 ID/名称。
    日志位置：Logger=pipeline.service，级别=INFO。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        cloned_id = svc.clone_scheme(source_scheme_id, new_name, description)
        click.echo(f"{OK} 方案克隆成功")
        click.echo(f"  源方案:   ID={source_scheme_id}")
        click.echo(f"  新方案:   ID={cloned_id}, 名称='{new_name}'")
    except SchemeConflictError as e:
        click.echo(f"{ERR} 冲突({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        sys.exit(1)
    except SchemeError as e:
        click.echo(f"{ERR} 方案错误: {e}", err=True)
        sys.exit(1)


@scheme.command("clone-apply")
@click.argument("source_scheme_id", type=int)
@click.argument("new_name")
@click.argument("batch_id", type=int)
@click.option("--description", default=None, help="新方案描述（不指定则沿用源方案）")
@click.pass_context
def scheme_clone_apply(ctx, source_scheme_id, new_name, batch_id, description):
    """克隆方案并立即应用到未锁定批次（不自动重跑）。

    原子性：先校验批次未锁定 + 新名称不冲突，再创建方案并应用。
    锁定批次直接拒绝（不会创建新方案），与 scheme apply 规则一致。
    同名时直接报错（name_exists），不会自动覆盖或重命名。
    成功后终端输出：源方案 ID/名称、新方案 ID/名称、批次 ID、新配置版本。
    日志位置：Logger=pipeline.service，级别=INFO（克隆一条 + 应用一条）。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.clone_and_apply_scheme(source_scheme_id, new_name, batch_id, description)
        click.echo(f"{OK} 方案克隆并应用成功")
        click.echo(f"  源方案:   ID={result.source_scheme_id}, 名称='{result.source_scheme_name}'")
        click.echo(f"  新方案:   ID={result.cloned_scheme_id}, 名称='{result.cloned_scheme_name}'")
        click.echo(f"  应用批次: ID={result.applied_batch_id}, 配置版本升至 v{result.new_config_version}")
        click.echo("  请执行 process 命令以使用新配置重跑该批次。")
    except SchemeConflictError as e:
        click.echo(f"{ERR} 冲突({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        sys.exit(1)
    except BatchLockedError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)
    except SchemeError as e:
        click.echo(f"{ERR} 方案错误: {e}", err=True)
        sys.exit(1)
    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


@scheme.command("derive")
@click.argument("source_scheme_id", type=int)
@click.argument("new_name")
@click.option("--description", default=None, help="新方案描述（不指定则沿用源方案）")
@click.pass_context
def scheme_derive(ctx, source_scheme_id, new_name, description):
    """基于已有方案派生出新方案，记录来源关系，可改名称和描述。

    派生方案会记录 source_scheme_id，可通过 scheme show 追溯来源。
    同名时直接报错（name_exists），不会自动覆盖或重命名。
    成功后终端输出源方案 ID/名称 和 派生方案 ID/名称。
    日志位置：Logger=pipeline.service，级别=INFO，每步校验均输出步骤级结果。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        derived_id = svc.derive_scheme(source_scheme_id, new_name, description)
        click.echo(f"{OK} 方案派生成功")
        click.echo(f"  源方案:     ID={source_scheme_id}")
        click.echo(f"  派生方案:   ID={derived_id}, 名称='{new_name}'")
        click.echo(f"  来源追溯:   可通过 scheme show {derived_id} 查看派生来源")
    except SchemeConflictError as e:
        click.echo(f"{ERR} 冲突({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        sys.exit(1)
    except SchemeError as e:
        click.echo(f"{ERR} 方案错误: {e}", err=True)
        sys.exit(1)


@scheme.command("derive-apply")
@click.argument("source_scheme_id", type=int)
@click.argument("new_name")
@click.argument("batch_id", type=int)
@click.option("--description", default=None, help="新方案描述（不指定则沿用源方案）")
@click.pass_context
def scheme_derive_apply(ctx, source_scheme_id, new_name, batch_id, description):
    """派生方案并立即应用到未锁定批次（不自动重跑）。

    原子性：先按顺序校验（源方案→批次→锁定→名称冲突→配置完整），再创建并应用。
    锁定批次直接拒绝（不会创建新方案），与 scheme apply 规则一致。
    同名时直接报错（name_exists），不会自动覆盖或重命名。
    派生方案会记录 source_scheme_id，可通过 scheme show 追溯来源。
    成功后终端输出：源方案 ID/名称、派生方案 ID/名称、批次 ID、新配置版本。
    日志位置：Logger=pipeline.service，级别=INFO，7步校验/操作均输出步骤级结果。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.derive_and_apply_scheme(source_scheme_id, new_name, batch_id, description)
        click.echo(f"{OK} 方案派生并应用成功")
        click.echo(f"  源方案:     ID={result.source_scheme_id}, 名称='{result.source_scheme_name}'")
        click.echo(f"  派生方案:   ID={result.derived_scheme_id}, 名称='{result.derived_scheme_name}'")
        click.echo(f"  应用批次:   ID={result.applied_batch_id}, 配置版本升至 v{result.new_config_version}")
        click.echo(f"  来源追溯:   可通过 scheme show {result.derived_scheme_id} 查看派生来源")
        click.echo("  请执行 process 命令以使用新配置重跑该批次。")
    except SchemeConflictError as e:
        click.echo(f"{ERR} 冲突({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        sys.exit(1)
    except BatchLockedError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)
    except SchemeError as e:
        click.echo(f"{ERR} 方案错误: {e}", err=True)
        sys.exit(1)
    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


@scheme.command("history")
@click.argument("batch_id", type=int)
@click.pass_context
def scheme_history(ctx, batch_id):
    """查看批次的方案应用/回滚历史记录"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        history = svc.get_scheme_history(batch_id)
        if not history:
            click.echo("(暂无方案历史记录)")
            return
        rows = []
        for h in history:
            action_label = {
                "apply": "应用",
                "rollback": "回滚",
                "direct": "直接修改"
            }.get(h["action"], h["action"])
            scheme_info = ""
            if h.get("scheme_id"):
                scheme_info = f"#{h['scheme_id']} {h.get('scheme_name') or ''}"
            else:
                scheme_info = "(无方案)"
            source_info = ""
            if h.get("source_scheme_id"):
                source_info = f"来源#{h['source_scheme_id']}"
            rows.append({
                "ID": h["id"],
                "操作": action_label,
                "方案": scheme_info.strip(),
                "来源": source_info,
                "配置版本": f"v{h['config_version']}",
                "回滚自": f"#{h['rolled_back_from_id']}" if h.get("rolled_back_from_id") else "-",
                "时间": h["applied_at"][:19]
            })
        _print_table(rows)
    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


@scheme.command("rollback")
@click.argument("batch_id", type=int)
@click.option("--dry-run", is_flag=True, help="仅执行回滚预检，不实际执行")
@click.pass_context
def scheme_rollback(ctx, batch_id, dry_run):
    """回滚批次到上一个配置版本（撤销最近一次方案应用或修改）。

    锁定批次拒绝回滚，回滚后配置版本号递增（回滚本身也是一次变更）。
    回滚操作会记录到历史和审计日志中，可追溯。
    --dry-run 仅预览，不实际修改。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        sw = svc.switch_scheme(
            SwitchSchemeResult.SWITCH_TYPE_ROLLBACK,
            batch_id,
            dry_run_only=dry_run
        )
        _print_switch_result(sw, "rollback", dry_run)
    except BatchLockedError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)
    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


@scheme.command("dry-run")
@click.argument("scheme_id", type=int)
@click.argument("batch_id", type=int)
@click.option("--new-name", default=None, help="新方案名称（用于 clone-apply/derive-apply 场景的预检）")
@click.option("--source-scheme-id", type=int, default=None, help="源方案 ID（用于 clone-apply/derive-apply 场景的预检）")
@click.pass_context
def scheme_dry_run(ctx, scheme_id, batch_id, new_name, source_scheme_id):
    """预检查方案应用风险，不实际修改任何数据。

    检查项：批次存在、方案存在、批次未锁定、新名称不冲突、源方案存在、版本兼容、配置完整。
    输出：当前生效方案 vs 待切换方案的对比、版本差异、配置变更预览、风险详情。
    结果会记录到审计日志中，可通过 audit-history 查询。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.dry_run_apply_scheme(
            scheme_id=scheme_id,
            batch_id=batch_id,
            new_scheme_name=new_name,
            source_scheme_id=source_scheme_id
        )
        _print_dry_run_enhanced(result)
    except (SchemeError, BatchServiceError) as e:
        click.echo(f"{ERR} 错误: {e}", err=True)
        sys.exit(1)


@scheme.command("rollback-dry-run")
@click.argument("batch_id", type=int)
@click.pass_context
def scheme_rollback_dry_run(ctx, batch_id):
    """回滚预检：预览回滚结果，不实际执行。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.dry_run_rollback_scheme(batch_id)
        _print_dry_run_enhanced(result)
    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


@scheme.command("switch")
@click.argument("switch_type", type=click.Choice(["apply", "clone", "derive", "rollback"]))
@click.argument("batch_id", type=int)
@click.option("--scheme-id", type=int, default=None, help="待应用方案 ID（apply 模式必填）")
@click.option("--source-scheme-id", type=int, default=None, help="源方案 ID（clone/derive 模式必填）")
@click.option("--new-name", default=None, help="新方案名称（clone/derive 模式必填）")
@click.option("--new-description", default=None, help="新方案描述（clone/derive 模式可选）")
@click.option("--dry-run", is_flag=True, help="仅执行预检，不实际修改")
@click.pass_context
def scheme_switch(ctx, switch_type, batch_id, scheme_id, source_scheme_id,
                  new_name, new_description, dry_run):
    """方案切换统一入口：预检→确认→执行的完整流水。

    SWITCH_TYPE:
      apply    直接应用已有方案（需 --scheme-id）
      clone    克隆源方案后应用（需 --source-scheme-id、--new-name）
      derive   派生源方案后应用（需 --source-scheme-id、--new-name）
      rollback 回滚到上一配置版本

    统一输出：当前方案、待切方案、配置变更预览、风险详情、执行结果。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        type_map = {
            "apply": SwitchSchemeResult.SWITCH_TYPE_APPLY,
            "clone": SwitchSchemeResult.SWITCH_TYPE_CLONE,
            "derive": SwitchSchemeResult.SWITCH_TYPE_DERIVE,
            "rollback": SwitchSchemeResult.SWITCH_TYPE_ROLLBACK
        }
        sw = svc.switch_scheme(
            switch_type=type_map[switch_type],
            batch_id=batch_id,
            scheme_id=scheme_id,
            new_scheme_name=new_name,
            new_description=new_description,
            source_scheme_id=source_scheme_id,
            dry_run_only=dry_run
        )
        _print_switch_result(sw, switch_type, dry_run)
    except (BatchLockedError, SchemeError, BatchServiceError) as e:
        click.echo(f"{ERR} 错误: {e}", err=True)
        sys.exit(1)


@scheme.command("audit-history")
@click.argument("batch_id", type=int, required=False)
@click.option("--scheme-id", type=int, default=None, help="按方案 ID 筛选")
@click.option("--action", type=click.Choice(["apply", "clone_apply", "derive_apply", "rollback", "direct_modify", "dry_run"]), default=None, help="按操作类型筛选")
@click.option("--result", type=click.Choice(["success", "failed", "blocked"]), default=None, help="按结果筛选")
@click.option("--limit", type=int, default=50, help="最多显示条数（默认 50）")
@click.option("--diff", is_flag=True, help="显示配置差异详情")
@click.pass_context
def scheme_audit_history(ctx, batch_id, scheme_id, action, result, limit, diff):
    """查看方案应用审计历史。

    可按批次、方案、操作类型、执行结果筛选。
    每条记录包含：操作类型、触发方式、前后配置、差异、结果、失败原因。
    数据持久化在数据库中，重启后仍可查询。
    导入导出后的方案继续应用时，历史记录保持连续。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        logs = svc.get_scheme_audit_logs(
            batch_id=batch_id,
            scheme_id=scheme_id,
            action=action,
            result=result,
            limit=limit
        )

        if not logs:
            click.echo("(暂无审计记录)")
            return

        action_labels = {
            "apply": "应用",
            "clone_apply": "克隆应用",
            "derive_apply": "派生应用",
            "rollback": "回滚",
            "direct_modify": "直接修改",
            "dry_run": "预检"
        }
        result_labels = {
            "success": "成功",
            "failed": "失败",
            "blocked": "阻止"
        }
        trigger_labels = {
            "cli": "CLI",
            "api": "API",
            "import": "导入"
        }

        rows = []
        for log in logs:
            scheme_info = ""
            if log.get("scheme_id"):
                scheme_info = f"#{log['scheme_id']}"
                if log.get("scheme_name"):
                    scheme_info += f" {log['scheme_name']}"
            source_info = ""
            if log.get("source_scheme_id"):
                source_info = f"#{log['source_scheme_id']}"

            result_label = result_labels.get(log["result"], log["result"])
            result_color = "green" if log["result"] == "success" else "red"

            rows.append({
                "ID": log["id"],
                "时间": log["created_at"][:19],
                "操作": action_labels.get(log["action"], log["action"]),
                "触发": trigger_labels.get(log["trigger_method"], log["trigger_method"]),
                "批次": f"#{log['batch_id']}",
                "方案": scheme_info,
                "来源": source_info,
                "结果": result_label,
                "错误": log.get("error_message") or ""
            })

        _print_table(rows)

        if diff:
            click.echo("\n=== 配置差异详情 ===")
            for log in logs:
                if log.get("config_diff"):
                    cd = log["config_diff"]
                    click.echo(f"\n--- 记录 #{log['id']} ({action_labels.get(log['action'], log['action'])}) ---")
                    if cd.get("version_change"):
                        click.echo(f"  版本: v{cd['version_change']['old']} → v{cd['version_change']['new']}")
                    if cd.get("added"):
                        for k, v in cd["added"].items():
                            click.echo(f"  + {k} = {v}")
                    if cd.get("modified"):
                        for k, v in cd["modified"].items():
                            click.echo(f"  ~ {k}: {v['old']} → {v['new']}")
                    if cd.get("removed"):
                        for k, v in cd["removed"].items():
                            click.echo(f"  - {k} = {v}")

    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


# ========== 对比分析 ==========

@cli.group()
def compare():
    """多批次对比分析"""
    pass


@compare.command("run")
@click.argument("name")
@click.argument("batch_ids", nargs=-1, type=int, required=True)
@click.option("--scheme-id", type=int, default=None, help="关联分析方案 ID")
@click.pass_context
def compare_run(ctx, name, batch_ids, scheme_id):
    """生成多批次对比报告。至少需要 2 个批次 ID。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        report = svc.generate_comparison_report(name, list(batch_ids), scheme_id)
        click.echo(f"{OK} 对比报告已生成: ID={report['report_id']}, 名称='{name}'")
        scheme = report["scheme"]
        if scheme and scheme.get("name"):
            click.echo(f"  使用方案: {scheme['name']} (v{scheme['version']}, id={scheme['id']})")
        else:
            click.echo(f"  使用方案: (无)")
        click.echo(f"  参与批次: {len(report['batch_summaries'])} 个")
        md = report["metrics_diff"]["summary"]
        click.echo(f"  指标比较: {md['total_metrics_compared']} 项, 有差异 {md['metrics_with_diff']} 项")
        ad = report["anomalies_diff"]["total_anomalies_range"]
        click.echo(f"  异常数量范围: [{ad['min']}, {ad['max']}], 差值={ad['abs_diff']}")
    except (SchemeError, BatchServiceError) as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@compare.command("list")
@click.pass_context
def compare_list(ctx):
    """列出所有对比报告"""
    svc = _get_service(ctx.obj.get("db_path"))
    reports = svc.list_comparison_reports()
    if not reports:
        click.echo("(暂无对比报告)")
        return
    rows = []
    for r in reports:
        rows.append({
            "ID": r["id"],
            "名称": r["name"],
            "方案": r.get("scheme_name") or "(无)",
            "方案版本": r.get("scheme_version") or "-",
            "批次": f"{len(r['batch_ids'])} 个",
            "批次ID": ",".join(str(x) for x in r["batch_ids"]),
            "创建": r["created_at"][:19]
        })
    _print_table(rows)


@compare.command("show")
@click.argument("report_id", type=int)
@click.option("--metrics", is_flag=True, help="显示指标差异")
@click.option("--anomalies", is_flag=True, help="显示异常数量")
@click.option("--batches", is_flag=True, help="显示批次摘要")
@click.pass_context
def compare_show(ctx, report_id, metrics, anomalies, batches):
    """显示对比报告详情"""
    svc = _get_service(ctx.obj.get("db_path"))
    r = svc.get_comparison_report(report_id)
    if not r:
        click.echo(f"{ERR} 报告不存在: {report_id}", err=True)
        sys.exit(1)
    rpt = r["report"]
    click.echo(f"=== 对比报告 #{r['id']} ===")
    click.echo(f"  名称:       {rpt['name']}")
    click.echo(f"  方案:       {rpt['scheme']['name'] or '(无)'} (v{rpt['scheme']['version'] or 'N/A'})")
    click.echo(f"  生成时间:   {rpt['generated_at']}")

    show_all = not (metrics or anomalies or batches)

    if show_all or batches:
        click.echo("\n--- 参与批次 ---")
        rows = []
        for bs in rpt["batch_summaries"]:
            rows.append({
                "ID": bs["batch_id"],
                "名称": bs["batch_name"],
                "状态": bs["status"],
                "锁定": "是" if bs["locked"] else "否",
                "源文件": bs["source_file"],
                "cfg_v": bs["config_version"],
                "run#": bs["run_number"],
                "异常": bs["anomalies_count"],
                "指标": bs["metrics_count"]
            })
        _print_table(rows)

    if show_all or metrics:
        click.echo("\n--- 指标差异（前10项有差异）---")
        md = rpt["metrics_diff"]
        rows = []
        count = 0
        for key, info in sorted(md["per_metric"].items(), key=lambda x: -(x[1]["abs_diff"] or 0)):
            if info["abs_diff"] and info["abs_diff"] > 0:
                rows.append({
                    "传感器": info["sensor"],
                    "指标": info["metric"],
                    "绝对差": round(info["abs_diff"], 6),
                    "相对差%": round(info["rel_diff_pct"], 2) if info["rel_diff_pct"] is not None else "-"
                })
                count += 1
                if count >= 10:
                    break
        _print_table(rows)

    if show_all or anomalies:
        click.echo("\n--- 异常数量对比 ---")
        ad = rpt["anomalies_diff"]
        rows = []
        for bk, info in ad["per_batch"].items():
            bid, bname = bk.split(":", 1)
            rows.append({
                "批次ID": bid,
                "名称": bname,
                "锁定": "是" if info["locked"] else "否",
                "异常总数": info["total"],
                "按传感器": str(info["per_sensor"])
            })
        _print_table(rows)
        click.echo(f"\n  数量范围: [{ad['total_anomalies_range']['min']}, {ad['total_anomalies_range']['max']}], "
                   f"差值={ad['total_anomalies_range']['abs_diff']}")


@compare.command("export")
@click.argument("report_id", type=int)
@click.option("--output", "-o", required=True, type=click.Path(), help="输出目录或 JSON 文件路径")
@click.option("--format", "fmt", type=click.Choice(["json", "csv"]), default="json", help="导出格式: json 或 csv")
@click.pass_context
def compare_export(ctx, report_id, output, fmt):
    """导出对比报告。json 输出单个文件，csv 输出多个文件到目录"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        if fmt == "json":
            path = svc.export_comparison_report_json(report_id, output)
            click.echo(f"{OK} JSON 报告已导出: {path}")
        else:
            paths = svc.export_comparison_report_csv(report_id, output)
            click.echo(f"{OK} CSV 报告已导出到目录: {os.path.abspath(output)}")
            for k, p in paths.items():
                click.echo(f"  - {k}: {p}")
    except BatchServiceError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@compare.command("delete")
@click.argument("report_id", type=int)
@click.pass_context
def compare_delete(ctx, report_id):
    """删除对比报告"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        svc.delete_comparison_report(report_id)
        click.echo(f"{OK} 报告 {report_id} 已删除")
    except BatchServiceError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


# ========== 运行包快照 ==========

@cli.group()
def snapshot():
    """运行包快照管理（导出/导入/重放可复现实验包）"""
    pass


@snapshot.command("export")
@click.argument("name")
@click.argument("batch_id", type=int)
@click.option("--run-id", type=int, default=None, help="指定运行 ID（默认最新）")
@click.option("--output", "-o", type=click.Path(), default=None, help="输出 ZIP 文件路径")
@click.option("--type", "snapshot_type", type=click.Choice(["batch", "run"]), default="run",
              help="快照类型: batch 或 run（默认 run）")
@click.pass_context
def snapshot_export(ctx, name, batch_id, run_id, output, snapshot_type):
    """导出运行包快照为可复现实验包（ZIP）。

    快照包含: 源 CSV 摘要、配置版本、指标结果、异常记录、
    依赖版本、SHA256 校验摘要。可在另一数据库或重启后导入查看。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.export_snapshot(name, batch_id, run_id, output, snapshot_type)
        click.echo(f"{OK} 快照导出成功")
        click.echo(f"  快照ID:     {result['snapshot_id']}")
        click.echo(f"  名称:       '{result['name']}'")
        click.echo(f"  类型:       {snapshot_type}")
        click.echo(f"  批次ID:     {batch_id}")
        if run_id:
            click.echo(f"  运行ID:     {run_id}")
        click.echo(f"  输出文件:   {result['file_path']}")
        click.echo(f"  文件大小:   {result['file_size']} bytes")
        click.echo(f"  SHA256:     {result['checksum_sha256'][:32]}...")
        mf = result['manifest']
        click.echo(f"  配置版本:   v{mf['config']['version']}")
        click.echo(f"  指标数量:   {mf['metrics_summary']['total_metrics']}")
        click.echo(f"  异常数量:   {mf['anomalies_summary']['total_anomalies']}")
        click.echo(f"  错误数量:   {mf['errors_summary']['total_errors']}")
    except snap.SnapshotConflictError as e:
        click.echo(f"{ERR} 快照冲突 ({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        sys.exit(1)
    except snap.SnapshotError as e:
        click.echo(f"{ERR} 快照错误: {e}", err=True)
        sys.exit(1)
    except BatchServiceError as e:
        click.echo(f"{ERR} 批次错误: {e}", err=True)
        sys.exit(1)


@snapshot.command("import")
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--on-conflict", type=click.Choice(["reject", "rename", "skip"]), default=None,
              help="冲突处理策略: reject=拒绝, rename=自动重命名, skip=跳过")
@click.option("--new-name", default=None, help="重命名时使用的新名称（仅 --on-conflict rename 时）")
@click.pass_context
def snapshot_import(ctx, file_path, on_conflict, new_name):
    """从 ZIP 文件导入快照，支持冲突处理策略。

    冲突场景: 同名快照、源文件缺失、配置版本不兼容。
    所有导入决定写入审计日志，可通过 audit-history 查询。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.import_snapshot(file_path, on_conflict, new_name)
        if result.success:
            action_label = {
                None: "导入",
                "rename": "重命名导入"
            }.get(result.action, "导入")
            click.echo(f"{OK} {action_label}成功")
            click.echo(f"  快照ID:     {result.snapshot_id}")
            if result.original_name and result.original_name != result.final_name:
                click.echo(f"  原始名称:   {result.original_name}")
                click.echo(f"  落地名称:   {result.final_name}")
            else:
                click.echo(f"  名称:       '{result.final_name}'")
            if result.original_batch_id:
                click.echo(f"  原始批次ID: {result.original_batch_id}")
            click.echo(f"  导入来源:   {result.imported_from}")
            click.echo(f"  提示: 可使用 snapshot show {result.snapshot_id} 查看详情")
        else:
            action_label = {
                "reject": "已拒绝导入",
                "skip": "已跳过"
            }.get(result.action, "操作")
            click.echo(f"  {action_label}: {result.message}")
            if result.original_name:
                click.echo(f"  原始名称:   {result.original_name}")
    except snap.SnapshotConflictError as e:
        click.echo(f"{ERR} 导入冲突 ({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        click.echo("  提示: 使用 --on-conflict reject/rename/skip 自动处理", err=True)
        sys.exit(1)
    except snap.SnapshotError as e:
        click.echo(f"{ERR} 快照错误: {e}", err=True)
        sys.exit(1)


@snapshot.command("list")
@click.option("--batch-id", type=int, default=None, help="按批次 ID 筛选")
@click.option("--status", type=click.Choice(["available", "deleted", "corrupted"]), default=None,
              help="按状态筛选")
@click.pass_context
def snapshot_list(ctx, batch_id, status):
    """列出快照，支持按批次和状态筛选。"""
    svc = _get_service(ctx.obj.get("db_path"))
    snapshots = svc.list_snapshots(batch_id, status)
    if not snapshots:
        click.echo("(暂无快照)")
        return
    rows = []
    for s in snapshots:
        status_label = {
            "available": "可用",
            "deleted": "已删除",
            "corrupted": "已损坏"
        }.get(s["status"], s["status"])
        type_label = "批次" if s["snapshot_type"] == "batch" else "运行"
        rows.append({
            "ID": s["id"],
            "名称": s["name"],
            "类型": type_label,
            "状态": status_label,
            "配置版本": f"v{s['config_version']}",
            "源批次": f"#{s['source_batch_id']}" if s.get("source_batch_id") else "-",
            "源运行": f"#{s['source_run_id']}" if s.get("source_run_id") else "-",
            "文件大小": s["file_size"],
            "创建时间": s["created_at"][:19]
        })
    _print_table(rows)


@snapshot.command("show")
@click.argument("snapshot_id", type=int)
@click.option("--manifest", is_flag=True, help="显示完整 manifest")
@click.option("--metrics", is_flag=True, help="显示指标列表")
@click.option("--source", is_flag=True, help="显示源数据摘要")
@click.pass_context
def snapshot_show(ctx, snapshot_id, manifest, metrics, source):
    """显示快照详情。"""
    svc = _get_service(ctx.obj.get("db_path"))
    s = svc.get_snapshot(snapshot_id)
    if not s:
        click.echo(f"{ERR} 快照不存在: {snapshot_id}", err=True)
        sys.exit(1)

    mf = s["manifest"]
    status_label = {
        "available": "可用",
        "deleted": "已删除",
        "corrupted": "已损坏"
    }.get(s["status"], s["status"])
    type_label = "批次" if s["snapshot_type"] == "batch" else "运行"

    click.echo(f"=== 快照 #{s['id']} ===")
    click.echo(f"  名称:       {s['name']}")
    click.echo(f"  类型:       {type_label}")
    click.echo(f"  状态:       {status_label}")
    click.echo(f"  配置版本:   v{s['config_version']}")
    click.echo(f"  文件路径:   {s['file_path']}")
    click.echo(f"  文件大小:   {s['file_size']} bytes")
    click.echo(f"  SHA256:     {s['checksum_sha256'][:32]}...")
    click.echo(f"  创建时间:   {s['created_at']}")

    if s.get("imported_from"):
        click.echo(f"  导入来源:   {s['imported_from']}")
    if s.get("original_batch_id"):
        click.echo(f"  原始批次ID: {s['original_batch_id']}")
    if s.get("original_run_id"):
        click.echo(f"  原始运行ID: {s['original_run_id']}")

    src = mf.get("source", {})
    click.echo(f"\n--- 来源信息 ---")
    click.echo(f"  原始批次:   #{src.get('original_batch_id')} '{src.get('original_batch_name')}'")
    click.echo(f"  原始运行:   #{src.get('original_run_id')} (第 {src.get('original_run_number')} 次)")
    click.echo(f"  源文件:     {src.get('original_source_file')}")
    click.echo(f"  原始创建:   {src.get('original_created_at')}")
    click.echo(f"  原始处理:   {src.get('original_processed_at')}")

    ms = mf.get("metrics_summary", {})
    click.echo(f"\n--- 处理摘要 ---")
    click.echo(f"  处理行数:   {ms.get('rows_processed', 0)}")
    click.echo(f"  错误行数:   {ms.get('rows_errors', 0)}")
    click.echo(f"  指标数量:   {ms.get('total_metrics', 0)}")
    click.echo(f"  异常数量:   {mf.get('anomalies_summary', {}).get('total_anomalies', 0)}")

    atc = mf.get("anomalies_summary", {}).get("anomaly_type_counts", {})
    if atc:
        click.echo(f"  异常类型:   {', '.join([f'{k}={v}' for k, v in atc.items()])}")

    dep = mf.get("dependencies", {})
    click.echo(f"\n--- 依赖版本 ---")
    for pkg, ver in dep.items():
        click.echo(f"  {pkg:15s}: {ver}")

    show_all = not (manifest or metrics or source)

    if show_all or manifest:
        click.echo(f"\n--- 校验和 ---")
        for k, v in mf.get("checksums", {}).items():
            click.echo(f"  {k:15s}: {v[:32]}...")

    if show_all or metrics:
        click.echo(f"\n--- 指标（前10项）---")
        m_list = ms.get("metrics", [])
        if m_list:
            rows = [{"传感器": m["sensor_name"], "指标": m["metric_name"],
                     "值": round(m["metric_value"], 6)} for m in m_list[:10]]
            _print_table(rows)
            if len(m_list) > 10:
                click.echo(f"  ... 共 {len(m_list)} 项指标")
        else:
            click.echo("(无)")

    if show_all or source:
        ss = mf.get("source_summary", {})
        click.echo(f"\n--- 源数据摘要 ---")
        click.echo(f"  文件名:     {ss.get('file_name')}")
        click.echo(f"  总行数:     {ss.get('total_rows', 0)}")
        click.echo(f"  列名:       {', '.join(ss.get('columns', []))}")
        if ss.get("time_range", {}).get("start"):
            click.echo(f"  时间范围:   {ss['time_range']['start']} 至 {ss['time_range']['end']}")
        cs = ss.get("column_statistics", {})
        if cs:
            click.echo(f"\n  列统计（前5列）:")
            for col, stats in list(cs.items())[:5]:
                click.echo(f"    {col}: mean={stats['mean']:.4f}, std={stats['std']:.4f}, "
                           f"min={stats['min']:.4f}, max={stats['max']:.4f}")


@snapshot.command("replay")
@click.argument("snapshot_id", type=int)
@click.option("--new-batch-name", default=None, help="新批次名称（默认自动生成）")
@click.option("--csv-path", type=click.Path(exists=True), default=None,
              help="新的 CSV 源文件路径（默认使用快照中的源文件）")
@click.option("--tolerance", type=float, default=1.0, help="指标差异容忍百分比（默认 1%）")
@click.pass_context
def snapshot_replay(ctx, snapshot_id, new_batch_name, csv_path, tolerance):
    """用快照中的配置和样本重新跑一遍，对比原指标并标出差异。

    输出: 对比指标差异（绝对差、相对差%）、失败原因、是否可接受。
    差异超出容忍度的指标会明确标出。所有操作写入审计日志。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.replay_snapshot(snapshot_id, new_batch_name, csv_path, tolerance)
        if result.success:
            click.echo(f"{OK} 快照重放完成")
            click.echo(f"  快照ID:     {result.snapshot_id}")
            click.echo(f"  新批次ID:   {result.new_batch_id}")
            click.echo(f"  新运行ID:   {result.new_run_id}")
            click.echo(f"  容忍度:     {tolerance}%")
            click.echo(f"  可接受:     {'是' if result.acceptable else '否'}")

            mc = result.metrics_comparison
            click.echo(f"\n--- 指标对比 ---")
            click.echo(f"  总对比数:   {mc.get('total_metrics_compared', 0)}")
            click.echo(f"  有差异:     {mc.get('metrics_with_diff', 0)}")
            click.echo(f"  超容忍:     {mc.get('metrics_out_of_tolerance', 0)}")

            if result.differences:
                click.echo(f"\n--- 差异详情（前20项）---")
                rows = []
                for d in result.differences[:20]:
                    status = "OK" if d.get("within_tolerance", True) else "FAIL"
                    abs_diff = d.get("abs_diff")
                    rel_diff = d.get("rel_diff_pct")
                    rows.append({
                        "状态": status,
                        "传感器": d.get("sensor"),
                        "指标": d.get("metric"),
                        "原值": round(d.get("original"), 6) if d.get("original") is not None else "-",
                        "新值": round(d.get("new"), 6) if d.get("new") is not None else "-",
                        "绝对差": round(abs_diff, 6) if abs_diff is not None else "-",
                        "相对差%": round(rel_diff, 2) if rel_diff is not None and rel_diff != float("inf") else "inf"
                    })
                _print_table(rows)
                if len(result.differences) > 20:
                    click.echo(f"  ... 共 {len(result.differences)} 项差异")

            if result.failures:
                click.echo(f"\n--- 超出容忍度的指标 ---")
                for i, f in enumerate(result.failures, 1):
                    click.echo(f"  {i}. {f}")

            if result.message:
                click.echo(f"\n  提示: {result.message}")
        else:
            click.echo(f"{ERR} 重放失败: {result.message}", err=True)
            if result.failures:
                for f in result.failures:
                    click.echo(f"  原因: {f}", err=True)
            sys.exit(1)
    except snap.SnapshotConflictError as e:
        click.echo(f"{ERR} 重放冲突 ({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        sys.exit(1)
    except snap.SnapshotNotFoundError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)
    except snap.SnapshotError as e:
        click.echo(f"{ERR} 快照错误: {e}", err=True)
        sys.exit(1)


@snapshot.command("delete")
@click.argument("snapshot_id", type=int)
@click.pass_context
def snapshot_delete(ctx, snapshot_id):
    """删除快照（软删除，保留历史记录）。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        svc.delete_snapshot(snapshot_id)
        click.echo(f"{OK} 快照 {snapshot_id} 已删除（软删除）")
    except snap.SnapshotNotFoundError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)
    except snap.SnapshotError as e:
        click.echo(f"{ERR} 快照错误: {e}", err=True)
        sys.exit(1)


@snapshot.command("audit-history")
@click.argument("snapshot_id", type=int, required=False)
@click.option("--action", type=click.Choice(["snapshot_export", "snapshot_import", "snapshot_replay"]),
              default=None, help="按操作类型筛选")
@click.option("--result", type=click.Choice(["success", "failed", "blocked"]), default=None,
              help="按结果筛选")
@click.option("--limit", type=int, default=50, help="最多显示条数（默认 50）")
@click.pass_context
def snapshot_audit_history(ctx, snapshot_id, action, result, limit):
    """查看快照审计历史（导出/导入/重放/删除）。

    所有操作决定均记录在此，重启后仍可查询。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        from . import database as db
        conn = svc._conn()
        try:
            logs = db.get_snapshot_audit_logs(
                conn,
                snapshot_id=snapshot_id,
                action=action,
                result=result,
                limit=limit
            )
        finally:
            conn.close()

        if not logs:
            click.echo("(暂无审计记录)")
            return

        action_labels = {
            "snapshot_export": "导出",
            "snapshot_import": "导入",
            "snapshot_replay": "重放"
        }
        result_labels = {
            "success": "成功",
            "failed": "失败",
            "blocked": "阻止"
        }

        rows = []
        for log in logs:
            snap_info = f"#{log['snapshot_id']}" if log.get("snapshot_id") else "-"
            batch_info = f"#{log['batch_id']}" if log.get("batch_id") else "-"
            run_info = f"#{log['run_id']}" if log.get("run_id") else "-"

            rows.append({
                "ID": log["id"],
                "时间": log["created_at"][:19],
                "操作": action_labels.get(log["action"], log["action"]),
                "快照": snap_info,
                "批次": batch_info,
                "运行": run_info,
                "结果": result_labels.get(log["result"], log["result"]),
                "错误": log.get("error_message") or ""
            })

        _print_table(rows)
    except Exception as e:
        click.echo(f"{ERR} 查询失败: {e}", err=True)
        sys.exit(1)


# ============ 基线批次库管理 ============

@cli.group()
def baseline():
    """基线批次库管理（注册/复核/导入/导出/历史追溯）。

    把已处理完成的批次登记为可复用基线，后续新批次可直接做漂移复核，
    输出通过/警告/阻断三级结论，明确列出超阈值指标、差异百分比和建议动作。"""
    pass


@baseline.command("register")
@click.argument("name")
@click.argument("batch_id", type=int)
@click.option("--run-id", type=int, default=None, help="指定运行 ID（默认使用批次最新 run）")
@click.option("--description", default=None, help="基线备注说明")
@click.option("--warn-pct", type=float, default=5.0, show_default=True, help="警告阈值（差异百分比）")
@click.option("--block-pct", type=float, default=15.0, show_default=True, help="阻断阈值（差异百分比）")
@click.pass_context
def baseline_register(ctx, name, batch_id, run_id, description, warn_pct, block_pct):
    """从已处理完成的批次注册基线。

    注册时会根据批次指标值自动生成 warn/block 两级阈值，
    后续复核时按此阈值判定通过/警告/阻断。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.register_baseline(
            name, batch_id, run_id=run_id,
            description=description, warn_pct=warn_pct, block_pct=block_pct
        )
        click.echo(f"{OK} 基线已注册")
        click.echo(f"  基线ID:     {result['baseline_id']}")
        click.echo(f"  基线名称:   {result['name']}")
        click.echo(f"  批次ID:     {result['source_batch_id']}")
        click.echo(f"  运行ID:     {result['source_run_id']}")
        click.echo(f"  配置版本:   v{result['config_version']}")
        click.echo(f"  指标数量:   {result['metrics_count']}")
        click.echo(f"  警告阈值:   ±{warn_pct}%")
        click.echo(f"  阻断阈值:   ±{block_pct}%")
    except Exception as e:
        click.echo(f"{ERR} 注册失败: {e}", err=True)
        sys.exit(1)


@baseline.command("check")
@click.argument("baseline_id", type=int)
@click.argument("batch_id", type=int)
@click.option("--run-id", type=int, default=None, help="指定目标运行 ID（默认使用批次最新 run）")
@click.pass_context
def baseline_check(ctx, baseline_id, batch_id, run_id):
    """用基线复核目标批次的指标漂移情况。

    输出三级结论：pass（通过）/ warn（警告）/ block（阻断），
    明确列出超阈值指标、差异百分比和建议动作。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.check_baseline(baseline_id, batch_id, run_id=run_id)

        status_labels = {
            "pass": "通过",
            "warn": "警告",
            "block": "阻断"
        }
        status_symbols = {
            "pass": OK,
            "warn": "! ",
            "block": "X "
        }

        click.echo(f"{status_symbols.get(result.overall_status, '  ')} 基线复核完成")
        click.echo(f"  基线ID:     {result.baseline_id}")
        click.echo(f"  基线名称:   {result.baseline_name}")
        click.echo(f"  目标批次:   #{result.target_batch_id}")
        click.echo(f"  目标运行:   #{result.target_run_id}")
        click.echo(f"  总体结论:   {status_labels.get(result.overall_status, result.overall_status)}")
        click.echo(f"  指标总计:   {result.total_metrics}")
        click.echo(f"  通过:       {result.pass_count}")
        click.echo(f"  警告:       {result.warn_count}")
        click.echo(f"  阻断:       {result.block_count}")
        click.echo()

        if result.metric_results:
            rows = []
            for mr in result.metric_results:
                label = status_labels.get(mr.status, mr.status)
                rows.append({
                    "指标": mr.metric_name,
                    "基线值": f"{mr.baseline_value:.4f}",
                    "实际值": f"{mr.actual_value:.4f}",
                    "绝对差": f"{mr.absolute_diff:+.4f}",
                    "相对差": f"{mr.relative_pct:+.2f}%",
                    "警告阈": f"±{mr.warn_threshold_pct}%",
                    "阻断阈": f"±{mr.block_threshold_pct}%",
                    "状态": label
                })
            _print_table(rows)
            click.echo()

        click.echo(f"建议动作: {result.recommended_action}")
    except Exception as e:
        click.echo(f"{ERR} 复核失败: {e}", err=True)
        sys.exit(1)


@baseline.command("export")
@click.argument("baseline_id", type=int)
@click.option("-o", "--output", "output", default=None, help="输出 ZIP 文件路径")
@click.pass_context
def baseline_export(ctx, baseline_id, output):
    """导出基线为 ZIP 文件。

    导出内容包含基线定义、配置版本、指标阈值、来源摘要和复核历史，
    可在另一份数据库或重启后导入查看。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.export_baseline(baseline_id, output_path=output)
        click.echo(f"{OK} 基线已导出")
        click.echo(f"  基线ID:     {result.baseline_id}")
        click.echo(f"  基线名称:   {result.baseline_name}")
        click.echo(f"  文件路径:   {result.file_path}")
        click.echo(f"  文件大小:   {result.file_size:,} bytes")
        click.echo(f"  SHA256:     {result.checksum_sha256[:16]}...")
    except Exception as e:
        click.echo(f"{ERR} 导出失败: {e}", err=True)
        sys.exit(1)


@baseline.command("import")
@click.argument("file_path")
@click.option("--on-conflict", type=click.Choice(["reject", "rename", "skip"]),
              default=None, help="同名冲突处理策略（默认报错）")
@click.option("--new-name", default=None, help="冲突时重命名为指定名称")
@click.pass_context
def baseline_import(ctx, file_path, on_conflict, new_name):
    """从 ZIP 文件导入基线。

    遇到同名基线时支持 reject（拒绝）/ rename（改名）/ skip（跳过）三种策略，
    所有决定写入审计日志。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.import_baseline(file_path, on_conflict=on_conflict, new_name=new_name)
        if not result.success:
            click.echo(f"{ERR} 导入拒绝")
            click.echo(f"  原名称:     {result.original_name}")
            click.echo(f"  尝试名称:   {result.final_name}")
            click.echo(f"  策略:       {result.conflict_action}")
            click.echo(f"  原因:       {result.error_message}")
            sys.exit(0)
        if result.baseline_id == 0 and result.conflict_action == "skip":
            click.echo(f"=  导入跳过（同名冲突已存在）")
            click.echo(f"  原名称:     {result.original_name}")
            click.echo(f"  策略:       skip")
            return

        click.echo(f"{OK} 基线已导入")
        click.echo(f"  基线ID:     {result.baseline_id}")
        click.echo(f"  原名称:     {result.original_name}")
        click.echo(f"  最终名称:   {result.final_name}")
        if result.conflict_action:
            click.echo(f"  冲突策略:   {result.conflict_action}")
    except Exception as e:
        click.echo(f"{ERR} 导入失败: {e}", err=True)
        sys.exit(1)


@baseline.command("list")
@click.option("--status", type=click.Choice(["active", "deprecated", "deleted"]),
              default=None, help="按状态筛选")
@click.option("--source-batch-id", type=int, default=None, help="按来源批次筛选")
@click.pass_context
def baseline_list(ctx, status, source_batch_id):
    """列出所有基线，支持按状态和来源批次筛选。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        baselines = svc.list_baselines(status=status, source_batch_id=source_batch_id)
        if not baselines:
            click.echo("(暂无基线)")
            return

        status_labels = {
            "active": "启用",
            "deprecated": "弃用",
            "deleted": "已删"
        }
        check_labels = {
            "pass": "通过",
            "warn": "警告",
            "block": "阻断",
            None: "-"
        }

        rows = []
        for b in baselines:
            last_check = check_labels.get(b.get("last_check_status"), b.get("last_check_status") or "-")
            rows.append({
                "ID": b["id"],
                "名称": b["name"],
                "状态": status_labels.get(b["status"], b["status"]),
                "版本": f"v{b['config_version']}",
                "来源批次": b.get("source_batch_name") or f"#{b.get('source_batch_id') or '-'}",
                "上次复核": last_check,
                "创建时间": b["created_at"][:19]
            })
        _print_table(rows)
    except Exception as e:
        click.echo(f"{ERR} 查询失败: {e}", err=True)
        sys.exit(1)


@baseline.command("show")
@click.argument("baseline_id", type=int)
@click.option("--config", is_flag=True, help="显示完整配置")
@click.option("--thresholds", is_flag=True, help="显示指标阈值")
@click.option("--checks", is_flag=True, help="显示最近复核历史")
@click.pass_context
def baseline_show(ctx, baseline_id, config, thresholds, checks):
    """显示基线详情，可选择查看配置、阈值和复核历史。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        b = svc.get_baseline(baseline_id)
        if not b:
            click.echo(f"{ERR} 基线 #{baseline_id} 不存在", err=True)
            sys.exit(1)

        status_labels = {
            "active": "启用",
            "deprecated": "弃用",
            "deleted": "已删"
        }
        check_labels = {
            "pass": "通过",
            "warn": "警告",
            "block": "阻断"
        }

        click.echo(f"基线 #{b['id']}: {b['name']}")
        click.echo(f"  状态:       {status_labels.get(b['status'], b['status'])}")
        if b.get("description"):
            click.echo(f"  备注:       {b['description']}")
        click.echo(f"  配置版本:   v{b['config_version']}")
        click.echo(f"  来源批次:   {b.get('source_batch_name') or '-'} (#{b.get('source_batch_id') or '-'})")
        click.echo(f"  来源运行:   #{b.get('source_run_number') or '-'} (run_id=#{b.get('source_run_id') or '-'})")
        if b.get("last_check_status"):
            click.echo(f"  上次复核:   {check_labels.get(b['last_check_status'], b['last_check_status'])} @ {b.get('last_checked_at', '-')[:19]}")
        if b.get("imported_from"):
            click.echo(f"  导入来源:   {b['imported_from']}")
            if b.get("original_baseline_id"):
                click.echo(f"  原始基线ID: #{b['original_baseline_id']}")
        click.echo(f"  创建时间:   {b['created_at'][:19]}")
        click.echo(f"  更新时间:   {b['updated_at'][:19]}")
        click.echo(f"  指标阈值:   {len(b.get('metric_thresholds', {}).get('metrics', {}))} 个指标")

        if config:
            click.echo()
            click.echo("=== 配置 ===")
            click.echo(json.dumps(b["config"], indent=2, ensure_ascii=False))

        if thresholds:
            click.echo()
            click.echo("=== 指标阈值 ===")
            mt = b.get("metric_thresholds", {})
            click.echo(f"默认警告阈值: ±{mt.get('default_warn_pct', 5)}%")
            click.echo(f"默认阻断阈值: ±{mt.get('default_block_pct', 15)}%")
            metrics = mt.get("metrics", {})
            if metrics:
                rows = []
                for name, t in metrics.items():
                    rows.append({
                        "指标": name,
                        "基线值": f"{t.get('baseline_value', 0):.4f}",
                        "警告阈": f"±{t.get('warn_threshold_pct', 5)}%",
                        "阻断阈": f"±{t.get('block_threshold_pct', 15)}%"
                    })
                _print_table(rows)

        if checks:
            click.echo()
            click.echo("=== 复核历史（最近10条）===")
            history = svc.get_baseline_checks(baseline_id=baseline_id, limit=10)
            if not history:
                click.echo("(暂无复核记录)")
            else:
                rows = []
                for h in history:
                    rows.append({
                        "ID": h["id"],
                        "目标批次": h.get("target_batch_name") or f"#{h.get('target_batch_id') or '-'}",
                        "结论": check_labels.get(h["check_status"], h["check_status"]),
                        "通过": h["pass_count"],
                        "警告": h["warn_count"],
                        "阻断": h["block_count"],
                        "时间": h["checked_at"][:19]
                    })
                _print_table(rows)
    except Exception as e:
        click.echo(f"{ERR} 查询失败: {e}", err=True)
        sys.exit(1)


@baseline.command("history")
@click.argument("baseline_id", type=int, required=False)
@click.option("--action", type=click.Choice([
    "baseline_register", "baseline_check", "baseline_export",
    "baseline_import", "baseline_delete"
]), default=None, help="按操作类型筛选")
@click.option("--result", type=click.Choice(["success", "failed", "blocked"]),
              default=None, help="按结果筛选")
@click.option("--limit", type=int, default=50, show_default=True, help="最多显示条数")
@click.pass_context
def baseline_history(ctx, baseline_id, action, result, limit):
    """查看基线审计历史（注册/复核/导入/导出/删除）。

    所有操作决定均记录在此，重启后仍可查询。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        logs = svc.get_baseline_audit_logs(
            baseline_id=baseline_id,
            action=action,
            result=result,
            limit=limit
        )

        if not logs:
            click.echo("(暂无审计记录)")
            return

        action_labels = {
            "baseline_register": "注册",
            "baseline_check": "复核",
            "baseline_export": "导出",
            "baseline_import": "导入",
            "baseline_delete": "删除"
        }
        result_labels = {
            "success": "成功",
            "failed": "失败",
            "blocked": "阻止"
        }

        rows = []
        for log in logs:
            bl_info = f"#{log['baseline_id']}" if log.get("baseline_id") else "-"
            batch_info = f"#{log['batch_id']}" if log.get("batch_id") else "-"

            rows.append({
                "ID": log["id"],
                "时间": log["created_at"][:19],
                "操作": action_labels.get(log["action"], log["action"]),
                "基线": bl_info,
                "批次": batch_info,
                "结果": result_labels.get(log["result"], log["result"]),
                "错误": log.get("error_message") or ""
            })
        _print_table(rows)
    except Exception as e:
        click.echo(f"{ERR} 查询失败: {e}", err=True)
        sys.exit(1)


@baseline.command("delete")
@click.argument("baseline_id", type=int)
@click.pass_context
def baseline_delete(ctx, baseline_id):
    """删除基线（软删除，保留历史记录和审计日志）。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.delete_baseline(baseline_id)
        if not result:
            click.echo(f"{ERR} 基线 #{baseline_id} 不存在", err=True)
            sys.exit(1)
        click.echo(f"{OK} 基线已删除（软删除，审计记录保留）")
        click.echo(f"  基线ID:     {baseline_id}")
    except Exception as e:
        click.echo(f"{ERR} 删除失败: {e}", err=True)
        sys.exit(1)


# ========== 复核工单 ==========

@cli.group()
def ticket():
    """复核工单管理（创建/列表/查看/分配/关闭/重开/导入/导出）。

    把复核失败、告警过多或被基线拦住的批次直接转成可追踪任务，
    保存来源批次和运行、触发规则、责任人、处理结论、备注时间线和当前状态。"""
    pass


@ticket.command("create")
@click.argument("title")
@click.option("--batch-id", type=int, default=None, help="来源批次 ID")
@click.option("--run-id", type=int, default=None, help="来源运行 ID")
@click.option("--trigger-rule", default=None, help="触发规则说明（如 baseline_block / alert_overflow）")
@click.option("--assignee", default=None, help="责任人")
@click.pass_context
def ticket_create(ctx, title, batch_id, run_id, trigger_rule, assignee):
    """创建复核工单。将复核失败、告警过多或被基线拦住的批次转成可追踪任务。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        tid = svc.create_ticket(title, source_batch_id=batch_id,
                                source_run_id=run_id, trigger_rule=trigger_rule,
                                assignee=assignee)
        click.echo(f"{OK} 工单已创建")
        click.echo(f"  工单ID:     {tid}")
        click.echo(f"  标题:       '{title}'")
        if batch_id:
            click.echo(f"  来源批次:   #{batch_id}")
        if run_id:
            click.echo(f"  来源运行:   #{run_id}")
        if trigger_rule:
            click.echo(f"  触发规则:   {trigger_rule}")
        if assignee:
            click.echo(f"  责任人:     {assignee}")
    except TicketConflictError as e:
        click.echo(f"{ERR} 冲突({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        sys.exit(1)
    except TicketError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@ticket.command("list")
@click.option("--status", type=click.Choice(["open", "assigned", "resolved", "reopened", "closed"]),
              default=None, help="按状态筛选")
@click.option("--assignee", default=None, help="按责任人筛选")
@click.option("--batch-id", type=int, default=None, help="按来源批次筛选")
@click.pass_context
def ticket_list(ctx, status, assignee, batch_id):
    """列出复核工单，支持按状态、责任人、来源批次筛选。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        tickets = svc.list_tickets(status=status, assignee=assignee,
                                   source_batch_id=batch_id)
        if not tickets:
            click.echo("(暂无工单)")
            return
        status_labels = {
            "open": "待处理", "assigned": "已分配",
            "resolved": "已关闭", "reopened": "已重开", "closed": "已结束"
        }
        rows = []
        for t in tickets:
            rows.append({
                "ID": t["id"],
                "标题": t["title"],
                "状态": status_labels.get(t["status"], t["status"]),
                "责任人": t.get("assignee") or "-",
                "来源批次": f"#{t['source_batch_id']}" if t.get("source_batch_id") else "-",
                "触发规则": t.get("trigger_rule") or "-",
                "处理结论": t.get("resolution") or "-",
                "更新时间": t["updated_at"][:19]
            })
        _print_table(rows)
    except TicketError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@ticket.command("show")
@click.argument("ticket_id", type=int)
@click.pass_context
def ticket_show(ctx, ticket_id):
    """显示工单详情，包含来源信息、状态、处理结论和完整备注时间线。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        t = svc.get_ticket(ticket_id)
        if not t:
            click.echo(f"{ERR} 工单不存在: {ticket_id}", err=True)
            sys.exit(1)

        status_labels = {
            "open": "待处理", "assigned": "已分配",
            "resolved": "已关闭", "reopened": "已重开", "closed": "已结束"
        }
        note_type_labels = {
            "comment": "备注", "assign": "分配",
            "resolve": "关闭", "reopen": "重开",
            "status_change": "状态变更"
        }

        click.echo(f"=== 工单 #{t['id']} ===")
        click.echo(f"  标题:       {t['title']}")
        click.echo(f"  状态:       {status_labels.get(t['status'], t['status'])}")
        click.echo(f"  责任人:     {t.get('assignee') or '(未分配)'}")
        click.echo(f"  来源批次:   #{t['source_batch_id']}" if t.get("source_batch_id") else "  来源批次:   (无)")
        click.echo(f"  来源运行:   #{t['source_run_id']}" if t.get("source_run_id") else "  来源运行:   (无)")
        click.echo(f"  触发规则:   {t.get('trigger_rule') or '(无)'}")
        click.echo(f"  处理结论:   {t.get('resolution') or '(无)'}")
        if t.get("original_ticket_id"):
            click.echo(f"  原始工单ID: #{t['original_ticket_id']}")
        if t.get("imported_from"):
            click.echo(f"  导入来源:   {t['imported_from']}")
        click.echo(f"  创建时间:   {t['created_at'][:19]}")
        click.echo(f"  更新时间:   {t['updated_at'][:19]}")

        notes = svc.get_ticket_notes(ticket_id)
        if notes:
            click.echo(f"\n--- 备注时间线 ({len(notes)} 条) ---")
            for n in notes:
                ntype = note_type_labels.get(n["note_type"], n["note_type"])
                author = n.get("author") or "(系统)"
                click.echo(f"  [{n['created_at'][:19]}] [{ntype}] {author}: {n['content']}")
        else:
            click.echo("\n--- 备注时间线: (无) ---")
    except TicketError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@ticket.command("assign")
@click.argument("ticket_id", type=int)
@click.argument("assignee")
@click.pass_context
def ticket_assign(ctx, ticket_id, assignee):
    """分配工单给指定责任人。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        svc.assign_ticket(ticket_id, assignee)
        click.echo(f"{OK} 工单已分配")
        click.echo(f"  工单ID:     {ticket_id}")
        click.echo(f"  责任人:     {assignee}")
    except TicketError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@ticket.command("resolve")
@click.argument("ticket_id", type=int)
@click.argument("resolution")
@click.option("--assignee", default=None, help="关闭人（默认沿用当前责任人）")
@click.pass_context
def ticket_resolve(ctx, ticket_id, resolution, assignee):
    """关闭工单，记录处理结论。只有待处理/已分配/已重开状态的工单可关闭。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        svc.resolve_ticket(ticket_id, resolution, assignee=assignee)
        click.echo(f"{OK} 工单已关闭")
        click.echo(f"  工单ID:     {ticket_id}")
        click.echo(f"  处理结论:   {resolution}")
    except TicketError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@ticket.command("reopen")
@click.argument("ticket_id", type=int)
@click.argument("reason")
@click.pass_context
def ticket_reopen(ctx, ticket_id, reason):
    """重新打开已关闭的工单，说明原因。只有已关闭状态的工单可重新打开。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        svc.reopen_ticket(ticket_id, reason)
        click.echo(f"{OK} 工单已重新打开")
        click.echo(f"  工单ID:     {ticket_id}")
        click.echo(f"  原因:       {reason}")
    except TicketError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@ticket.command("export")
@click.argument("ticket_id", type=int)
@click.option("--output", "-o", required=True, type=click.Path(), help="输出 JSON 文件路径")
@click.pass_context
def ticket_export(ctx, ticket_id, output):
    """导出工单为 JSON 文件（含完整处理历史）。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        path = svc.export_ticket(ticket_id, output)
        click.echo(f"{OK} 工单已导出")
        click.echo(f"  工单ID:     {ticket_id}")
        click.echo(f"  文件路径:   {path}")
    except TicketError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


@ticket.command("import")
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--on-conflict", type=click.Choice(["reject", "rename"]),
              default=None, help="冲突处理策略: reject=拒绝, rename=自动重命名")
@click.option("--new-title", default=None, help="重命名时使用的新标题")
@click.pass_context
def ticket_import(ctx, file_path, on_conflict, new_title):
    """从 JSON 文件导入工单（含处理历史）。遇到同标题冲突时支持 reject 和 rename。"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        result = svc.import_ticket(file_path, on_conflict=on_conflict,
                                   new_title=new_title)
        if result.success:
            action_label = {
                None: "导入", "rename": "重命名导入"
            }.get(result.action, "导入")
            click.echo(f"{OK} {action_label}成功")
            click.echo(f"  工单ID:     {result.ticket_id}")
            if result.original_title and result.original_title != result.final_title:
                click.echo(f"  原始标题:   {result.original_title}")
                click.echo(f"  落地标题:   {result.final_title}")
            else:
                click.echo(f"  标题:       '{result.final_title}'")
            if result.original_ticket_id:
                click.echo(f"  原始工单ID: #{result.original_ticket_id}")
            if result.imported_from:
                click.echo(f"  导入来源:   {result.imported_from}")
        else:
            click.echo(f"  已拒绝: {result.message}")
            if result.original_title:
                click.echo(f"  原始标题:   {result.original_title}")
    except TicketConflictError as e:
        click.echo(f"{ERR} 导入冲突 ({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        click.echo("  提示: 使用 --on-conflict reject/rename 自动处理", err=True)
        sys.exit(1)
    except TicketError as e:
        click.echo(f"{ERR} {e}", err=True)
        sys.exit(1)


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
