"""CLI 命令行界面"""
import os
import sys
import json
import click
from tabulate import tabulate

from .service import (
    PipelineService, BatchServiceError, BatchLockedError,
    SchemeError, SchemeConflictError, SchemeImportResult, SchemeCloneResult
)
from .config import get_default_config, load_config

OK = "[OK]"
ERR = "[ERROR]"


def _get_service(db_path: str = None) -> PipelineService:
    return PipelineService(db_path)


def _print_table(rows, headers=None):
    if not rows:
        click.echo("(无数据)")
        return
    click.echo(tabulate(rows, headers=headers or "keys", tablefmt="simple"))


@click.group()
@click.option("--db", "db_path", default=None, help="数据库文件路径")
@click.pass_context
def cli(ctx, db_path):
    """实验数据处理流水线 CLI"""
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
        rows.append({
            "ID": b["id"],
            "名称": b["name"],
            "状态": b["status"],
            "锁定": "是" if b["locked"] else "否",
            "配置版本": b["config_version"],
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
    click.echo(f"  创建时间:   {s['created_at']}")
    click.echo(f"  更新时间:   {s['updated_at']}")
    click.echo("\n  配置内容:")
    click.echo(json.dumps(s["config"], indent=4, ensure_ascii=False))


@scheme.command("apply")
@click.argument("scheme_id", type=int)
@click.argument("batch_id", type=int)
@click.pass_context
def scheme_apply(ctx, scheme_id, batch_id):
    """将方案配置应用到未锁定批次（不自动重跑）"""
    svc = _get_service(ctx.obj.get("db_path"))
    try:
        new_cfg = svc.apply_scheme_to_batch(scheme_id, batch_id)
        click.echo(f"{OK} 方案已应用到批次 {batch_id}，配置版本升至 v{new_cfg['version']}")
        click.echo("  请执行 process 命令以使用新配置重跑该批次。")
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
        else:
            click.echo(f"  已跳过: {result.message}")
    except SchemeConflictError as e:
        click.echo(f"{ERR} 导入冲突 ({e.conflict_type}): {e}", err=True)
        click.echo(f"  详情: {e.details}", err=True)
        click.echo("  提示: 使用 --on-conflict overwrite/rename/skip 自动处理", err=True)
        sys.exit(1)
    except SchemeError as e:
        click.echo(f"{ERR} 方案错误: {e}", err=True)
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


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
