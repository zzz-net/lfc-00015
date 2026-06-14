"""CLI 命令行界面"""
import os
import sys
import json
import click
from tabulate import tabulate

from .service import PipelineService, BatchServiceError, BatchLockedError
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


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
