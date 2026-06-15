"""
基线批次库模块
将已处理完成的批次登记为可复用基线，支持漂移复核、导入导出和历史追溯。
"""

import os
import sys
import json
import hashlib
import zipfile
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

from . import database as db

logger = logging.getLogger(__name__)


# ============ 异常定义 ============

class BaselineError(Exception):
    """基线操作基础异常"""
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message)
        self.details = details or {}


class BaselineNotFoundError(BaselineError):
    """基线不存在"""
    pass


class BaselineConflictError(BaselineError):
    """基线冲突异常"""
    CONFLICT_NAME = "name_exists"
    CONFLICT_VERSION = "version_incompatible"
    CONFLICT_FORMAT = "format_invalid"
    CONFLICT_SOURCE_MISSING = "source_missing"

    def __init__(self, conflict_type: str, message: str, details: Dict[str, Any] = None):
        super().__init__(message, details)
        self.conflict_type = conflict_type


# ============ 结果定义 ============

@dataclass
class BaselineCheckMetricResult:
    """单个指标的复核结果"""
    metric_name: str
    baseline_value: float
    actual_value: float
    absolute_diff: float
    relative_pct: float
    warn_threshold_pct: float
    block_threshold_pct: float
    status: str  # pass / warn / block


@dataclass
class BaselineCheckResult:
    """基线复核结果"""
    success: bool
    baseline_id: int
    baseline_name: str
    target_batch_id: int
    target_run_id: int
    overall_status: str  # pass / warn / block
    total_metrics: int = 0
    pass_count: int = 0
    warn_count: int = 0
    block_count: int = 0
    metric_results: List[BaselineCheckMetricResult] = field(default_factory=list)
    recommended_action: str = ""
    error_message: str = ""


@dataclass
class BaselineImportResult:
    """基线导入结果"""
    success: bool
    baseline_id: int = 0
    original_name: str = ""
    final_name: str = ""
    conflict_action: str = ""  # reject / rename / skip
    error_message: str = ""


@dataclass
class BaselineExportResult:
    """基线导出结果"""
    success: bool
    baseline_id: int = 0
    baseline_name: str = ""
    file_path: str = ""
    file_size: int = 0
    checksum_sha256: str = ""
    error_message: str = ""


# ============ 冲突策略常量 ============

CONFLICT_REJECT = "reject"
CONFLICT_RENAME = "rename"
CONFLICT_SKIP = "skip"


# ============ 工具函数 ============

def _compute_sha256(file_path: str) -> str:
    """计算文件 SHA256 哈希"""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def _compute_json_sha256(data: Any) -> str:
    """计算 JSON 数据的 SHA256 哈希"""
    json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_str.encode("utf-8")).hexdigest()


def _generate_source_summary(csv_path: str, sample_rows: int = 10) -> Dict[str, Any]:
    """生成源 CSV 摘要信息（从快照模块复用逻辑）"""
    from . import snapshot as snap_module
    if hasattr(snap_module, '_generate_source_summary'):
        return snap_module._generate_source_summary(csv_path, sample_rows)
    if not os.path.exists(csv_path):
        return {"file_missing": True, "path": csv_path}
    return {
        "file_path": csv_path,
        "file_size": os.path.getsize(csv_path),
        "file_sha256": _compute_sha256(csv_path)
    }


def _build_metric_thresholds(metrics: List[Dict[str, Any]],
                             warn_pct: float = 5.0,
                             block_pct: float = 15.0) -> Dict[str, Any]:
    """基于指标值构建默认阈值（warn=±5%, block=±15%）"""
    thresholds = {}
    for m in metrics:
        name = m.get("metric_name") or m.get("name")
        value = m.get("metric_value") or m.get("value")
        if not name or value is None:
            continue
        try:
            val = float(value)
            if val == 0:
                warn_delta = 0.01  # 0 值特殊处理
                block_delta = 0.1
            else:
                warn_delta = abs(val) * (warn_pct / 100.0)
                block_delta = abs(val) * (block_pct / 100.0)
            thresholds[name] = {
                "baseline_value": val,
                "warn_threshold_pct": warn_pct,
                "block_threshold_pct": block_pct,
                "warn_min": val - warn_delta,
                "warn_max": val + warn_delta,
                "block_min": val - block_delta,
                "block_max": val + block_delta
            }
        except (TypeError, ValueError):
            continue
    return {
        "default_warn_pct": warn_pct,
        "default_block_pct": block_pct,
        "metrics": thresholds
    }


# ============ 核心功能 ============

def register_baseline(conn, name: str, batch_id: int, run_id: int = None,
                      description: str = None,
                      custom_thresholds: Dict[str, Any] = None,
                      warn_pct: float = 5.0,
                      block_pct: float = 15.0) -> Dict[str, Any]:
    """
    从已处理完成的批次注册基线。

    Args:
        conn: 数据库连接
        name: 基线名称
        batch_id: 批次 ID
        run_id: 运行 ID（None 时使用最新 run）
        description: 备注说明
        custom_thresholds: 自定义指标阈值（可选）
        warn_pct: 默认警告阈值百分比
        block_pct: 默认阻断阈值百分比

    Returns:
        包含 baseline_id 等信息的 dict
    """
    batch = db.get_batch(conn, batch_id)
    if not batch:
        raise BaselineError(f"批次不存在: {batch_id}")

    if run_id is None:
        run = db.get_latest_run(conn, batch_id)
        if not run:
            raise BaselineError(f"批次 {batch_id} 尚未运行，无法注册基线")
        run_id = run["id"]
    else:
        run = db.get_run(conn, run_id)
        if not run or run["batch_id"] != batch_id:
            raise BaselineError(f"运行 {run_id} 不存在或不属于批次 {batch_id}")

    if run.get("status") != "success":
        raise BaselineError(f"运行 {run_id} 未完成（状态: {run.get('status')}），无法注册基线")

    existing = db.get_baseline_by_name(conn, name)
    if existing:
        raise BaselineConflictError(
            BaselineConflictError.CONFLICT_NAME,
            f"基线名称已存在: '{name}'",
            {"existing_baseline_id": existing["id"]}
        )

    metrics = db.get_metrics(conn, run_id)
    config = json.loads(run["config_json"]) if isinstance(run["config_json"], str) else run["config_json"]
    config_version = config.get("version", 1)

    if custom_thresholds:
        metric_thresholds = custom_thresholds
    else:
        metric_thresholds = _build_metric_thresholds(metrics, warn_pct, block_pct)

    try:
        source_summary = _generate_source_summary(batch["source_file"])
    except Exception as e:
        source_summary = {"error": str(e), "path": batch["source_file"]}

    baseline_id = db.create_baseline(
        conn,
        name=name,
        description=description,
        source_batch_id=batch_id,
        source_run_id=run_id,
        source_batch_name=batch["name"],
        source_run_number=run["run_number"],
        config_version=config_version,
        config=config,
        metric_thresholds=metric_thresholds,
        source_summary=source_summary
    )

    db.add_baseline_audit_log(
        conn,
        action=db.AUDIT_ACTION_BASELINE_REGISTER,
        baseline_id=baseline_id,
        baseline_name=name,
        batch_id=batch_id,
        result=db.AUDIT_RESULT_SUCCESS,
        previous_config=None,
        new_config=config,
        details={"run_id": run_id, "metrics_count": len(metrics)}
    )

    logger.info(
        f"基线已注册: id={baseline_id}, name='{name}', "
        f"batch_id={batch_id}, run_id={run_id}, "
        f"metrics_count={len(metrics)}"
    )

    return {
        "baseline_id": baseline_id,
        "name": name,
        "source_batch_id": batch_id,
        "source_run_id": run_id,
        "config_version": config_version,
        "metrics_count": len(metrics)
    }


def check_baseline(conn, baseline_id: int, batch_id: int, run_id: int = None) -> BaselineCheckResult:
    """
    用基线复核目标批次的指标漂移情况。

    Args:
        conn: 数据库连接
        baseline_id: 基线 ID
        batch_id: 目标批次 ID
        run_id: 目标运行 ID（None 时使用最新 run）

    Returns:
        BaselineCheckResult
    """
    baseline = db.get_baseline(conn, baseline_id)
    if not baseline:
        raise BaselineNotFoundError(f"基线不存在: {baseline_id}")

    batch = db.get_batch(conn, batch_id)
    if not batch:
        raise BaselineError(f"目标批次不存在: {batch_id}")

    if run_id is None:
        run = db.get_latest_run(conn, batch_id)
        if not run:
            raise BaselineError(f"目标批次 {batch_id} 尚未运行")
        run_id = run["id"]
    else:
        run = db.get_run(conn, run_id)
        if not run or run["batch_id"] != batch_id:
            raise BaselineError(f"运行 {run_id} 不存在或不属于批次 {batch_id}")

    if run.get("status") != "success":
        raise BaselineError(f"目标运行 {run_id} 未完成（状态: {run.get('status')}）")

    thresholds_data = baseline["metric_thresholds"]
    thresholds = thresholds_data.get("metrics", {})

    actual_metrics_list = db.get_metrics(conn, run_id)
    actual_metrics = {}
    for m in actual_metrics_list:
        mname = m.get("metric_name") or m.get("name")
        mval = m.get("metric_value") or m.get("value")
        if mname and mval is not None:
            try:
                actual_metrics[mname] = float(mval)
            except (TypeError, ValueError):
                continue

    metric_results: List[BaselineCheckMetricResult] = []
    pass_count = 0
    warn_count = 0
    block_count = 0

    for metric_name, t in thresholds.items():
        baseline_value = t.get("baseline_value", 0)
        actual_value = actual_metrics.get(metric_name)
        if actual_value is None:
            continue

        warn_pct = t.get("warn_threshold_pct", 5.0)
        block_pct = t.get("block_threshold_pct", 15.0)

        abs_diff = actual_value - baseline_value
        if baseline_value == 0:
            rel_pct = 0.0 if abs_diff == 0 else 100.0
        else:
            rel_pct = (abs_diff / abs(baseline_value)) * 100.0

        abs_rel = abs(rel_pct)
        if abs_rel <= warn_pct:
            status = db.BASELINE_CHECK_PASS
            pass_count += 1
        elif abs_rel <= block_pct:
            status = db.BASELINE_CHECK_WARN
            warn_count += 1
        else:
            status = db.BASELINE_CHECK_BLOCK
            block_count += 1

        metric_results.append(BaselineCheckMetricResult(
            metric_name=metric_name,
            baseline_value=baseline_value,
            actual_value=actual_value,
            absolute_diff=abs_diff,
            relative_pct=rel_pct,
            warn_threshold_pct=warn_pct,
            block_threshold_pct=block_pct,
            status=status
        ))

    total = pass_count + warn_count + block_count
    if block_count > 0:
        overall_status = db.BASELINE_CHECK_BLOCK
        recommended_action = "阻断：存在严重漂移指标，建议停止当前流程并排查数据质量或环境差异"
        audit_result = db.AUDIT_RESULT_BLOCKED
    elif warn_count > 0:
        overall_status = db.BASELINE_CHECK_WARN
        recommended_action = "警告：存在轻微漂移指标，建议人工复核后再决定是否继续"
        audit_result = db.AUDIT_RESULT_SUCCESS
    else:
        overall_status = db.BASELINE_CHECK_PASS
        recommended_action = "通过：所有指标在基线容忍范围内"
        audit_result = db.AUDIT_RESULT_SUCCESS

    details_list = []
    for mr in metric_results:
        details_list.append({
            "metric_name": mr.metric_name,
            "baseline_value": mr.baseline_value,
            "actual_value": mr.actual_value,
            "absolute_diff": mr.absolute_diff,
            "relative_pct": mr.relative_pct,
            "warn_threshold_pct": mr.warn_threshold_pct,
            "block_threshold_pct": mr.block_threshold_pct,
            "status": mr.status
        })
    details_data = {"metrics": details_list}

    check_id = db.create_baseline_check(
        conn,
        baseline_id=baseline_id,
        target_batch_id=batch_id,
        target_run_id=run_id,
        target_batch_name=batch["name"],
        check_status=overall_status,
        total_metrics=total,
        pass_count=pass_count,
        warn_count=warn_count,
        block_count=block_count,
        details=details_data,
        recommended_action=recommended_action
    )

    summary = {
        "check_id": check_id,
        "overall_status": overall_status,
        "total": total,
        "pass": pass_count,
        "warn": warn_count,
        "block": block_count,
        "target_batch": batch["name"],
        "target_run_id": run_id
    }
    db.update_baseline_last_check(conn, baseline_id, overall_status, summary)

    db.add_baseline_audit_log(
        conn,
        action=db.AUDIT_ACTION_BASELINE_CHECK,
        baseline_id=baseline_id,
        baseline_name=baseline["name"],
        batch_id=batch_id,
        result=audit_result,
        config_diff={"thresholds": thresholds_data.get("default_warn_pct", 5.0)},
        details=summary,
        error_message=None if block_count == 0 else f"{block_count} 个指标超出阻断阈值"
    )

    logger.info(
        f"基线复核完成: baseline_id={baseline_id}, target_batch={batch_id}, "
        f"status={overall_status}, pass={pass_count}, warn={warn_count}, block={block_count}"
    )

    return BaselineCheckResult(
        success=True,
        baseline_id=baseline_id,
        baseline_name=baseline["name"],
        target_batch_id=batch_id,
        target_run_id=run_id,
        overall_status=overall_status,
        total_metrics=total,
        pass_count=pass_count,
        warn_count=warn_count,
        block_count=block_count,
        metric_results=metric_results,
        recommended_action=recommended_action
    )


def export_baseline(conn, baseline_id: int, output_path: str = None) -> BaselineExportResult:
    """
    导出基线为 JSON/ZIP 文件（包含基线定义、配置、阈值和复核摘要）。

    Args:
        conn: 数据库连接
        baseline_id: 基线 ID
        output_path: 输出 ZIP 文件路径

    Returns:
        BaselineExportResult
    """
    baseline = db.get_baseline(conn, baseline_id)
    if not baseline:
        raise BaselineNotFoundError(f"基线不存在: {baseline_id}")

    if output_path is None:
        output_dir = os.path.join(
            os.path.dirname(os.path.dirname(conn.execute("PRAGMA database_list").fetchone()[2])),
            "baselines"
        )
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(
            output_dir,
            f"baseline_{baseline['name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        )

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    check_history = db.get_baseline_checks(conn, baseline_id=baseline_id, limit=50)

    manifest = {
        "format_version": db.BASELINE_FORMAT_VERSION,
        "baseline_name": baseline["name"],
        "description": baseline.get("description"),
        "source": {
            "original_baseline_id": baseline_id,
            "source_batch_id": baseline.get("source_batch_id"),
            "source_run_id": baseline.get("source_run_id"),
            "source_batch_name": baseline.get("source_batch_name"),
            "source_run_number": baseline.get("source_run_number"),
            "imported_from": baseline.get("imported_from")
        },
        "config_version": baseline.get("config_version"),
        "status": baseline.get("status"),
        "last_check": {
            "status": baseline.get("last_check_status"),
            "summary": baseline.get("last_check_summary"),
            "checked_at": baseline.get("last_checked_at")
        },
        "check_history_count": len(check_history),
        "created_at": baseline.get("created_at"),
        "updated_at": baseline.get("updated_at")
    }

    export_data = {
        "baseline_summary.json": json.dumps(manifest, indent=2, ensure_ascii=False),
        "config.json": json.dumps(baseline["config"], indent=2, ensure_ascii=False),
        "metric_thresholds.json": json.dumps(baseline["metric_thresholds"], indent=2, ensure_ascii=False),
        "source_summary.json": json.dumps(baseline.get("source_summary") or {}, indent=2, ensure_ascii=False),
        "check_history.json": json.dumps(
            {"total_checks": len(check_history), "checks": check_history},
            indent=2, ensure_ascii=False
        ),
    }

    checksum_files = {}
    overall_hash = hashlib.sha256()
    for fname, content in export_data.items():
        content_bytes = content.encode("utf-8")
        file_hash = hashlib.sha256(content_bytes).hexdigest()
        checksum_files[fname] = {
            "sha256": file_hash,
            "file_size": len(content_bytes)
        }
        overall_hash.update(content_bytes)

    checksum_data = {
        "baseline_format_version": db.BASELINE_FORMAT_VERSION,
        "files": checksum_files,
        "overall_sha256": overall_hash.hexdigest()
    }
    export_data["checksum.json"] = json.dumps(checksum_data, indent=2, ensure_ascii=False)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname, content in export_data.items():
            zf.writestr(fname, content)

    file_size = os.path.getsize(output_path)
    file_sha256 = _compute_sha256(output_path)

    db.add_baseline_audit_log(
        conn,
        action=db.AUDIT_ACTION_BASELINE_EXPORT,
        baseline_id=baseline_id,
        baseline_name=baseline["name"],
        result=db.AUDIT_RESULT_SUCCESS,
        details={"output_path": output_path, "file_size": file_size, "checksum": file_sha256}
    )

    logger.info(
        f"基线已导出: id={baseline_id}, name='{baseline['name']}', "
        f"path='{output_path}', size={file_size} bytes"
    )

    return BaselineExportResult(
        success=True,
        baseline_id=baseline_id,
        baseline_name=baseline["name"],
        file_path=os.path.abspath(output_path),
        file_size=file_size,
        checksum_sha256=file_sha256
    )


def _validate_baseline_zip(zip_path: str) -> Tuple[bool, Dict[str, Any], List[str]]:
    """验证基线 ZIP 文件完整性"""
    if not os.path.exists(zip_path):
        return False, {}, [f"文件不存在: {zip_path}"]

    errors = []
    manifest = {}

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            required_files = [
                "baseline_summary.json", "config.json", "metric_thresholds.json",
                "source_summary.json", "check_history.json", "checksum.json"
            ]
            file_list = zf.namelist()

            for req_file in required_files:
                if req_file not in file_list:
                    errors.append(f"缺少必需文件: {req_file}")

            if errors:
                return False, {}, errors

            try:
                manifest = json.loads(zf.read("baseline_summary.json").decode("utf-8"))
            except Exception as e:
                errors.append(f"无法解析 baseline_summary.json: {e}")
                return False, {}, errors

            format_version = manifest.get("format_version", "0.0")
            if format_version != db.BASELINE_FORMAT_VERSION:
                try:
                    major = format_version.split(".")[0]
                    current_major = db.BASELINE_FORMAT_VERSION.split(".")[0]
                    if major != current_major:
                        errors.append(
                            f"基线格式版本不兼容: 导入 {format_version}, 当前 {db.BASELINE_FORMAT_VERSION}"
                        )
                except Exception:
                    errors.append(f"无法解析格式版本: {format_version}")

            try:
                checksum_data = json.loads(zf.read("checksum.json").decode("utf-8"))
                for fname, file_info in checksum_data.get("files", {}).items():
                    if fname in file_list and fname != "checksum.json":
                        content = zf.read(fname)
                        actual_hash = hashlib.sha256(content).hexdigest()
                        expected_hash = file_info.get("sha256") if isinstance(file_info, dict) else file_info
                        if actual_hash != expected_hash:
                            errors.append(
                                f"文件校验失败: {fname} "
                                f"(expected: {str(expected_hash)[:16]}..., "
                                f"actual: {actual_hash[:16]}...)"
                            )
            except Exception as e:
                errors.append(f"校验和验证失败: {e}")

    except zipfile.BadZipFile:
        errors.append("不是有效的 ZIP 文件")
    except Exception as e:
        errors.append(f"读取 ZIP 文件失败: {e}")

    return len(errors) == 0, manifest, errors


def import_baseline(conn, file_path: str, on_conflict: str = None,
                    new_name: str = None) -> BaselineImportResult:
    """
    从 ZIP 文件导入基线。

    Args:
        conn: 数据库连接
        file_path: ZIP 文件路径
        on_conflict: 冲突处理策略 (reject/rename/skip)
        new_name: 重命名时的新名称

    Returns:
        BaselineImportResult
    """
    valid, manifest, errors = _validate_baseline_zip(file_path)
    if not valid:
        raise BaselineConflictError(
            BaselineConflictError.CONFLICT_FORMAT,
            "基线文件验证失败",
            {"validation_errors": errors}
        )

    original_name = manifest.get("baseline_name", "imported_baseline")
    baseline_name = new_name or original_name
    conflict_action = ""

    existing_orig = db.get_baseline_by_name(conn, original_name)
    existing_new = db.get_baseline_by_name(conn, baseline_name) if new_name else existing_orig

    if existing_orig and not new_name:
        if on_conflict is None:
            raise BaselineConflictError(
                BaselineConflictError.CONFLICT_NAME,
                f"基线名称已存在: '{original_name}'，请指定 --on-conflict 策略",
                {"existing_baseline_id": existing_orig["id"], "original_name": original_name}
            )
        elif on_conflict == CONFLICT_REJECT:
            db.add_baseline_audit_log(
                conn,
                action=db.AUDIT_ACTION_BASELINE_IMPORT,
                baseline_name=original_name,
                result=db.AUDIT_RESULT_BLOCKED,
                error_message=f"导入拒绝：基线名称已存在 '{original_name}' (reject)"
            )
            return BaselineImportResult(
                success=False,
                original_name=original_name,
                final_name=original_name,
                conflict_action=CONFLICT_REJECT,
                error_message=f"基线名称已存在: '{original_name}'"
            )
        elif on_conflict == CONFLICT_SKIP:
            db.add_baseline_audit_log(
                conn,
                action=db.AUDIT_ACTION_BASELINE_IMPORT,
                baseline_name=original_name,
                result=db.AUDIT_RESULT_SUCCESS,
                details={"skipped": True, "reason": "name_conflict"}
            )
            return BaselineImportResult(
                success=True,
                baseline_id=0,
                original_name=original_name,
                final_name=original_name,
                conflict_action=CONFLICT_SKIP
            )
        elif on_conflict == CONFLICT_RENAME:
            if new_name:
                baseline_name = new_name
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                baseline_name = f"{original_name}_imported_{timestamp}"
            conflict_action = CONFLICT_RENAME
        else:
            raise BaselineError(f"未知的冲突处理策略: {on_conflict}")
    elif existing_orig and new_name and existing_new:
        if on_conflict == CONFLICT_RENAME:
            conflict_action = CONFLICT_RENAME
        elif on_conflict == CONFLICT_REJECT:
            db.add_baseline_audit_log(
                conn,
                action=db.AUDIT_ACTION_BASELINE_IMPORT,
                baseline_name=baseline_name,
                result=db.AUDIT_RESULT_BLOCKED,
                error_message=f"导入拒绝：重命名后名称仍已存在 '{baseline_name}' (reject)"
            )
            return BaselineImportResult(
                success=False,
                original_name=original_name,
                final_name=baseline_name,
                conflict_action=CONFLICT_REJECT,
                error_message=f"重命名后名称仍已存在: '{baseline_name}'"
            )
        elif on_conflict is None:
            raise BaselineConflictError(
                BaselineConflictError.CONFLICT_NAME,
                f"重命名后名称仍已存在: '{baseline_name}'，请指定其他 --new-name",
                {"existing_baseline_id": existing_new["id"], "original_name": original_name, "final_name": baseline_name}
            )
    elif existing_orig and new_name and not existing_new:
        conflict_action = CONFLICT_RENAME
    elif new_name and not existing_orig:
        conflict_action = CONFLICT_RENAME

    with zipfile.ZipFile(file_path, "r") as zf:
        config = json.loads(zf.read("config.json").decode("utf-8"))
        metric_thresholds = json.loads(zf.read("metric_thresholds.json").decode("utf-8"))
        source_summary = json.loads(zf.read("source_summary.json").decode("utf-8"))

    src_info = manifest.get("source", {})
    original_baseline_id = src_info.get("original_baseline_id")

    config_version = manifest.get("config_version", config.get("version", 1))

    baseline_id = db.create_baseline(
        conn,
        name=baseline_name,
        description=manifest.get("description"),
        source_batch_id=None,
        source_run_id=None,
        source_batch_name=src_info.get("source_batch_name"),
        source_run_number=src_info.get("source_run_number"),
        config_version=config_version,
        config=config,
        metric_thresholds=metric_thresholds,
        source_summary=source_summary,
        original_baseline_id=original_baseline_id,
        imported_from=os.path.abspath(file_path)
    )

    last_check = manifest.get("last_check", {})
    if last_check.get("status") and last_check.get("summary"):
        db.update_baseline_last_check(
            conn, baseline_id, last_check["status"], last_check["summary"]
        )

    db.add_baseline_audit_log(
        conn,
        action=db.AUDIT_ACTION_BASELINE_IMPORT,
        baseline_id=baseline_id,
        baseline_name=baseline_name,
        result=db.AUDIT_RESULT_SUCCESS,
        previous_config=None,
        new_config=config,
        details={
            "original_name": original_name,
            "final_name": baseline_name,
            "original_baseline_id": original_baseline_id,
            "conflict_action": conflict_action,
            "imported_from": file_path
        }
    )

    logger.info(
        f"基线已导入: id={baseline_id}, name='{baseline_name}', "
        f"original_name='{original_name}', conflict_action='{conflict_action}'"
    )

    return BaselineImportResult(
        success=True,
        baseline_id=baseline_id,
        original_name=original_name,
        final_name=baseline_name,
        conflict_action=conflict_action
    )
