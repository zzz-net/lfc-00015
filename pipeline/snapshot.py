"""运行包快照模块 - 导出/导入/重放实验包，支持可复现实验"""
import os
import json
import zipfile
import hashlib
import shutil
import tempfile
import logging
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from . import database as db
from . import processor as proc
from .config import get_default_config

logger = logging.getLogger("pipeline.snapshot")


class SnapshotError(Exception):
    """快照异常基类"""
    pass


class SnapshotNotFoundError(SnapshotError):
    """快照不存在"""
    pass


class SnapshotConflictError(SnapshotError):
    """快照冲突异常"""
    CONFLICT_NAME = "name_exists"
    CONFLICT_SOURCE_MISSING = "source_missing"
    CONFLICT_CONFIG_VERSION = "config_version_conflict"
    CONFLICT_FORMAT = "format_incompatible"

    def __init__(self, conflict_type: str, message: str, details: Dict[str, Any] = None):
        super().__init__(message)
        self.conflict_type = conflict_type
        self.details = details or {}


class SnapshotImportResult:
    """快照导入结果"""
    ACTION_REJECT = "reject"
    ACTION_RENAME = "rename"
    ACTION_SKIP = "skip"

    def __init__(self, success: bool, snapshot_id: int = None, action: str = None,
                 message: str = None, original_name: str = None,
                 final_name: str = None, original_batch_id: int = None,
                 imported_from: str = None):
        self.success = success
        self.snapshot_id = snapshot_id
        self.action = action
        self.message = message
        self.original_name = original_name
        self.final_name = final_name
        self.original_batch_id = original_batch_id
        self.imported_from = imported_from


class SnapshotReplayResult:
    """快照重放结果"""
    def __init__(self, success: bool, snapshot_id: int = None,
                 new_batch_id: int = None, new_run_id: int = None,
                 metrics_comparison: Dict[str, Any] = None,
                 differences: List[Dict[str, Any]] = None,
                 failures: List[str] = None,
                 acceptable: bool = False,
                 message: str = None):
        self.success = success
        self.snapshot_id = snapshot_id
        self.new_batch_id = new_batch_id
        self.new_run_id = new_run_id
        self.metrics_comparison = metrics_comparison or {}
        self.differences = differences or []
        self.failures = failures or []
        self.acceptable = acceptable
        self.message = message


# ============ 内部工具函数 ============

def _compute_sha256(file_path: str) -> str:
    """计算文件 SHA256 哈希"""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def _compute_json_sha256(data: Dict[str, Any]) -> str:
    """计算 JSON 数据的 SHA256 哈希"""
    json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_str.encode("utf-8")).hexdigest()


def _get_dependency_versions() -> Dict[str, str]:
    """获取关键依赖版本"""
    versions = {}
    for pkg_name in ["pandas", "numpy", "click", "tabulate"]:
        try:
            module = __import__(pkg_name)
            versions[pkg_name] = getattr(module, "__version__", "unknown")
        except (ImportError, AttributeError):
            versions[pkg_name] = "unknown"
    versions["python"] = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    versions["sqlite"] = db.sqlite3.sqlite_version
    versions["snapshot_format"] = db.SNAPSHOT_FORMAT_VERSION
    return versions


def _generate_source_summary(csv_path: str, sample_rows: int = 10) -> Dict[str, Any]:
    """生成源 CSV 摘要信息（不包含完整数据）"""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 文件不存在: {csv_path}")

    file_size = os.path.getsize(csv_path)
    file_sha256 = _compute_sha256(csv_path)

    try:
        df, row_errors = proc.import_csv(csv_path)
        total_rows = len(df)
        columns = list(df.columns)

        sample_data = []
        if len(df) > 0:
            sample_indices = np.linspace(0, len(df) - 1, min(sample_rows, len(df)), dtype=int)
            for idx in sample_indices:
                row = df.iloc[idx]
                sample_row = {"row_number": int(row.get("_row_number", idx + 2))}
                for col in columns:
                    if col == "_row_number":
                        continue
                    val = row[col]
                    if pd.isna(val):
                        sample_row[col] = None
                    elif isinstance(val, (np.integer,)):
                        sample_row[col] = int(val)
                    elif isinstance(val, (np.floating,)):
                        sample_row[col] = float(val)
                    elif col == "timestamp":
                        sample_row[col] = str(val)
                    else:
                        sample_row[col] = float(val) if isinstance(val, (int, float)) else str(val)
                sample_data.append(sample_row)

        column_stats = {}
        for col in columns:
            if col in ("timestamp", "_row_number"):
                continue
            series = df[col].dropna()
            if len(series) > 0:
                column_stats[col] = {
                    "count": int(len(series)),
                    "null_count": int(df[col].isna().sum()),
                    "mean": float(series.mean()),
                    "std": float(series.std()) if len(series) > 1 else 0.0,
                    "min": float(series.min()),
                    "max": float(series.max()),
                    "median": float(series.median())
                }

        return {
            "file_name": os.path.basename(csv_path),
            "file_size": file_size,
            "file_sha256": file_sha256,
            "total_rows": total_rows,
            "error_rows": len(row_errors),
            "columns": [c for c in columns if c != "_row_number"],
            "sample_rows": sample_data,
            "column_statistics": column_stats,
            "time_range": {
                "start": str(df["timestamp"].min()) if "timestamp" in df.columns and len(df) > 0 else None,
                "end": str(df["timestamp"].max()) if "timestamp" in df.columns and len(df) > 0 else None
            }
        }
    except Exception as e:
        logger.warning(f"生成源数据摘要失败: {e}")
        return {
            "file_name": os.path.basename(csv_path),
            "file_size": file_size,
            "file_sha256": file_sha256,
            "total_rows": 0,
            "error_rows": 0,
            "columns": [],
            "sample_rows": [],
            "column_statistics": {},
            "time_range": {"start": None, "end": None},
            "summary_error": str(e)
        }


def _build_manifest(batch: Dict[str, Any], run: Dict[str, Any],
                    metrics: List[Dict[str, Any]],
                    anomalies: List[Dict[str, Any]],
                    errors: List[Dict[str, Any]],
                    source_summary: Dict[str, Any],
                    dependencies: Dict[str, str],
                    snapshot_type: str) -> Dict[str, Any]:
    """构建快照 manifest"""
    config = json.loads(run["config_json"]) if isinstance(run["config_json"], str) else run["config_json"]

    manifest = {
        "format_version": db.SNAPSHOT_FORMAT_VERSION,
        "snapshot_type": snapshot_type,
        "created_at": datetime.now().isoformat(),
        "source": {
            "original_batch_id": batch["id"],
            "original_batch_name": batch["name"],
            "original_run_id": run["id"],
            "original_run_number": run["run_number"],
            "original_source_file": batch["source_file"],
            "original_created_at": batch["created_at"],
            "original_processed_at": run["finished_at"] or run["started_at"]
        },
        "config": {
            "version": config.get("version", 1),
            "config": config
        },
        "source_summary": source_summary,
        "metrics_summary": {
            "total_metrics": len(metrics),
            "metrics": metrics,
            "rows_processed": run["rows_processed"],
            "rows_errors": run["rows_errors"]
        },
        "anomalies_summary": {
            "total_anomalies": len(anomalies),
            "anomalies": anomalies[:1000],
            "anomaly_type_counts": {}
        },
        "errors_summary": {
            "total_errors": len(errors),
            "errors": errors[:1000],
            "error_type_counts": {}
        },
        "dependencies": dependencies,
        "checksums": {}
    }

    for a in anomalies:
        atype = a["anomaly_type"]
        manifest["anomalies_summary"]["anomaly_type_counts"][atype] = \
            manifest["anomalies_summary"]["anomaly_type_counts"].get(atype, 0) + 1

    for e in errors:
        etype = e["error_type"]
        manifest["errors_summary"]["error_type_counts"][etype] = \
            manifest["errors_summary"]["error_type_counts"].get(etype, 0) + 1

    manifest["checksums"]["config"] = _compute_json_sha256(config)
    manifest["checksums"]["metrics"] = _compute_json_sha256({"metrics": metrics})
    manifest["checksums"]["anomalies"] = _compute_json_sha256({"anomalies": anomalies[:1000]})
    manifest["checksums"]["source_summary"] = _compute_json_sha256(source_summary)
    manifest["checksums"]["manifest"] = _compute_json_sha256({
        k: v for k, v in manifest.items() if k != "checksums"
    })

    return manifest


# ============ 导出功能 ============

def export_snapshot(conn, name: str, batch_id: int, run_id: int = None,
                    output_path: str = None,
                    snapshot_type: str = db.SNAPSHOT_TYPE_RUN) -> Dict[str, Any]:
    """
    导出运行包快照。

    Args:
        conn: 数据库连接
        name: 快照名称
        batch_id: 批次 ID
        run_id: 运行 ID（None 时使用最新 run）
        output_path: 输出 ZIP 文件路径
        snapshot_type: 快照类型（batch/run）

    Returns:
        包含 snapshot_id, file_path, checksum 等信息的 dict
    """
    batch = db.get_batch(conn, batch_id)
    if not batch:
        raise SnapshotError(f"批次不存在: {batch_id}")

    if run_id is None:
        run = db.get_latest_run(conn, batch_id)
        if not run:
            raise SnapshotError(f"批次 {batch_id} 尚未运行，无法导出快照")
        run_id = run["id"]
    else:
        run = db.get_run(conn, run_id)
        if not run or run["batch_id"] != batch_id:
            raise SnapshotError(f"运行 {run_id} 不存在或不属于批次 {batch_id}")

    existing = db.get_snapshot_by_name(conn, name)
    if existing:
        raise SnapshotConflictError(
            SnapshotConflictError.CONFLICT_NAME,
            f"快照名称已存在: '{name}'",
            {"existing_snapshot_id": existing["id"]}
        )

    metrics = db.get_metrics(conn, run_id)
    anomalies = db.get_anomalies(conn, run_id)
    errors = db.get_row_errors(conn, run_id)

    source_summary = _generate_source_summary(batch["source_file"])
    dependencies = _get_dependency_versions()

    manifest = _build_manifest(
        batch, run, metrics, anomalies, errors,
        source_summary, dependencies, snapshot_type
    )

    if output_path is None:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(conn.execute("PRAGMA database_list").fetchone()[2])), "snapshots")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"snapshot_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    file_contents = {
        "manifest.json": json.dumps(manifest, indent=2, ensure_ascii=False),
        "config.json": json.dumps(manifest["config"]["config"], indent=2, ensure_ascii=False),
        "metrics.json": json.dumps(metrics, indent=2, ensure_ascii=False),
        "anomalies.json": json.dumps(anomalies, indent=2, ensure_ascii=False),
        "errors.json": json.dumps(errors, indent=2, ensure_ascii=False),
        "source_summary.json": json.dumps(source_summary, indent=2, ensure_ascii=False),
        "dependencies.json": json.dumps(dependencies, indent=2, ensure_ascii=False)
    }

    checksum_files = {}
    overall_hash = hashlib.sha256()
    for fname, content in file_contents.items():
        content_bytes = content.encode("utf-8")
        file_hash = hashlib.sha256(content_bytes).hexdigest()
        checksum_files[fname] = {
            "sha256": file_hash,
            "file_size": len(content_bytes)
        }
        overall_hash.update(content_bytes)

    checksum_data = {
        "files": checksum_files,
        "overall_sha256": overall_hash.hexdigest()
    }
    checksum_content = json.dumps(checksum_data, indent=2, ensure_ascii=False)
    file_contents["checksum.json"] = checksum_content

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname, content in file_contents.items():
            zf.writestr(fname, content)

    file_size = os.path.getsize(output_path)
    file_sha256 = _compute_sha256(output_path)

    snapshot_id = db.create_snapshot(
        conn,
        name=name,
        snapshot_type=snapshot_type,
        source_batch_id=batch_id,
        source_run_id=run_id,
        source_batch_name=batch["name"],
        source_run_number=run["run_number"],
        config_version=manifest["config"]["version"],
        manifest=manifest,
        file_path=os.path.abspath(output_path),
        file_size=file_size,
        checksum_sha256=file_sha256
    )

    db.add_snapshot_audit_log(
        conn,
        action=db.AUDIT_ACTION_SNAPSHOT_EXPORT,
        snapshot_id=snapshot_id,
        snapshot_name=name,
        batch_id=batch_id,
        run_id=run_id,
        result=db.AUDIT_RESULT_SUCCESS,
        details={"output_path": output_path, "file_size": file_size, "checksum": file_sha256}
    )

    logger.info(
        f"快照已导出: id={snapshot_id}, name='{name}', "
        f"batch_id={batch_id}, run_id={run_id}, "
        f"path='{output_path}', size={file_size} bytes"
    )

    return {
        "snapshot_id": snapshot_id,
        "name": name,
        "file_path": os.path.abspath(output_path),
        "file_size": file_size,
        "checksum_sha256": file_sha256,
        "manifest": manifest
    }


# ============ 导入功能 ============

def _validate_snapshot_zip(zip_path: str) -> Tuple[bool, Dict[str, Any], List[str]]:
    """验证快照 ZIP 文件完整性"""
    if not os.path.exists(zip_path):
        return False, {}, [f"文件不存在: {zip_path}"]

    errors = []
    manifest = {}

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            required_files = ["manifest.json", "config.json", "metrics.json",
                              "anomalies.json", "errors.json", "source_summary.json",
                              "dependencies.json", "checksum.json"]
            file_list = zf.namelist()

            for req_file in required_files:
                if req_file not in file_list:
                    errors.append(f"缺少必需文件: {req_file}")

            if errors:
                return False, {}, errors

            try:
                manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
            except Exception as e:
                errors.append(f"无法解析 manifest.json: {e}")
                return False, {}, errors

            format_version = manifest.get("format_version", "0.0")
            if format_version != db.SNAPSHOT_FORMAT_VERSION:
                try:
                    major = format_version.split(".")[0]
                    current_major = db.SNAPSHOT_FORMAT_VERSION.split(".")[0]
                    if major != current_major:
                        errors.append(
                            f"快照格式版本不兼容: 导入 {format_version}, 当前 {db.SNAPSHOT_FORMAT_VERSION}"
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
                            errors.append(f"文件校验失败: {fname} (expected: {expected_hash[:16]}..., actual: {actual_hash[:16]}...)")
            except Exception as e:
                errors.append(f"校验和验证失败: {e}")

    except zipfile.BadZipFile:
        errors.append("不是有效的 ZIP 文件")
    except Exception as e:
        errors.append(f"读取 ZIP 文件失败: {e}")

    return len(errors) == 0, manifest, errors


def import_snapshot(conn, file_path: str,
                    on_conflict: str = None,
                    new_name: str = None) -> SnapshotImportResult:
    """
    从 ZIP 文件导入快照。

    Args:
        conn: 数据库连接
        file_path: 快照 ZIP 文件路径
        on_conflict: 冲突处理策略 (reject/rename/skip)
        new_name: 重命名时使用的新名称

    Returns:
        SnapshotImportResult
    """
    if not os.path.exists(file_path):
        raise SnapshotError(f"快照文件不存在: {file_path}")

    valid, manifest, errors = _validate_snapshot_zip(file_path)
    if not valid:
        raise SnapshotConflictError(
            SnapshotConflictError.CONFLICT_FORMAT,
            "快照文件验证失败",
            {"validation_errors": errors}
        )

    original_name = manifest.get("source", {}).get("original_batch_name", "imported_snapshot")
    snapshot_name = new_name or f"{original_name}_snapshot"
    original_batch_id = manifest.get("source", {}).get("original_batch_id")
    original_run_id = manifest.get("source", {}).get("original_run_id")
    config_version = manifest.get("config", {}).get("version", 1)

    source_file = manifest.get("source", {}).get("original_source_file", "")
    source_file_exists = os.path.exists(source_file)

    existing = db.get_snapshot_by_name(conn, snapshot_name)
    if existing:
        if on_conflict is None:
            raise SnapshotConflictError(
                SnapshotConflictError.CONFLICT_NAME,
                f"快照名称已存在: '{snapshot_name}'",
                {
                    "existing_snapshot_id": existing["id"],
                    "available_actions": ["reject", "rename", "skip"]
                }
            )
        elif on_conflict == SnapshotImportResult.ACTION_REJECT:
            db.add_snapshot_audit_log(
                conn,
                action=db.AUDIT_ACTION_SNAPSHOT_IMPORT,
                snapshot_name=snapshot_name,
                result=db.AUDIT_RESULT_BLOCKED,
                error_message=f"同名快照已存在，拒绝导入: '{snapshot_name}'",
                details={"original_name": original_name, "file_path": file_path}
            )
            return SnapshotImportResult(
                success=False,
                action=SnapshotImportResult.ACTION_REJECT,
                message=f"已拒绝导入同名快照: '{snapshot_name}'",
                original_name=original_name,
                imported_from=file_path
            )
        elif on_conflict == SnapshotImportResult.ACTION_RENAME:
            final_name = new_name or f"{snapshot_name}_imported"
            counter = 1
            while db.get_snapshot_by_name(conn, final_name):
                final_name = f"{snapshot_name}_imported_{counter}"
                counter += 1
            snapshot_name = final_name
        elif on_conflict == SnapshotImportResult.ACTION_SKIP:
            db.add_snapshot_audit_log(
                conn,
                action=db.AUDIT_ACTION_SNAPSHOT_IMPORT,
                snapshot_name=snapshot_name,
                result=db.AUDIT_RESULT_BLOCKED,
                error_message=f"同名快照已存在，跳过导入: '{snapshot_name}'",
                details={"original_name": original_name, "file_path": file_path}
            )
            return SnapshotImportResult(
                success=False,
                action=SnapshotImportResult.ACTION_SKIP,
                message=f"已跳过同名快照: '{snapshot_name}'",
                original_name=original_name,
                imported_from=file_path
            )
        else:
            raise SnapshotError(f"未知冲突处理策略: {on_conflict}")

    if not source_file_exists:
        logger.warning(
            f"快照引用的源文件不存在: {source_file}。"
            f"导入可以完成，但 replay 将需要指定新的源文件。"
        )

    source_batch_id = None
    source_run_id = None
    source_run_number = manifest.get("source", {}).get("original_run_number", 1)

    file_size = os.path.getsize(file_path)
    file_sha256 = _compute_sha256(file_path)
    imported_from = f"{file_path}@{datetime.now().isoformat()}"

    snapshot_id = db.create_snapshot(
        conn,
        name=snapshot_name,
        snapshot_type=manifest.get("snapshot_type", db.SNAPSHOT_TYPE_RUN),
        source_batch_id=source_batch_id,
        source_run_id=source_run_id,
        source_batch_name=original_name,
        source_run_number=source_run_number,
        config_version=config_version,
        manifest=manifest,
        file_path=os.path.abspath(file_path),
        file_size=file_size,
        checksum_sha256=file_sha256,
        original_batch_id=original_batch_id,
        original_run_id=original_run_id,
        imported_from=imported_from
    )

    db.add_snapshot_audit_log(
        conn,
        action=db.AUDIT_ACTION_SNAPSHOT_IMPORT,
        snapshot_id=snapshot_id,
        snapshot_name=snapshot_name,
        result=db.AUDIT_RESULT_SUCCESS,
        details={
            "original_name": original_name,
            "final_name": snapshot_name,
            "original_batch_id": original_batch_id,
            "source_file_exists": source_file_exists,
            "source_file": source_file,
            "file_path": file_path,
            "config_version": config_version
        }
    )

    action = SnapshotImportResult.ACTION_RENAME if new_name or on_conflict == SnapshotImportResult.ACTION_RENAME else None
    logger.info(
        f"快照已导入: id={snapshot_id}, name='{snapshot_name}', "
        f"original='{original_name}', path='{file_path}'"
    )

    return SnapshotImportResult(
        success=True,
        snapshot_id=snapshot_id,
        action=action,
        message=f"已导入快照 '{snapshot_name}'",
        original_name=original_name,
        final_name=snapshot_name,
        original_batch_id=original_batch_id,
        imported_from=imported_from
    )


# ============ Replay 功能 ============

def replay_snapshot(conn, snapshot_id: int, new_batch_name: str = None,
                    csv_path: str = None,
                    tolerance_pct: float = 1.0) -> SnapshotReplayResult:
    """
    使用快照中的配置和样本重新运行实验，对比原指标。

    Args:
        conn: 数据库连接
        snapshot_id: 快照 ID
        new_batch_name: 新批次名称（默认自动生成）
        csv_path: 新的 CSV 源文件路径（默认使用快照中的源文件）
        tolerance_pct: 指标差异容忍百分比（默认 1%）

    Returns:
        SnapshotReplayResult
    """
    snapshot = db.get_snapshot(conn, snapshot_id)
    if not snapshot:
        raise SnapshotNotFoundError(f"快照不存在: {snapshot_id}")

    manifest = snapshot["manifest"]
    original_config = manifest["config"]["config"]
    original_metrics = manifest["metrics_summary"]["metrics"]
    source_summary = manifest["source_summary"]

    if csv_path is None:
        csv_path = manifest.get("source", {}).get("original_source_file", "")
        if not os.path.exists(csv_path):
            raise SnapshotConflictError(
                SnapshotConflictError.CONFLICT_SOURCE_MISSING,
                f"源文件不存在且未指定替代文件: {csv_path}",
                {"missing_file": csv_path}
            )

    if not os.path.exists(csv_path):
        raise SnapshotError(f"指定的源文件不存在: {csv_path}")

    if new_batch_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_batch_name = f"replay_{snapshot['name']}_{timestamp}"

    existing_batch = db.get_batch_by_name(conn, new_batch_name)
    if existing_batch:
        raise SnapshotConflictError(
            SnapshotConflictError.CONFLICT_NAME,
            f"批次名称已存在: '{new_batch_name}'",
            {"existing_batch_id": existing_batch["id"]}
        )

    current_cfg = get_default_config()
    current_version = current_cfg.get("version", 1)
    original_version = original_config.get("version", 1)
    if current_version != original_version:
        logger.warning(
            f"配置版本不匹配: 快照 v{original_version}, 当前默认 v{current_version}。"
            f"将使用快照中的配置。"
        )

    replay_config = dict(original_config)
    replay_config["version"] = current_version + 1

    try:
        from . import processor as proc_module

        new_batch_id = db.create_batch(conn, new_batch_name, os.path.abspath(csv_path), replay_config)

        db.add_snapshot_audit_log(
            conn,
            action=db.AUDIT_ACTION_SNAPSHOT_REPLAY,
            snapshot_id=snapshot_id,
            snapshot_name=snapshot["name"],
            batch_id=new_batch_id,
            result=db.AUDIT_RESULT_SUCCESS,
            previous_config=original_config,
            new_config=replay_config,
            details={"new_batch_name": new_batch_name, "csv_path": csv_path}
        )

        run_id, run_number = db.create_run(conn, new_batch_id, replay_config)
        db.update_batch_status(conn, new_batch_id, db.BATCH_STATUS_PENDING if "BATCH_STATUS_PENDING" in dir(db) else "pending")

        try:
            raw_df, row_errors = proc.import_csv(csv_path)
            for err in row_errors:
                db.add_row_error(conn, run_id, err.row_number, err.error_type,
                                 err.error_detail, err.raw_data)

            cleaned_df = proc.clean_data(raw_df, replay_config)
            filled_df = proc.handle_missing_values(cleaned_df, replay_config)

            new_metrics = proc.compute_metrics(filled_df, replay_config)
            for m in new_metrics:
                db.add_metric(conn, run_id, m["sensor_name"], m["metric_name"], m["metric_value"])

            new_anomalies = proc.detect_anomalies(filled_df, replay_config)
            for a in new_anomalies:
                db.add_anomaly(conn, run_id, a["sensor_name"], a["row_number"],
                               a["timestamp"], a["value"], a["anomaly_type"])

            rows_processed = len(filled_df)
            rows_errors = len(row_errors)
            db.finish_run(conn, run_id, "success", rows_processed, rows_errors)
            db.update_batch_status(conn, new_batch_id, "processed")

            conn.commit()

            metrics_comparison = _compare_metrics(original_metrics, new_metrics, tolerance_pct)

            differences = []
            failures = []

            if metrics_comparison["metrics_with_diff"] > 0:
                for diff in metrics_comparison["differences"]:
                    differences.append({
                        "sensor": diff["sensor"],
                        "metric": diff["metric"],
                        "original_value": diff["original"],
                        "new_value": diff["new"],
                        "abs_diff": diff["abs_diff"],
                        "rel_diff_pct": diff["rel_diff_pct"],
                        "within_tolerance": diff["within_tolerance"]
                    })
                    if not diff["within_tolerance"]:
                        failures.append(
                            f"{diff['sensor']}.{diff['metric']}: "
                            f"{diff['original']:.6f} → {diff['new']:.6f} "
                            f"(diff: {diff['rel_diff_pct']:.2f}%, tolerance: {tolerance_pct}%)"
                        )

            acceptable = len(failures) == 0

            db.add_snapshot_audit_log(
                conn,
                action=db.AUDIT_ACTION_SNAPSHOT_REPLAY,
                snapshot_id=snapshot_id,
                snapshot_name=snapshot["name"],
                batch_id=new_batch_id,
                run_id=run_id,
                result=db.AUDIT_RESULT_SUCCESS,
                details={
                    "differences_count": len(differences),
                    "failures_count": len(failures),
                    "acceptable": acceptable,
                    "tolerance_pct": tolerance_pct
                }
            )

            logger.info(
                f"快照重放完成: snapshot_id={snapshot_id}, "
                f"new_batch_id={new_batch_id}, run_id={run_id}, "
                f"differences={len(differences)}, failures={len(failures)}, "
                f"acceptable={acceptable}"
            )

            return SnapshotReplayResult(
                success=True,
                snapshot_id=snapshot_id,
                new_batch_id=new_batch_id,
                new_run_id=run_id,
                metrics_comparison=metrics_comparison,
                differences=differences,
                failures=failures,
                acceptable=acceptable,
                message="重放完成" + ("，所有指标在容忍范围内" if acceptable else f"，有 {len(failures)} 个指标超出容忍范围")
            )

        except Exception as e:
            db.finish_run(conn, run_id, "failed", error_message=str(e))
            db.update_batch_status(conn, new_batch_id, "failed", error_message=str(e))
            conn.commit()

            db.add_snapshot_audit_log(
                conn,
                action=db.AUDIT_ACTION_SNAPSHOT_REPLAY,
                snapshot_id=snapshot_id,
                snapshot_name=snapshot["name"],
                batch_id=new_batch_id,
                run_id=run_id,
                result=db.AUDIT_RESULT_FAILED,
                error_message=str(e)
            )

            return SnapshotReplayResult(
                success=False,
                snapshot_id=snapshot_id,
                new_batch_id=new_batch_id,
                new_run_id=run_id,
                failures=[f"处理失败: {e}"],
                message=f"重放失败: {e}"
            )

    except Exception as e:
        db.add_snapshot_audit_log(
            conn,
            action=db.AUDIT_ACTION_SNAPSHOT_REPLAY,
            snapshot_id=snapshot_id,
            snapshot_name=snapshot["name"],
            result=db.AUDIT_RESULT_FAILED,
            error_message=str(e)
        )
        raise


def _compare_metrics(original_metrics: List[Dict[str, Any]],
                     new_metrics: List[Dict[str, Any]],
                     tolerance_pct: float = 1.0) -> Dict[str, Any]:
    """对比原始指标和新指标"""
    original_map = {}
    for m in original_metrics:
        key = f"{m['sensor_name']}::{m['metric_name']}"
        original_map[key] = m["metric_value"]

    new_map = {}
    for m in new_metrics:
        key = f"{m['sensor_name']}::{m['metric_name']}"
        new_map[key] = m["metric_value"]

    all_keys = set(original_map.keys()) | set(new_map.keys())
    differences = []
    metrics_with_diff = 0
    metrics_out_of_tolerance = 0

    for key in sorted(all_keys):
        sensor, metric = key.split("::", 1)
        orig_val = original_map.get(key)
        new_val = new_map.get(key)

        if orig_val is None or new_val is None:
            differences.append({
                "sensor": sensor,
                "metric": metric,
                "original": orig_val,
                "new": new_val,
                "abs_diff": None,
                "rel_diff_pct": None,
                "within_tolerance": False,
                "note": "仅在一侧存在"
            })
            metrics_with_diff += 1
            metrics_out_of_tolerance += 1
            continue

        abs_diff = abs(new_val - orig_val)
        if orig_val != 0:
            rel_diff_pct = (abs_diff / abs(orig_val)) * 100
        else:
            rel_diff_pct = 0.0 if abs_diff == 0 else float("inf")

        within_tolerance = rel_diff_pct <= tolerance_pct

        if abs_diff > 0:
            metrics_with_diff += 1
            if not within_tolerance:
                metrics_out_of_tolerance += 1

            differences.append({
                "sensor": sensor,
                "metric": metric,
                "original": orig_val,
                "new": new_val,
                "abs_diff": abs_diff,
                "rel_diff_pct": rel_diff_pct,
                "within_tolerance": within_tolerance
            })

    return {
        "total_metrics_compared": len(all_keys),
        "metrics_with_diff": metrics_with_diff,
        "metrics_out_of_tolerance": metrics_out_of_tolerance,
        "tolerance_pct": tolerance_pct,
        "differences": differences
    }
