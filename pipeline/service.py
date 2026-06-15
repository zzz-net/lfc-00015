"""批次管理服务 - 状态流转、锁定保护、重跑机制、行级错误处理"""
import os
import json
import sqlite3
import logging
from datetime import datetime
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

from . import database as db
from . import processor as proc
from .config import get_default_config, bump_config_version

logger = logging.getLogger("pipeline.service")


BATCH_STATUS_PENDING = "pending"
BATCH_STATUS_PROCESSED = "processed"
BATCH_STATUS_LOCKED = "locked"
BATCH_STATUS_FAILED = "failed"

VALID_STATUSES = {BATCH_STATUS_PENDING, BATCH_STATUS_PROCESSED, BATCH_STATUS_LOCKED, BATCH_STATUS_FAILED}


class BatchServiceError(Exception):
    """批次服务异常"""
    pass


class BatchLockedError(BatchServiceError):
    """批次已锁定异常"""
    pass


class SchemeError(BatchServiceError):
    """分析方案异常"""
    pass


class SchemeConflictError(SchemeError):
    """方案冲突异常，包含冲突详情以便用户选择"""
    CONFLICT_NAME = "name_exists"
    CONFLICT_MISSING_FIELDS = "missing_fields"
    CONFLICT_VERSION = "version_incompatible"

    def __init__(self, conflict_type: str, message: str, details: Dict[str, Any] = None):
        super().__init__(message)
        self.conflict_type = conflict_type
        self.details = details or {}


class SchemeImportResult:
    """方案导入结果"""
    ACTION_OVERWRITE = "overwrite"
    ACTION_RENAME = "rename"
    ACTION_SKIP = "skip"

    def __init__(self, success: bool, scheme_id: int = None, action: str = None,
                 message: str = None):
        self.success = success
        self.scheme_id = scheme_id
        self.action = action
        self.message = message


class SchemeCloneResult:
    """方案克隆结果"""
    def __init__(self, success: bool, source_scheme_id: int = None,
                 source_scheme_name: str = None, cloned_scheme_id: int = None,
                 cloned_scheme_name: str = None, applied_batch_id: int = None,
                 new_config_version: int = None, message: str = None):
        self.success = success
        self.source_scheme_id = source_scheme_id
        self.source_scheme_name = source_scheme_name
        self.cloned_scheme_id = cloned_scheme_id
        self.cloned_scheme_name = cloned_scheme_name
        self.applied_batch_id = applied_batch_id
        self.new_config_version = new_config_version
        self.message = message


class SchemeDeriveResult:
    """方案派生结果，含来源追溯和步骤级状态"""
    STEP_VALIDATE_SOURCE = "validate_source"
    STEP_VALIDATE_BATCH = "validate_batch"
    STEP_CHECK_LOCKED = "check_locked"
    STEP_CHECK_CONFLICT = "check_conflict"
    STEP_VALIDATE_CONFIG = "validate_config"
    STEP_CREATE_DERIVED = "create_derived"
    STEP_APPLY_TO_BATCH = "apply_to_batch"

    def __init__(self, success: bool, source_scheme_id: int = None,
                 source_scheme_name: str = None, derived_scheme_id: int = None,
                 derived_scheme_name: str = None, applied_batch_id: int = None,
                 new_config_version: int = None, failed_step: str = None,
                 message: str = None):
        self.success = success
        self.source_scheme_id = source_scheme_id
        self.source_scheme_name = source_scheme_name
        self.derived_scheme_id = derived_scheme_id
        self.derived_scheme_name = derived_scheme_name
        self.applied_batch_id = applied_batch_id
        self.new_config_version = new_config_version
        self.failed_step = failed_step
        self.message = message


class PipelineService:
    """流水线服务类"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path
        db.init_db(db_path)

    def _conn(self) -> sqlite3.Connection:
        return db.get_connection(self.db_path)

    # ========== 批次创建 ==========

    def create_batch(self, name: str, csv_path: str, config: Dict[str, Any] = None) -> int:
        """创建新批次"""
        conn = self._conn()
        try:
            existing = db.get_batch_by_name(conn, name)
            if existing:
                raise BatchServiceError(f"批次名称已存在: {name}")

            if not os.path.exists(csv_path):
                raise BatchServiceError(f"CSV 文件不存在: {csv_path}")

            cfg = config if config is not None else get_default_config()
            batch_id = db.create_batch(conn, name, os.path.abspath(csv_path), cfg)
            return batch_id
        finally:
            conn.close()

    # ========== 批次查询 ==========

    def get_batch(self, batch_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.get_batch(conn, batch_id)
        finally:
            conn.close()

    def list_batches(self) -> List[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.list_batches(conn)
        finally:
            conn.close()

    # ========== 配置管理 ==========

    def update_config(self, batch_id: int, new_config: Dict[str, Any]) -> Dict[str, Any]:
        """更新批次配置（版本号自动递增）"""
        conn = self._conn()
        try:
            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")
            if db.is_batch_locked(conn, batch_id):
                raise BatchLockedError(f"批次 {batch_id} 已锁定，无法修改配置")

            updated_cfg = bump_config_version(new_config)
            db.update_batch_config(conn, batch_id, updated_cfg)
            return updated_cfg
        finally:
            conn.close()

    def set_threshold(self, batch_id: int, zscore_threshold: float = None,
                      iqr_multiplier: float = None) -> Dict[str, Any]:
        """便捷修改异常检测阈值"""
        conn = self._conn()
        try:
            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")
            if db.is_batch_locked(conn, batch_id):
                raise BatchLockedError(f"批次 {batch_id} 已锁定，无法修改阈值")

            current_cfg = json.loads(batch["config_json"])
            if zscore_threshold is not None:
                current_cfg["anomaly_detection"]["zscore_threshold"] = zscore_threshold
            if iqr_multiplier is not None:
                current_cfg["anomaly_detection"]["iqr_multiplier"] = iqr_multiplier

            return self.update_config(batch_id, current_cfg)
        finally:
            conn.close()

    # ========== 锁定管理 ==========

    def lock_batch(self, batch_id: int) -> None:
        """锁定批次"""
        conn = self._conn()
        try:
            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")
            if batch["status"] not in (BATCH_STATUS_PROCESSED, BATCH_STATUS_LOCKED):
                raise BatchServiceError(f"只有已处理的批次才能锁定，当前状态: {batch['status']}")
            db.set_batch_locked(conn, batch_id, True)
            db.update_batch_status(conn, batch_id, BATCH_STATUS_LOCKED)
        finally:
            conn.close()

    def unlock_batch(self, batch_id: int) -> None:
        """解锁批次（仅允许从 locked -> processed）"""
        conn = self._conn()
        try:
            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")
            if batch["status"] != BATCH_STATUS_LOCKED:
                raise BatchServiceError(f"只有锁定状态才能解锁，当前状态: {batch['status']}")
            db.set_batch_locked(conn, batch_id, False)
            db.update_batch_status(conn, batch_id, BATCH_STATUS_PROCESSED)
        finally:
            conn.close()

    def is_locked(self, batch_id: int) -> bool:
        conn = self._conn()
        try:
            return db.is_batch_locked(conn, batch_id)
        finally:
            conn.close()

    # ========== 处理流程 ==========

    def process_batch(self, batch_id: int) -> Tuple[int, int]:
        """
        处理批次。
        - 若批次已锁定: 无条件抛出 BatchLockedError，不会产生新 run
        - 未锁定批次: 创建新的运行记录（run_number 递增）
        - 返回 (run_id, run_number)
        """
        conn = self._conn()
        try:
            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")

            if db.is_batch_locked(conn, batch_id):
                raise BatchLockedError(
                    f"批次 {batch_id} 已锁定，禁止重跑或产生新运行记录。"
                    f"历史结果受保护，默认导出始终指向锁定时的最新 run。"
                    f"如需修改，请先执行 unlock 解锁。"
                )

            config = json.loads(batch["config_json"])
            csv_path = batch["source_file"]

            run_id, run_number = db.create_run(conn, batch_id, config)
            db.update_batch_status(conn, batch_id, BATCH_STATUS_PENDING)

            try:
                raw_df, row_errors = proc.import_csv(csv_path)
                for err in row_errors:
                    db.add_row_error(conn, run_id, err.row_number, err.error_type,
                                     err.error_detail, err.raw_data)

                cleaned_df = proc.clean_data(raw_df, config)
                filled_df = proc.handle_missing_values(cleaned_df, config)

                metrics = proc.compute_metrics(filled_df, config)
                for m in metrics:
                    db.add_metric(conn, run_id, m["sensor_name"], m["metric_name"], m["metric_value"])

                anomalies = proc.detect_anomalies(filled_df, config)
                for a in anomalies:
                    db.add_anomaly(conn, run_id, a["sensor_name"], a["row_number"],
                                   a["timestamp"], a["value"], a["anomaly_type"])

                rows_processed = len(filled_df)
                rows_errors = len(row_errors)
                db.finish_run(conn, run_id, "success", rows_processed, rows_errors)

                if db.is_batch_locked(conn, batch_id):
                    db.update_batch_status(conn, batch_id, BATCH_STATUS_LOCKED)
                else:
                    db.update_batch_status(conn, batch_id, BATCH_STATUS_PROCESSED)

                conn.commit()
                return run_id, run_number

            except Exception as e:
                db.finish_run(conn, run_id, "failed", error_message=str(e))
                db.update_batch_status(conn, batch_id, BATCH_STATUS_FAILED, error_message=str(e))
                conn.commit()
                raise
        finally:
            conn.close()

    # ========== 运行历史查询 ==========

    def list_runs(self, batch_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.list_runs(conn, batch_id)
        finally:
            conn.close()

    def get_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.get_run(conn, run_id)
        finally:
            conn.close()

    def get_latest_run(self, batch_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.get_latest_run(conn, batch_id)
        finally:
            conn.close()

    def get_run_errors(self, run_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.get_row_errors(conn, run_id)
        finally:
            conn.close()

    def get_run_metrics(self, run_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.get_metrics(conn, run_id)
        finally:
            conn.close()

    def get_run_anomalies(self, run_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.get_anomalies(conn, run_id)
        finally:
            conn.close()

    # ========== 导出 ==========

    def export_metrics(self, batch_id: int, output_path: str, run_id: int = None) -> str:
        """导出指标到 CSV"""
        conn = self._conn()
        try:
            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")

            if run_id is None:
                latest = db.get_latest_run(conn, batch_id)
                if not latest:
                    raise BatchServiceError(f"批次 {batch_id} 尚未运行")
                target_run = latest
                run_id = latest["id"]
            else:
                target_run = db.get_run(conn, run_id)
                if not target_run or target_run["batch_id"] != batch_id:
                    raise BatchServiceError(f"运行 {run_id} 不存在或不属于批次 {batch_id}")

            metrics = db.get_metrics(conn, run_id)
            if not metrics:
                raise BatchServiceError(f"运行 {run_id} 没有指标数据")

            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            df = pd.DataFrame([{
                "batch_id": batch_id,
                "batch_name": batch["name"],
                "run_id": run_id,
                "run_number": target_run["run_number"],
                "config_version": target_run["config_version"],
                "sensor": m["sensor_name"],
                "metric": m["metric_name"],
                "value": m["metric_value"]
            } for m in metrics])
            df.to_csv(output_path, index=False, encoding="utf-8-sig")

            db.add_export(conn, batch_id, run_id, os.path.abspath(output_path), "metrics")
            return os.path.abspath(output_path)
        finally:
            conn.close()

    def export_errors(self, batch_id: int, output_path: str, run_id: int = None) -> str:
        """导出行级错误到 CSV"""
        conn = self._conn()
        try:
            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")

            if run_id is None:
                latest = db.get_latest_run(conn, batch_id)
                if not latest:
                    raise BatchServiceError(f"批次 {batch_id} 尚未运行")
                target_run = latest
                run_id = latest["id"]
            else:
                target_run = db.get_run(conn, run_id)
                if not target_run or target_run["batch_id"] != batch_id:
                    raise BatchServiceError(f"运行 {run_id} 不存在或不属于批次 {batch_id}")

            errors = db.get_row_errors(conn, run_id)
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            if errors:
                df = pd.DataFrame([{
                    "batch_id": batch_id,
                    "batch_name": batch["name"],
                    "run_id": run_id,
                    "run_number": target_run["run_number"],
                    "config_version": target_run["config_version"],
                    "row_number": e["row_number"],
                    "error_type": e["error_type"],
                    "error_detail": e["error_detail"],
                    "raw_data": e["raw_data"]
                } for e in errors])
                df.to_csv(output_path, index=False, encoding="utf-8-sig")
            else:
                with open(output_path, "w", encoding="utf-8-sig") as f:
                    f.write("batch_id,batch_name,run_id,run_number,config_version,row_number,error_type,error_detail,raw_data\n")

            db.add_export(conn, batch_id, run_id, os.path.abspath(output_path), "errors")
            return os.path.abspath(output_path)
        finally:
            conn.close()

    def export_anomalies(self, batch_id: int, output_path: str, run_id: int = None) -> str:
        """导出异常点到 CSV"""
        conn = self._conn()
        try:
            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")

            if run_id is None:
                latest = db.get_latest_run(conn, batch_id)
                if not latest:
                    raise BatchServiceError(f"批次 {batch_id} 尚未运行")
                target_run = latest
                run_id = latest["id"]
            else:
                target_run = db.get_run(conn, run_id)
                if not target_run or target_run["batch_id"] != batch_id:
                    raise BatchServiceError(f"运行 {run_id} 不存在或不属于批次 {batch_id}")

            anomalies = db.get_anomalies(conn, run_id)
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            if anomalies:
                df = pd.DataFrame([{
                    "batch_id": batch_id,
                    "batch_name": batch["name"],
                    "run_id": run_id,
                    "run_number": target_run["run_number"],
                    "config_version": target_run["config_version"],
                    "sensor": a["sensor_name"],
                    "row_number": a["row_number"],
                    "timestamp": a["timestamp"],
                    "value": a["value"],
                    "anomaly_type": a["anomaly_type"]
                } for a in anomalies])
                df.to_csv(output_path, index=False, encoding="utf-8-sig")
            else:
                with open(output_path, "w", encoding="utf-8-sig") as f:
                    f.write("batch_id,batch_name,run_id,run_number,config_version,sensor,row_number,timestamp,value,anomaly_type\n")

            db.add_export(conn, batch_id, run_id, os.path.abspath(output_path), "anomalies")
            return os.path.abspath(output_path)
        finally:
            conn.close()

    def list_exports(self, batch_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.list_exports(conn, batch_id)
        finally:
            conn.close()

    # ========== 分析方案管理 ==========

    def save_scheme(self, name: str, config: Dict[str, Any] = None,
                    description: str = None, batch_id: int = None) -> int:
        """
        保存分析方案。若指定 batch_id，则从该批次提取当前配置。
        """
        conn = self._conn()
        try:
            existing = db.get_scheme_by_name(conn, name)
            if existing:
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_NAME,
                    f"方案名称已存在: '{name}'",
                    {"existing_scheme_id": existing["id"]}
                )

            if config is None:
                if batch_id is None:
                    raise SchemeError("必须提供 config 或 batch_id 之一")
                batch = db.get_batch(conn, batch_id)
                if not batch:
                    raise SchemeError(f"批次不存在: {batch_id}")
                config = json.loads(batch["config_json"])

            valid, missing = db.validate_scheme_config(config)
            if not valid:
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_MISSING_FIELDS,
                    f"方案配置缺少必填字段: {', '.join(missing)}",
                    {"missing_fields": missing}
                )

            sid = db.create_scheme(conn, name, config, description)
            logger.info(f"分析方案已保存: id={sid}, name='{name}', batch_id={batch_id}")
            return sid
        finally:
            conn.close()

    def get_scheme(self, scheme_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.get_scheme(conn, scheme_id)
        finally:
            conn.close()

    def get_scheme_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.get_scheme_by_name(conn, name)
        finally:
            conn.close()

    def list_schemes(self) -> List[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.list_schemes(conn)
        finally:
            conn.close()

    def update_scheme(self, scheme_id: int, config: Dict[str, Any],
                      description: str = None) -> None:
        conn = self._conn()
        try:
            scheme = db.get_scheme(conn, scheme_id)
            if not scheme:
                raise SchemeError(f"方案不存在: {scheme_id}")
            valid, missing = db.validate_scheme_config(config)
            if not valid:
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_MISSING_FIELDS,
                    f"方案配置缺少必填字段: {', '.join(missing)}",
                    {"missing_fields": missing}
                )
            db.update_scheme(conn, scheme_id, config, description)
        finally:
            conn.close()

    def delete_scheme(self, scheme_id: int) -> None:
        conn = self._conn()
        try:
            scheme = db.get_scheme(conn, scheme_id)
            if not scheme:
                raise SchemeError(f"方案不存在: {scheme_id}")
            db.delete_scheme(conn, scheme_id)
        finally:
            conn.close()

    def apply_scheme_to_batch(self, scheme_id: int, batch_id: int) -> Dict[str, Any]:
        """
        将方案配置应用到批次（返回新配置，不自动重跑）。
        锁定批次不允许修改配置。
        """
        conn = self._conn()
        try:
            scheme = db.get_scheme(conn, scheme_id)
            if not scheme:
                raise SchemeError(f"方案不存在: {scheme_id}")
            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")
            if db.is_batch_locked(conn, batch_id):
                raise BatchLockedError(
                    f"批次 {batch_id} 已锁定，无法应用新方案。"
                    f"如需使用该方案进行对比分析，请使用 compare 命令直接生成报告，"
                    f"而不要修改已锁定批次的历史配置。"
                )
            new_cfg = bump_config_version(scheme["config"])
            db.update_batch_config(conn, batch_id, new_cfg)
            logger.info(f"方案已应用到批次: scheme_id={scheme_id}, scheme_name='{scheme['name']}', "
                        f"batch_id={batch_id}, new_config_version={new_cfg['version']}")
            return new_cfg
        finally:
            conn.close()

    def clone_scheme(self, source_scheme_id: int, new_name: str,
                     new_description: str = None) -> int:
        """
        基于已有方案克隆出新方案。
        - 校验源方案存在
        - 校验新名称不冲突（同名抛出 SchemeConflictError）
        - 克隆配置内容，创建新方案记录
        """
        conn = self._conn()
        try:
            source = db.get_scheme(conn, source_scheme_id)
            if not source:
                raise SchemeError(f"源方案不存在: {source_scheme_id}")

            existing = db.get_scheme_by_name(conn, new_name)
            if existing:
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_NAME,
                    f"方案名称已存在: '{new_name}'",
                    {
                        "existing_scheme_id": existing["id"],
                        "source_scheme_id": source_scheme_id,
                        "source_scheme_name": source["name"]
                    }
                )

            description = new_description if new_description is not None else source.get("description")
            config = source["config"]

            valid, missing = db.validate_scheme_config(config)
            if not valid:
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_MISSING_FIELDS,
                    f"源方案配置缺少必填字段: {', '.join(missing)}",
                    {"missing_fields": missing, "source_scheme_id": source_scheme_id}
                )

            cloned_id = db.create_scheme(conn, new_name, config, description)
            logger.info(
                f"方案已克隆: source_id={source_scheme_id}, source_name='{source['name']}', "
                f"cloned_id={cloned_id}, cloned_name='{new_name}'"
            )
            return cloned_id
        finally:
            conn.close()

    def clone_and_apply_scheme(self, source_scheme_id: int, new_name: str,
                               batch_id: int, new_description: str = None) -> SchemeCloneResult:
        """
        克隆方案并立即应用到指定批次。
        步骤：
        1. 克隆源方案为新方案（含冲突检测）
        2. 将新方案应用到批次（含锁定批次保护）
        3. 返回克隆结果（含新方案 ID、批次新配置版本等）
        """
        conn = self._conn()
        try:
            source = db.get_scheme(conn, source_scheme_id)
            if not source:
                raise SchemeError(f"源方案不存在: {source_scheme_id}")

            batch = db.get_batch(conn, batch_id)
            if not batch:
                raise BatchServiceError(f"批次不存在: {batch_id}")

            if db.is_batch_locked(conn, batch_id):
                raise BatchLockedError(
                    f"批次 {batch_id} 已锁定，无法应用克隆方案。"
                    f"如需使用该方案进行对比分析，请使用 compare 命令直接生成报告，"
                    f"而不要修改已锁定批次的历史配置。"
                )

            existing = db.get_scheme_by_name(conn, new_name)
            if existing:
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_NAME,
                    f"方案名称已存在: '{new_name}'",
                    {
                        "existing_scheme_id": existing["id"],
                        "source_scheme_id": source_scheme_id,
                        "source_scheme_name": source["name"]
                    }
                )

            description = new_description if new_description is not None else source.get("description")
            config = source["config"]

            valid, missing = db.validate_scheme_config(config)
            if not valid:
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_MISSING_FIELDS,
                    f"源方案配置缺少必填字段: {', '.join(missing)}",
                    {"missing_fields": missing, "source_scheme_id": source_scheme_id}
                )

            cloned_id = db.create_scheme(conn, new_name, config, description)
            logger.info(
                f"方案已克隆(链路): source_id={source_scheme_id}, source_name='{source['name']}', "
                f"cloned_id={cloned_id}, cloned_name='{new_name}'"
            )

            new_cfg = bump_config_version(config)
            db.update_batch_config(conn, batch_id, new_cfg)
            logger.info(
                f"克隆方案已应用到批次: scheme_id={cloned_id}, scheme_name='{new_name}', "
                f"batch_id={batch_id}, new_config_version={new_cfg['version']}"
            )

            return SchemeCloneResult(
                success=True,
                source_scheme_id=source_scheme_id,
                source_scheme_name=source["name"],
                cloned_scheme_id=cloned_id,
                cloned_scheme_name=new_name,
                applied_batch_id=batch_id,
                new_config_version=new_cfg["version"],
                message=f"方案克隆并应用成功"
            )
        finally:
            conn.close()

    def derive_scheme(self, source_scheme_id: int, new_name: str,
                      new_description: str = None) -> int:
        """
        基于已有方案派生出新方案，记录来源关系（source_scheme_id）。
        步骤级校验 + 日志：
        1. 校验源方案存在
        2. 校验新名称不冲突
        3. 校验配置完整
        4. 创建派生方案（含 source_scheme_id）
        """
        conn = self._conn()
        try:
            source = db.get_scheme(conn, source_scheme_id)
            if not source:
                logger.info(
                    f"派生失败(步骤1-校验源方案): source_id={source_scheme_id}, "
                    f"result=源方案不存在"
                )
                raise SchemeError(f"源方案不存在: {source_scheme_id}")
            logger.info(
                f"派生步骤1-校验源方案: source_id={source_scheme_id}, "
                f"source_name='{source['name']}', result=通过"
            )

            existing = db.get_scheme_by_name(conn, new_name)
            if existing:
                logger.info(
                    f"派生失败(步骤2-校验名称冲突): source_id={source_scheme_id}, "
                    f"source_name='{source['name']}', new_name='{new_name}', "
                    f"result=名称已存在(existing_id={existing['id']})"
                )
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_NAME,
                    f"方案名称已存在: '{new_name}'",
                    {
                        "existing_scheme_id": existing["id"],
                        "source_scheme_id": source_scheme_id,
                        "source_scheme_name": source["name"]
                    }
                )
            logger.info(
                f"派生步骤2-校验名称冲突: new_name='{new_name}', result=通过"
            )

            description = new_description if new_description is not None else source.get("description")
            config = source["config"]

            valid, missing = db.validate_scheme_config(config)
            if not valid:
                logger.info(
                    f"派生失败(步骤3-校验配置): source_id={source_scheme_id}, "
                    f"result=缺少必填字段({', '.join(missing)})"
                )
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_MISSING_FIELDS,
                    f"源方案配置缺少必填字段: {', '.join(missing)}",
                    {"missing_fields": missing, "source_scheme_id": source_scheme_id}
                )
            logger.info(
                f"派生步骤3-校验配置: source_id={source_scheme_id}, result=通过"
            )

            derived_id = db.create_scheme(conn, new_name, config, description,
                                          source_scheme_id=source_scheme_id)
            logger.info(
                f"派生步骤4-创建派生方案: source_id={source_scheme_id}, "
                f"source_name='{source['name']}', derived_id={derived_id}, "
                f"derived_name='{new_name}', result=成功"
            )
            return derived_id
        finally:
            conn.close()

    def derive_and_apply_scheme(self, source_scheme_id: int, new_name: str,
                                batch_id: int, new_description: str = None) -> SchemeDeriveResult:
        """
        派生方案并立即应用到指定批次。步骤级校验 + 日志，失败时记录 failed_step。
        校验顺序：源方案 → 批次存在 → 批次未锁定 → 名称不冲突 → 配置完整 → 创建方案 → 应用
        """
        conn = self._conn()
        try:
            source = db.get_scheme(conn, source_scheme_id)
            if not source:
                logger.info(
                    f"派生并应用失败(步骤1-校验源方案): source_id={source_scheme_id}, "
                    f"batch_id={batch_id}, result=源方案不存在"
                )
                raise SchemeError(f"源方案不存在: {source_scheme_id}")
            logger.info(
                f"派生并应用步骤1-校验源方案: source_id={source_scheme_id}, "
                f"source_name='{source['name']}', result=通过"
            )

            batch = db.get_batch(conn, batch_id)
            if not batch:
                logger.info(
                    f"派生并应用失败(步骤2-校验批次): source_id={source_scheme_id}, "
                    f"batch_id={batch_id}, result=批次不存在"
                )
                raise BatchServiceError(f"批次不存在: {batch_id}")
            logger.info(
                f"派生并应用步骤2-校验批次: batch_id={batch_id}, result=通过"
            )

            if db.is_batch_locked(conn, batch_id):
                logger.info(
                    f"派生并应用失败(步骤3-校验锁定): source_id={source_scheme_id}, "
                    f"batch_id={batch_id}, result=批次已锁定"
                )
                raise BatchLockedError(
                    f"批次 {batch_id} 已锁定，无法应用派生方案。"
                    f"如需使用该方案进行对比分析，请使用 compare 命令直接生成报告，"
                    f"而不要修改已锁定批次的历史配置。"
                )
            logger.info(
                f"派生并应用步骤3-校验锁定: batch_id={batch_id}, result=未锁定(通过)"
            )

            existing = db.get_scheme_by_name(conn, new_name)
            if existing:
                logger.info(
                    f"派生并应用失败(步骤4-校验名称冲突): source_id={source_scheme_id}, "
                    f"new_name='{new_name}', result=名称已存在(existing_id={existing['id']})"
                )
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_NAME,
                    f"方案名称已存在: '{new_name}'",
                    {
                        "existing_scheme_id": existing["id"],
                        "source_scheme_id": source_scheme_id,
                        "source_scheme_name": source["name"]
                    }
                )
            logger.info(
                f"派生并应用步骤4-校验名称冲突: new_name='{new_name}', result=通过"
            )

            description = new_description if new_description is not None else source.get("description")
            config = source["config"]

            valid, missing = db.validate_scheme_config(config)
            if not valid:
                logger.info(
                    f"派生并应用失败(步骤5-校验配置): source_id={source_scheme_id}, "
                    f"result=缺少必填字段({', '.join(missing)})"
                )
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_MISSING_FIELDS,
                    f"源方案配置缺少必填字段: {', '.join(missing)}",
                    {"missing_fields": missing, "source_scheme_id": source_scheme_id}
                )
            logger.info(
                f"派生并应用步骤5-校验配置: source_id={source_scheme_id}, result=通过"
            )

            derived_id = db.create_scheme(conn, new_name, config, description,
                                          source_scheme_id=source_scheme_id)
            logger.info(
                f"派生并应用步骤6-创建派生方案: source_id={source_scheme_id}, "
                f"source_name='{source['name']}', derived_id={derived_id}, "
                f"derived_name='{new_name}', result=成功"
            )

            new_cfg = bump_config_version(config)
            db.update_batch_config(conn, batch_id, new_cfg)
            logger.info(
                f"派生并应用步骤7-应用到批次: derived_id={derived_id}, "
                f"derived_name='{new_name}', batch_id={batch_id}, "
                f"new_config_version={new_cfg['version']}, result=成功"
            )

            return SchemeDeriveResult(
                success=True,
                source_scheme_id=source_scheme_id,
                source_scheme_name=source["name"],
                derived_scheme_id=derived_id,
                derived_scheme_name=new_name,
                applied_batch_id=batch_id,
                new_config_version=new_cfg["version"],
                message="方案派生并应用成功"
            )
        finally:
            conn.close()

    def export_scheme_to_file(self, scheme_id: int, file_path: str) -> str:
        """将方案导出为 JSON 文件"""
        conn = self._conn()
        try:
            scheme = db.get_scheme(conn, scheme_id)
            if not scheme:
                raise SchemeError(f"方案不存在: {scheme_id}")
            export_data = {
                "name": scheme["name"],
                "description": scheme.get("description"),
                "scheme_version": scheme["scheme_version"],
                "config": scheme["config"],
                "source_scheme_id": scheme.get("source_scheme_id"),
                "exported_at": datetime.now().isoformat()
            }
            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            return os.path.abspath(file_path)
        finally:
            conn.close()

    def import_scheme_from_file(self, file_path: str,
                                on_conflict: str = None,
                                new_name: str = None) -> SchemeImportResult:
        """
        从 JSON 文件导入方案。
        on_conflict: overwrite / rename / skip (None 时抛出异常让调用方处理)
        """
        if not os.path.exists(file_path):
            raise SchemeError(f"方案文件不存在: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        name = data.get("name")
        if not name:
            raise SchemeError("导入文件缺少 'name' 字段")

        config = data.get("config")
        if not config:
            raise SchemeError("导入文件缺少 'config' 字段")

        imported_version = data.get("scheme_version", "0.0")
        version_ok, version_msg = db.check_scheme_version_compatibility(imported_version)
        if not version_ok:
            if on_conflict is None:
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_VERSION,
                    f"方案版本不兼容: {version_msg}",
                    {"imported_version": imported_version, "current_version": db.SCHEME_VERSION}
                )
            elif on_conflict == SchemeImportResult.ACTION_SKIP:
                return SchemeImportResult(False, None, SchemeImportResult.ACTION_SKIP, version_msg)
            else:
                pass

        valid, missing = db.validate_scheme_config(config)
        if not valid:
            if on_conflict is None:
                raise SchemeConflictError(
                    SchemeConflictError.CONFLICT_MISSING_FIELDS,
                    f"方案配置缺少必填字段: {', '.join(missing)}",
                    {"missing_fields": missing}
                )
            elif on_conflict == SchemeImportResult.ACTION_SKIP:
                return SchemeImportResult(False, None, SchemeImportResult.ACTION_SKIP,
                                          f"缺少字段: {', '.join(missing)}")
            else:
                pass

        description = data.get("description")
        source_scheme_id = data.get("source_scheme_id")
        conn = self._conn()
        try:
            existing = db.get_scheme_by_name(conn, name)
            if existing:
                if on_conflict is None:
                    raise SchemeConflictError(
                        SchemeConflictError.CONFLICT_NAME,
                        f"方案名称已存在: '{name}'",
                        {"existing_scheme_id": existing["id"]}
                    )
                elif on_conflict == SchemeImportResult.ACTION_OVERWRITE:
                    db.update_scheme(conn, existing["id"], config, description)
                    result = SchemeImportResult(True, existing["id"],
                                                SchemeImportResult.ACTION_OVERWRITE,
                                                f"已覆盖方案 '{name}'")
                    logger.info(f"方案导入(覆盖): file='{file_path}', scheme_id={existing['id']}, name='{name}'")
                    return result
                elif on_conflict == SchemeImportResult.ACTION_RENAME:
                    final_name = new_name or f"{name}_imported"
                    counter = 1
                    while db.get_scheme_by_name(conn, final_name):
                        final_name = f"{name}_imported_{counter}"
                        counter += 1
                    sid = db.create_scheme(conn, final_name, config, description,
                                           source_scheme_id=source_scheme_id)
                    result = SchemeImportResult(True, sid, SchemeImportResult.ACTION_RENAME,
                                                f"已重命名导入: '{final_name}'")
                    logger.info(f"方案导入(重命名): file='{file_path}', scheme_id={sid}, original='{name}', final='{final_name}'")
                    return result
                elif on_conflict == SchemeImportResult.ACTION_SKIP:
                    result = SchemeImportResult(False, None, SchemeImportResult.ACTION_SKIP,
                                                f"已跳过同名方案 '{name}'")
                    logger.info(f"方案导入(跳过): file='{file_path}', name='{name}' (已存在同名方案)")
                    return result
                else:
                    raise SchemeError(f"未知冲突处理策略: {on_conflict}")
            else:
                sid = db.create_scheme(conn, name, config, description,
                                       source_scheme_id=source_scheme_id)
                result = SchemeImportResult(True, sid, None, f"已导入方案 '{name}'")
                logger.info(f"方案导入: file='{file_path}', scheme_id={sid}, name='{name}'")
                return result
        finally:
            conn.close()

    # ========== 对比报告 ==========

    def generate_comparison_report(self, name: str, batch_ids: List[int],
                                   scheme_id: int = None) -> Dict[str, Any]:
        """
        生成多批次对比报告。
        锁定批次可以参与对比，但不会被修改。
        """
        if len(batch_ids) < 2:
            raise BatchServiceError("对比分析至少需要 2 个批次")

        conn = self._conn()
        try:
            scheme = None
            if scheme_id is not None:
                scheme = db.get_scheme(conn, scheme_id)
                if not scheme:
                    raise SchemeError(f"方案不存在: {scheme_id}")

            batches = []
            batch_summaries = []
            all_runs = []
            for bid in batch_ids:
                batch = db.get_batch(conn, bid)
                if not batch:
                    raise BatchServiceError(f"批次不存在: {bid}")
                batches.append(batch)
                latest_run = db.get_latest_run(conn, bid)
                if not latest_run:
                    raise BatchServiceError(f"批次 {bid} 尚未处理，无法参与对比")
                all_runs.append(latest_run)
                metrics = db.get_metrics(conn, latest_run["id"])
                anomalies = db.get_anomalies(conn, latest_run["id"])
                errors = db.get_row_errors(conn, latest_run["id"])
                batch_summaries.append({
                    "batch_id": bid,
                    "batch_name": batch["name"],
                    "status": batch["status"],
                    "locked": bool(batch["locked"]),
                    "source_file": os.path.basename(batch["source_file"]),
                    "config_version": latest_run["config_version"],
                    "run_id": latest_run["id"],
                    "run_number": latest_run["run_number"],
                    "rows_processed": latest_run["rows_processed"],
                    "rows_errors": latest_run["rows_errors"],
                    "anomalies_count": len(anomalies),
                    "metrics_count": len(metrics),
                    "processed_at": latest_run["finished_at"] or latest_run["started_at"]
                })

            metrics_diff = self._compute_metrics_diff(batches, all_runs, conn)
            anomalies_diff = self._compute_anomalies_diff(batches, all_runs, conn)

            full_report = {
                "name": name,
                "scheme": {
                    "id": scheme["id"] if scheme else None,
                    "name": scheme["name"] if scheme else None,
                    "version": scheme["scheme_version"] if scheme else None
                },
                "generated_at": datetime.now().isoformat(),
                "batch_summaries": batch_summaries,
                "metrics_diff": metrics_diff,
                "anomalies_diff": anomalies_diff
            }

            sid = db.create_comparison_report(
                conn, name,
                scheme["id"] if scheme else None,
                scheme["name"] if scheme else None,
                scheme["scheme_version"] if scheme else None,
                batch_ids, batch_summaries,
                metrics_diff, anomalies_diff, full_report
            )
            full_report["report_id"] = sid
            scheme_info = f"scheme={scheme['name']}(id={scheme['id']})" if scheme else "scheme=(无)"
            logger.info(f"对比报告已生成: id={sid}, name='{name}', {scheme_info}, batch_ids={batch_ids}")
            return full_report
        finally:
            conn.close()

    def _compute_metrics_diff(self, batches, runs, conn) -> Dict[str, Any]:
        """计算指标差异矩阵"""
        all_metrics = {}
        sensor_metric_keys = set()
        for batch, run in zip(batches, runs):
            metrics = db.get_metrics(conn, run["id"])
            bid_key = f"{batch['id']}:{batch['name']}"
            all_metrics[bid_key] = {}
            for m in metrics:
                key = f"{m['sensor_name']}::{m['metric_name']}"
                sensor_metric_keys.add(key)
                all_metrics[bid_key][key] = m["metric_value"]

        batch_keys = list(all_metrics.keys())
        per_metric = {}
        for key in sorted(sensor_metric_keys):
            sensor, metric = key.split("::", 1)
            values = {}
            for bk in batch_keys:
                values[bk] = all_metrics[bk].get(key)
            vals = [v for v in values.values() if v is not None]
            if len(vals) >= 2:
                diff = max(vals) - min(vals)
                if min(vals) != 0:
                    rel_diff_pct = (diff / abs(min(vals))) * 100
                else:
                    rel_diff_pct = None
            else:
                diff = None
                rel_diff_pct = None
            per_metric[key] = {
                "sensor": sensor,
                "metric": metric,
                "values": values,
                "abs_diff": diff,
                "rel_diff_pct": rel_diff_pct
            }

        summary = {
            "total_metrics_compared": len(per_metric),
            "metrics_with_diff": sum(1 for v in per_metric.values() if v["abs_diff"] and v["abs_diff"] > 0),
            "batch_keys": batch_keys
        }
        return {"summary": summary, "per_metric": per_metric}

    def _compute_anomalies_diff(self, batches, runs, conn) -> Dict[str, Any]:
        """计算异常数量差异"""
        per_batch = {}
        per_sensor_total = {}
        for batch, run in zip(batches, runs):
            anomalies = db.get_anomalies(conn, run["id"])
            bid_key = f"{batch['id']}:{batch['name']}"
            sensor_counts = {}
            for a in anomalies:
                sensor = a["sensor_name"]
                sensor_counts[sensor] = sensor_counts.get(sensor, 0) + 1
                per_sensor_total[sensor] = per_sensor_total.get(sensor, 0) + 1
            per_batch[bid_key] = {
                "total": len(anomalies),
                "per_sensor": sensor_counts,
                "locked": bool(batch["locked"])
            }

        totals = [v["total"] for v in per_batch.values()]
        diff = max(totals) - min(totals) if len(totals) >= 2 else None

        return {
            "per_batch": per_batch,
            "total_anomalies_range": {
                "min": min(totals) if totals else 0,
                "max": max(totals) if totals else 0,
                "abs_diff": diff
            },
            "sensors_with_anomalies": sorted(per_sensor_total.keys())
        }

    def get_comparison_report(self, report_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.get_comparison_report(conn, report_id)
        finally:
            conn.close()

    def list_comparison_reports(self) -> List[Dict[str, Any]]:
        conn = self._conn()
        try:
            return db.list_comparison_reports(conn)
        finally:
            conn.close()

    def delete_comparison_report(self, report_id: int) -> None:
        conn = self._conn()
        try:
            r = db.get_comparison_report(conn, report_id)
            if not r:
                raise BatchServiceError(f"报告不存在: {report_id}")
            db.delete_comparison_report(conn, report_id)
        finally:
            conn.close()

    def export_comparison_report_json(self, report_id: int, output_path: str) -> str:
        """导出对比报告为 JSON"""
        report = self.get_comparison_report(report_id)
        if not report:
            raise BatchServiceError(f"报告不存在: {report_id}")
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report["report"], f, indent=2, ensure_ascii=False)
        return os.path.abspath(output_path)

    def export_comparison_report_csv(self, report_id: int, output_dir: str) -> Dict[str, str]:
        """
        导出对比报告为 CSV（拆分为多个文件）。
        返回 {文件名类型: 绝对路径}
        """
        report = self.get_comparison_report(report_id)
        if not report:
            raise BatchServiceError(f"报告不存在: {report_id}")
        os.makedirs(output_dir, exist_ok=True)
        paths = {}

        rpt = report["report"]
        scheme_name = rpt["scheme"]["name"] or "(无方案)"
        scheme_version = rpt["scheme"]["version"] or "N/A"
        base_prefix = f"report_{report_id}"

        summary_path = os.path.join(output_dir, f"{base_prefix}_summary.csv")
        with open(summary_path, "w", encoding="utf-8-sig") as f:
            f.write("report_id,report_name,scheme_name,scheme_version,generated_at,batch_count\n")
            f.write(f"{report_id},{rpt['name']},{scheme_name},{scheme_version},{rpt['generated_at']},{len(rpt['batch_summaries'])}\n")
        paths["summary"] = os.path.abspath(summary_path)

        batches_path = os.path.join(output_dir, f"{base_prefix}_batches.csv")
        with open(batches_path, "w", encoding="utf-8-sig") as f:
            headers = ["batch_id", "batch_name", "status", "locked", "source_file",
                       "config_version", "run_id", "run_number", "rows_processed",
                       "rows_errors", "anomalies_count", "metrics_count", "processed_at"]
            f.write(",".join(headers) + "\n")
            for bs in rpt["batch_summaries"]:
                row = [str(bs.get(h, "")) for h in headers]
                f.write(",".join(row) + "\n")
        paths["batches"] = os.path.abspath(batches_path)

        metrics_path = os.path.join(output_dir, f"{base_prefix}_metrics.csv")
        md = rpt["metrics_diff"]
        batch_keys = md["summary"]["batch_keys"]
        with open(metrics_path, "w", encoding="utf-8-sig") as f:
            headers = ["sensor", "metric"] + [f"value_{bk}" for bk in batch_keys] + ["abs_diff", "rel_diff_pct"]
            f.write(",".join(headers) + "\n")
            for key, info in md["per_metric"].items():
                row = [info["sensor"], info["metric"]]
                for bk in batch_keys:
                    v = info["values"].get(bk)
                    row.append("" if v is None else f"{v}")
                row.append("" if info["abs_diff"] is None else f"{info['abs_diff']}")
                row.append("" if info["rel_diff_pct"] is None else f"{info['rel_diff_pct']:.2f}")
                f.write(",".join(row) + "\n")
        paths["metrics"] = os.path.abspath(metrics_path)

        anomalies_path = os.path.join(output_dir, f"{base_prefix}_anomalies.csv")
        ad = rpt["anomalies_diff"]
        with open(anomalies_path, "w", encoding="utf-8-sig") as f:
            headers = ["batch_id", "batch_name", "locked", "total_anomalies"]
            sensors = ad["sensors_with_anomalies"]
            headers += [f"anomalies_{s}" for s in sensors]
            f.write(",".join(headers) + "\n")
            for bk, info in ad["per_batch"].items():
                bid, bname = bk.split(":", 1)
                row = [bid, bname, str(info["locked"]), str(info["total"])]
                for s in sensors:
                    row.append(str(info["per_sensor"].get(s, 0)))
                f.write(",".join(row) + "\n")
            f.write(f"\n# range_min,{ad['total_anomalies_range']['min']}\n")
            f.write(f"# range_max,{ad['total_anomalies_range']['max']}\n")
            f.write(f"# range_abs_diff,{ad['total_anomalies_range']['abs_diff']}\n")
        paths["anomalies"] = os.path.abspath(anomalies_path)

        return paths
