"""批次管理服务 - 状态流转、锁定保护、重跑机制、行级错误处理"""
import os
import json
import sqlite3
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

from . import database as db
from . import processor as proc
from .config import get_default_config, bump_config_version


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
