"""数据处理核心模块 - CSV导入、清洗、缺失值处理、指标计算、异常标记"""
import os
import json
import pandas as pd
import numpy as np
from typing import Any, Callable, Dict, List, Optional, Tuple


class DataProcessingError(Exception):
    """数据处理异常基类"""
    pass


class RowError:
    """行级错误记录"""
    def __init__(self, row_number: int, error_type: str, error_detail: str, raw_data: str = None):
        self.row_number = row_number
        self.error_type = error_type
        self.error_detail = error_detail
        self.raw_data = raw_data

    def to_dict(self) -> Dict[str, Any]:
        return {
            "row_number": self.row_number,
            "error_type": self.error_type,
            "error_detail": self.error_detail,
            "raw_data": self.raw_data
        }


def import_csv(file_path: str) -> Tuple[pd.DataFrame, List[RowError]]:
    """
    导入传感器 CSV 文件。
    预期列: timestamp, sensor_1, sensor_2, ...
    返回: (原始 DataFrame, 行级错误列表)
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV 文件不存在: {file_path}")

    row_errors: List[RowError] = []
    valid_rows: List[Dict[str, Any]] = []

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        raise DataProcessingError("CSV 文件为空")

    header_line = lines[0].strip()
    headers = [h.strip() for h in header_line.split(",")]

    if "timestamp" not in headers:
        raise DataProcessingError("CSV 缺少必需的 timestamp 列")

    sensor_columns = [h for h in headers if h != "timestamp"]
    if not sensor_columns:
        raise DataProcessingError("CSV 未包含任何传感器列")

    for idx, line in enumerate(lines[1:], start=2):
        line_stripped = line.strip()
        if not line_stripped:
            continue

        parts = line_stripped.split(",")
        if len(parts) != len(headers):
            row_errors.append(RowError(
                row_number=idx,
                error_type="column_mismatch",
                error_detail=f"列数不匹配: 期望 {len(headers)}, 实际 {len(parts)}",
                raw_data=line_stripped
            ))
            continue

        row_dict = dict(zip(headers, parts))
        raw_timestamp = str(row_dict.get("timestamp", "")).strip()

        if not raw_timestamp:
            row_errors.append(RowError(
                row_number=idx,
                error_type="missing_timestamp",
                error_detail="timestamp 缺失",
                raw_data=line_stripped
            ))
            continue

        parsed_timestamp = _parse_timestamp(raw_timestamp)
        if parsed_timestamp is None:
            row_errors.append(RowError(
                row_number=idx,
                error_type="invalid_timestamp",
                error_detail=f"无法解析 timestamp: '{raw_timestamp}'",
                raw_data=line_stripped
            ))
            continue

        valid_row: Dict[str, Any] = {"timestamp": parsed_timestamp}
        valid_row["_row_number"] = idx
        has_valid_sensor = False

        for sensor in sensor_columns:
            raw_val = str(row_dict.get(sensor, "")).strip()
            if not raw_val:
                valid_row[sensor] = np.nan
                continue

            parsed_val = _parse_numeric(raw_val)
            if parsed_val is None:
                row_errors.append(RowError(
                    row_number=idx,
                    error_type="non_numeric_value",
                    error_detail=f"传感器 {sensor} 的值不是数字: '{raw_val}'",
                    raw_data=line_stripped
                ))
                valid_row[sensor] = np.nan
            else:
                valid_row[sensor] = parsed_val
                has_valid_sensor = True

        if has_valid_sensor or parsed_timestamp is not None:
            valid_rows.append(valid_row)

    if not valid_rows:
        raise DataProcessingError("CSV 中没有有效数据行")

    df = pd.DataFrame(valid_rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df, row_errors


def _parse_timestamp(value: str) -> Optional[str]:
    """尝试解析多种时间戳格式"""
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            pd.Timestamp(value, format=fmt)
            return value
        except (ValueError, TypeError):
            continue
    try:
        pd.Timestamp(value)
        return value
    except (ValueError, TypeError):
        return None


def _parse_numeric(value: str) -> Optional[float]:
    """解析数值，失败返回 None"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def clean_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """数据清洗"""
    cleaning_cfg = config.get("cleaning", {})
    df = df.copy()

    if cleaning_cfg.get("remove_duplicates", True):
        df = df.drop_duplicates(subset=["timestamp"], keep="first")
        df = df.reset_index(drop=True)

    return df


def handle_missing_values(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """缺失值处理"""
    missing_cfg = config.get("missing_values", {})
    strategy = missing_cfg.get("strategy", "interpolate")
    fill_value = missing_cfg.get("numeric_fill_value", 0)
    max_gap = missing_cfg.get("max_interpolate_gap", 5)

    df = df.copy()
    sensor_cols = [c for c in df.columns if c not in ("timestamp", "_row_number")]

    for col in sensor_cols:
        if strategy == "interpolate":
            df[col] = df[col].interpolate(method="linear", limit=max_gap, limit_direction="both")
            df[col] = df[col].fillna(fill_value)
        elif strategy == "fill":
            df[col] = df[col].fillna(fill_value)
        elif strategy == "drop":
            df = df.dropna(subset=[col])
            df = df.reset_index(drop=True)
        elif strategy == "ffill":
            df[col] = df[col].ffill().bfill()
            df[col] = df[col].fillna(fill_value)

    return df


def compute_metrics(df: pd.DataFrame, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """计算指标，返回 [{sensor_name, metric_name, metric_value}, ...]"""
    metrics_cfg = config.get("metrics", {})
    sensor_cols = [c for c in df.columns if c not in ("timestamp", "_row_number")]
    result: List[Dict[str, Any]] = []

    for col in sensor_cols:
        series = df[col].dropna()
        if len(series) == 0:
            continue

        if metrics_cfg.get("compute_mean", True):
            result.append({"sensor_name": col, "metric_name": "mean", "metric_value": float(series.mean())})
        if metrics_cfg.get("compute_std", True):
            result.append({"sensor_name": col, "metric_name": "std", "metric_value": float(series.std()) if len(series) > 1 else 0.0})
        if metrics_cfg.get("compute_min", True):
            result.append({"sensor_name": col, "metric_name": "min", "metric_value": float(series.min())})
        if metrics_cfg.get("compute_max", True):
            result.append({"sensor_name": col, "metric_name": "max", "metric_value": float(series.max())})
        if metrics_cfg.get("compute_median", True):
            result.append({"sensor_name": col, "metric_name": "median", "metric_value": float(series.median())})
        if metrics_cfg.get("compute_range", True):
            result.append({"sensor_name": col, "metric_name": "range", "metric_value": float(series.max() - series.min())})
        result.append({"sensor_name": col, "metric_name": "count", "metric_value": float(len(series))})

    return result


def detect_anomalies(df: pd.DataFrame, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """异常检测，返回异常点列表"""
    anomaly_cfg = config.get("anomaly_detection", {})
    method = anomaly_cfg.get("method", "zscore")
    zscore_threshold = anomaly_cfg.get("zscore_threshold", 3.0)
    iqr_multiplier = anomaly_cfg.get("iqr_multiplier", 1.5)
    min_values = anomaly_cfg.get("min_values", 5)

    sensor_cols = [c for c in df.columns if c not in ("timestamp", "_row_number")]
    anomalies: List[Dict[str, Any]] = []

    for col in sensor_cols:
        series = df[col].dropna()
        if len(series) < min_values:
            continue

        if method == "zscore":
            mean = series.mean()
            std = series.std()
            if std == 0 or np.isnan(std):
                continue
            for idx in df.index:
                val = df.at[idx, col]
                if pd.isna(val):
                    continue
                z = abs((val - mean) / std)
                if z > zscore_threshold:
                    anomalies.append({
                        "sensor_name": col,
                        "row_number": int(df.at[idx, "_row_number"]) if "_row_number" in df.columns else int(idx) + 2,
                        "timestamp": str(df.at[idx, "timestamp"]),
                        "value": float(val),
                        "anomaly_type": f"zscore (z={z:.2f})"
                    })
        elif method == "iqr":
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - iqr_multiplier * iqr
            upper = q3 + iqr_multiplier * iqr
            for idx in df.index:
                val = df.at[idx, col]
                if pd.isna(val):
                    continue
                if val < lower or val > upper:
                    which = "below_lower" if val < lower else "above_upper"
                    anomalies.append({
                        "sensor_name": col,
                        "row_number": int(df.at[idx, "_row_number"]) if "_row_number" in df.columns else int(idx) + 2,
                        "timestamp": str(df.at[idx, "timestamp"]),
                        "value": float(val),
                        "anomaly_type": f"iqr ({which}: [{lower:.2f}, {upper:.2f}])"
                    })

    return anomalies
