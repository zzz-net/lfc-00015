"""数据库持久化层 - SQLite"""
import sqlite3
import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pipeline.db")


def get_connection(db_path: str = None) -> sqlite3.Connection:
    """获取数据库连接"""
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str = None) -> None:
    """初始化数据库表结构"""
    conn = get_connection(db_path)
    try:
        cursor = conn.cursor()
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                source_file TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                locked INTEGER NOT NULL DEFAULT 0,
                config_version INTEGER NOT NULL DEFAULT 1,
                config_json TEXT NOT NULL,
                current_scheme_id INTEGER,
                current_scheme_name TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                run_number INTEGER NOT NULL,
                config_version INTEGER NOT NULL,
                config_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                rows_processed INTEGER DEFAULT 0,
                rows_errors INTEGER DEFAULT 0,
                error_message TEXT,
                FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS row_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                row_number INTEGER NOT NULL,
                error_type TEXT NOT NULL,
                error_detail TEXT NOT NULL,
                raw_data TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                sensor_name TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL,
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                sensor_name TEXT NOT NULL,
                row_number INTEGER NOT NULL,
                timestamp TEXT,
                value REAL,
                anomaly_type TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS exports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                run_id INTEGER NOT NULL,
                export_path TEXT NOT NULL,
                exported_at TEXT NOT NULL,
                export_type TEXT NOT NULL,
                FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS analysis_schemes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                scheme_version TEXT NOT NULL,
                config_json TEXT NOT NULL,
                source_scheme_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (source_scheme_id) REFERENCES analysis_schemes(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS batch_scheme_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                scheme_id INTEGER,
                scheme_name TEXT,
                source_scheme_id INTEGER,
                config_version INTEGER NOT NULL,
                config_json TEXT NOT NULL,
                action TEXT NOT NULL,
                rolled_back_from_id INTEGER,
                applied_at TEXT NOT NULL,
                FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
                FOREIGN KEY (scheme_id) REFERENCES analysis_schemes(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS comparison_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                scheme_id INTEGER,
                scheme_name TEXT,
                scheme_version TEXT,
                batch_ids_json TEXT NOT NULL,
                batch_summaries_json TEXT NOT NULL,
                metrics_diff_json TEXT NOT NULL,
                anomalies_diff_json TEXT NOT NULL,
                report_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scheme_id) REFERENCES analysis_schemes(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_runs_batch_id ON runs(batch_id);
            CREATE INDEX IF NOT EXISTS idx_row_errors_run_id ON row_errors(run_id);
            CREATE INDEX IF NOT EXISTS idx_metrics_run_id ON metrics(run_id);
            CREATE INDEX IF NOT EXISTS idx_anomalies_run_id ON anomalies(run_id);
            CREATE INDEX IF NOT EXISTS idx_exports_batch_id ON exports(batch_id);
            CREATE INDEX IF NOT EXISTS idx_analysis_schemes_name ON analysis_schemes(name);
            CREATE INDEX IF NOT EXISTS idx_batch_scheme_history_batch_id ON batch_scheme_history(batch_id);
            CREATE INDEX IF NOT EXISTS idx_comparison_reports_scheme_id ON comparison_reports(scheme_id);

            CREATE TABLE IF NOT EXISTS scheme_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                scheme_id INTEGER,
                scheme_name TEXT,
                source_scheme_id INTEGER,
                action TEXT NOT NULL,
                trigger_method TEXT NOT NULL,
                previous_config_json TEXT,
                new_config_json TEXT,
                config_diff_json TEXT,
                result TEXT NOT NULL,
                error_message TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
                FOREIGN KEY (scheme_id) REFERENCES analysis_schemes(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_scheme_audit_log_batch_id ON scheme_audit_log(batch_id);
            CREATE INDEX IF NOT EXISTS idx_scheme_audit_log_scheme_id ON scheme_audit_log(scheme_id);
            CREATE INDEX IF NOT EXISTS idx_scheme_audit_log_action ON scheme_audit_log(action);
            CREATE INDEX IF NOT EXISTS idx_scheme_audit_log_result ON scheme_audit_log(result);
        """)
        conn.commit()

        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(analysis_schemes)")
        columns = [col[1] for col in cursor.fetchall()]
        if "source_scheme_id" not in columns:
            cursor.execute(
                "ALTER TABLE analysis_schemes ADD COLUMN source_scheme_id INTEGER "
                "REFERENCES analysis_schemes(id) ON DELETE SET NULL"
            )
            conn.commit()
        if "original_id" not in columns:
            cursor.execute(
                "ALTER TABLE analysis_schemes ADD COLUMN original_id INTEGER"
            )
            conn.commit()
        if "imported_from" not in columns:
            cursor.execute(
                "ALTER TABLE analysis_schemes ADD COLUMN imported_from TEXT"
            )
            conn.commit()

        cursor.execute("PRAGMA table_info(batches)")
        batch_columns = [col[1] for col in cursor.fetchall()]
        if "current_scheme_id" not in batch_columns:
            cursor.execute("ALTER TABLE batches ADD COLUMN current_scheme_id INTEGER")
            conn.commit()
        if "current_scheme_name" not in batch_columns:
            cursor.execute("ALTER TABLE batches ADD COLUMN current_scheme_name TEXT")
            conn.commit()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS batch_scheme_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                scheme_id INTEGER,
                scheme_name TEXT,
                source_scheme_id INTEGER,
                config_version INTEGER NOT NULL,
                config_json TEXT NOT NULL,
                action TEXT NOT NULL,
                rolled_back_from_id INTEGER,
                applied_at TEXT NOT NULL,
                FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
                FOREIGN KEY (scheme_id) REFERENCES analysis_schemes(id) ON DELETE SET NULL
            );
        """)
        conn.commit()

        cursor.execute("PRAGMA index_list(batch_scheme_history)")
        has_index = any("idx_batch_scheme_history_batch_id" in row[1] for row in cursor.fetchall())
        if not has_index:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_batch_scheme_history_batch_id ON batch_scheme_history(batch_id)")
            conn.commit()
    finally:
        conn.close()


# ============ Batch Operations ============

def create_batch(conn: sqlite3.Connection, name: str, source_file: str, config: Dict[str, Any]) -> int:
    """创建新批次，返回批次 ID"""
    now = datetime.now().isoformat()
    config_json = json.dumps(config, ensure_ascii=False)
    config_version = config.get("version", 1)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO batches (name, source_file, status, locked, config_version, config_json, created_at, updated_at)
        VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)
    """, (name, source_file, config_version, config_json, now, now))
    conn.commit()
    return cursor.lastrowid


def get_batch(conn: sqlite3.Connection, batch_id: int) -> Optional[Dict[str, Any]]:
    """获取批次信息"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM batches WHERE id = ?", (batch_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_batch_by_name(conn: sqlite3.Connection, name: str) -> Optional[Dict[str, Any]]:
    """按名称获取批次"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM batches WHERE name = ?", (name,))
    row = cursor.fetchone()
    return dict(row) if row else None


def list_batches(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """列出所有批次"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM batches ORDER BY updated_at DESC")
    return [dict(row) for row in cursor.fetchall()]


def update_batch_status(conn: sqlite3.Connection, batch_id: int, status: str, error_message: str = None) -> None:
    """更新批次状态"""
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE batches SET status = ?, error_message = ?, updated_at = ? WHERE id = ?
    """, (status, error_message, now, batch_id))
    conn.commit()


def update_batch_config(conn: sqlite3.Connection, batch_id: int, config: Dict[str, Any],
                        scheme_id: int = None, scheme_name: str = None) -> None:
    """更新批次配置，可同时更新当前方案信息"""
    now = datetime.now().isoformat()
    config_json = json.dumps(config, ensure_ascii=False)
    config_version = config.get("version", 1)
    if scheme_id is not None or scheme_name is not None:
        conn.execute("""
            UPDATE batches SET config_version = ?, config_json = ?, current_scheme_id = ?,
                               current_scheme_name = ?, updated_at = ? WHERE id = ?
        """, (config_version, config_json, scheme_id, scheme_name, now, batch_id))
    else:
        conn.execute("""
            UPDATE batches SET config_version = ?, config_json = ?, updated_at = ? WHERE id = ?
        """, (config_version, config_json, now, batch_id))
    conn.commit()


def update_batch_scheme_info(conn: sqlite3.Connection, batch_id: int,
                             scheme_id: int = None, scheme_name: str = None) -> None:
    """仅更新批次的当前方案信息，不修改配置"""
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE batches SET current_scheme_id = ?, current_scheme_name = ?, updated_at = ? WHERE id = ?
    """, (scheme_id, scheme_name, now, batch_id))
    conn.commit()


def set_batch_locked(conn: sqlite3.Connection, batch_id: int, locked: bool) -> None:
    """设置批次锁定状态"""
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE batches SET locked = ?, updated_at = ? WHERE id = ?
    """, (1 if locked else 0, now, batch_id))
    conn.commit()


def is_batch_locked(conn: sqlite3.Connection, batch_id: int) -> bool:
    """检查批次是否锁定"""
    batch = get_batch(conn, batch_id)
    return bool(batch and batch["locked"])


# ============ Run Operations ============

def create_run(conn: sqlite3.Connection, batch_id: int, config: Dict[str, Any]) -> Tuple[int, int]:
    """创建新运行记录，返回 (run_id, run_number)"""
    now = datetime.now().isoformat()
    config_json = json.dumps(config, ensure_ascii=False)
    config_version = config.get("version", 1)

    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(run_number), 0) + 1 FROM runs WHERE batch_id = ?", (batch_id,))
    run_number = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO runs (batch_id, run_number, config_version, config_json, started_at, status)
        VALUES (?, ?, ?, ?, ?, 'running')
    """, (batch_id, run_number, config_version, config_json, now))
    conn.commit()
    return cursor.lastrowid, run_number


def finish_run(conn: sqlite3.Connection, run_id: int, status: str, rows_processed: int = 0,
               rows_errors: int = 0, error_message: str = None) -> None:
    """完成运行记录"""
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE runs SET finished_at = ?, status = ?, rows_processed = ?, rows_errors = ?, error_message = ?
        WHERE id = ?
    """, (now, status, rows_processed, rows_errors, error_message, run_id))
    conn.commit()


def get_run(conn: sqlite3.Connection, run_id: int) -> Optional[Dict[str, Any]]:
    """获取运行记录"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM runs WHERE id = ?", (run_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_latest_run(conn: sqlite3.Connection, batch_id: int) -> Optional[Dict[str, Any]]:
    """获取批次最新的运行记录"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM runs WHERE batch_id = ? ORDER BY id DESC LIMIT 1", (batch_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def list_runs(conn: sqlite3.Connection, batch_id: int) -> List[Dict[str, Any]]:
    """列出批次的所有运行记录"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM runs WHERE batch_id = ? ORDER BY id DESC", (batch_id,))
    return [dict(row) for row in cursor.fetchall()]


# ============ Row Errors Operations ============

def add_row_error(conn: sqlite3.Connection, run_id: int, row_number: int, error_type: str,
                  error_detail: str, raw_data: str = None) -> None:
    """添加行级错误"""
    conn.execute("""
        INSERT INTO row_errors (run_id, row_number, error_type, error_detail, raw_data)
        VALUES (?, ?, ?, ?, ?)
    """, (run_id, row_number, error_type, error_detail, raw_data))


def get_row_errors(conn: sqlite3.Connection, run_id: int) -> List[Dict[str, Any]]:
    """获取运行的所有行级错误"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM row_errors WHERE run_id = ? ORDER BY row_number", (run_id,))
    return [dict(row) for row in cursor.fetchall()]


# ============ Metrics Operations ============

def add_metric(conn: sqlite3.Connection, run_id: int, sensor_name: str, metric_name: str,
               metric_value: float) -> None:
    """添加指标"""
    conn.execute("""
        INSERT INTO metrics (run_id, sensor_name, metric_name, metric_value)
        VALUES (?, ?, ?, ?)
    """, (run_id, sensor_name, metric_name, metric_value))


def get_metrics(conn: sqlite3.Connection, run_id: int) -> List[Dict[str, Any]]:
    """获取运行的所有指标"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM metrics WHERE run_id = ? ORDER BY sensor_name, metric_name", (run_id,))
    return [dict(row) for row in cursor.fetchall()]


# ============ Anomaly Operations ============

def add_anomaly(conn: sqlite3.Connection, run_id: int, sensor_name: str, row_number: int,
                timestamp: str, value: float, anomaly_type: str) -> None:
    """添加异常记录"""
    conn.execute("""
        INSERT INTO anomalies (run_id, sensor_name, row_number, timestamp, value, anomaly_type)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (run_id, sensor_name, row_number, timestamp, value, anomaly_type))


def get_anomalies(conn: sqlite3.Connection, run_id: int) -> List[Dict[str, Any]]:
    """获取运行的所有异常记录"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM anomalies WHERE run_id = ? ORDER BY sensor_name, row_number", (run_id,))
    return [dict(row) for row in cursor.fetchall()]


# ============ Export Operations ============

def add_export(conn: sqlite3.Connection, batch_id: int, run_id: int, export_path: str,
               export_type: str) -> int:
    """添加导出记录，返回导出 ID"""
    now = datetime.now().isoformat()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO exports (batch_id, run_id, export_path, exported_at, export_type)
        VALUES (?, ?, ?, ?, ?)
    """, (batch_id, run_id, export_path, now, export_type))
    conn.commit()
    return cursor.lastrowid


def list_exports(conn: sqlite3.Connection, batch_id: int) -> List[Dict[str, Any]]:
    """列出批次的所有导出记录"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM exports WHERE batch_id = ? ORDER BY id DESC", (batch_id,))
    return [dict(row) for row in cursor.fetchall()]


# ============ Analysis Scheme Operations ============

SCHEME_VERSION = "1.0"
REQUIRED_SCHEME_FIELDS = {"cleaning", "missing_values", "anomaly_detection", "metrics"}


def create_scheme(conn: sqlite3.Connection, name: str, config: Dict[str, Any],
                  description: str = None, source_scheme_id: int = None,
                  original_id: int = None, imported_from: str = None) -> int:
    """创建新分析方案，返回方案 ID"""
    now = datetime.now().isoformat()
    config_json = json.dumps(config, ensure_ascii=False)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO analysis_schemes (name, description, scheme_version, config_json, source_scheme_id, original_id, imported_from, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, description, SCHEME_VERSION, config_json, source_scheme_id, original_id, imported_from, now, now))
    conn.commit()
    return cursor.lastrowid


def update_scheme(conn: sqlite3.Connection, scheme_id: int, config: Dict[str, Any],
                  description: str = None) -> None:
    """更新已有分析方案（覆盖）"""
    now = datetime.now().isoformat()
    config_json = json.dumps(config, ensure_ascii=False)
    cursor = conn.cursor()
    if description is not None:
        cursor.execute("""
            UPDATE analysis_schemes SET config_json = ?, description = ?, updated_at = ? WHERE id = ?
        """, (config_json, description, now, scheme_id))
    else:
        cursor.execute("""
            UPDATE analysis_schemes SET config_json = ?, updated_at = ? WHERE id = ?
        """, (config_json, now, scheme_id))
    conn.commit()


def get_scheme(conn: sqlite3.Connection, scheme_id: int) -> Optional[Dict[str, Any]]:
    """按 ID 获取方案"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM analysis_schemes WHERE id = ?", (scheme_id,))
    row = cursor.fetchone()
    if not row:
        return None
    result = dict(row)
    result["config"] = json.loads(result["config_json"])
    del result["config_json"]
    return result


def get_scheme_by_name(conn: sqlite3.Connection, name: str) -> Optional[Dict[str, Any]]:
    """按名称获取方案"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM analysis_schemes WHERE name = ?", (name,))
    row = cursor.fetchone()
    if not row:
        return None
    result = dict(row)
    result["config"] = json.loads(result["config_json"])
    del result["config_json"]
    return result


def list_schemes(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """列出所有分析方案（不含 config 详情，节省内存）"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, description, scheme_version, source_scheme_id, original_id, imported_from, created_at, updated_at FROM analysis_schemes ORDER BY updated_at DESC")
    return [dict(row) for row in cursor.fetchall()]


def get_scheme_by_original_id(conn: sqlite3.Connection, original_id: int) -> Optional[Dict[str, Any]]:
    """按原始 ID（导入前的原始 ID）查找方案，用于导入后历史追溯"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM analysis_schemes WHERE original_id = ? ORDER BY id DESC LIMIT 1", (original_id,))
    row = cursor.fetchone()
    if not row:
        return None
    result = dict(row)
    result["config"] = json.loads(result["config_json"])
    del result["config_json"]
    return result


def delete_scheme(conn: sqlite3.Connection, scheme_id: int) -> None:
    """删除方案"""
    conn.execute("DELETE FROM analysis_schemes WHERE id = ?", (scheme_id,))
    conn.commit()


def validate_scheme_config(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """校验方案配置是否包含必填字段，返回 (是否合法, 缺失字段列表)"""
    missing = []
    for field in REQUIRED_SCHEME_FIELDS:
        if field not in config:
            missing.append(field)
    return (len(missing) == 0), missing


def check_scheme_version_compatibility(scheme_version: str) -> Tuple[bool, str]:
    """校验导入方案的版本兼容性"""
    if scheme_version == SCHEME_VERSION:
        return True, ""
    try:
        major = scheme_version.split(".")[0]
        current_major = SCHEME_VERSION.split(".")[0]
        if major == current_major:
            return True, f"版本兼容 (导入 {scheme_version}, 当前 {SCHEME_VERSION})"
        else:
            return False, f"主版本不兼容: 导入 {scheme_version}, 当前 {SCHEME_VERSION}"
    except Exception:
        return False, f"无法解析版本号: {scheme_version}"


# ============ Comparison Report Operations ============

def create_comparison_report(conn: sqlite3.Connection, name: str, scheme_id: Optional[int],
                             scheme_name: Optional[str], scheme_version: Optional[str],
                             batch_ids: List[int], batch_summaries: List[Dict[str, Any]],
                             metrics_diff: Dict[str, Any], anomalies_diff: Dict[str, Any],
                             full_report: Dict[str, Any]) -> int:
    """创建对比报告记录，返回报告 ID"""
    now = datetime.now().isoformat()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO comparison_reports (
            name, scheme_id, scheme_name, scheme_version,
            batch_ids_json, batch_summaries_json,
            metrics_diff_json, anomalies_diff_json, report_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name, scheme_id, scheme_name, scheme_version,
        json.dumps(batch_ids, ensure_ascii=False),
        json.dumps(batch_summaries, ensure_ascii=False),
        json.dumps(metrics_diff, ensure_ascii=False),
        json.dumps(anomalies_diff, ensure_ascii=False),
        json.dumps(full_report, ensure_ascii=False),
        now
    ))
    conn.commit()
    return cursor.lastrowid


def get_comparison_report(conn: sqlite3.Connection, report_id: int) -> Optional[Dict[str, Any]]:
    """获取对比报告"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM comparison_reports WHERE id = ?", (report_id,))
    row = cursor.fetchone()
    if not row:
        return None
    result = dict(row)
    result["batch_ids"] = json.loads(result["batch_ids_json"])
    result["batch_summaries"] = json.loads(result["batch_summaries_json"])
    result["metrics_diff"] = json.loads(result["metrics_diff_json"])
    result["anomalies_diff"] = json.loads(result["anomalies_diff_json"])
    result["report"] = json.loads(result["report_json"])
    for k in ("batch_ids_json", "batch_summaries_json", "metrics_diff_json", "anomalies_diff_json", "report_json"):
        del result[k]
    return result


def list_comparison_reports(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """列出所有对比报告（不含详细 diff，节省内存）"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, scheme_id, scheme_name, scheme_version, batch_ids_json, created_at
        FROM comparison_reports ORDER BY created_at DESC
    """)
    results = []
    for row in cursor.fetchall():
        r = dict(row)
        r["batch_ids"] = json.loads(r["batch_ids_json"])
        del r["batch_ids_json"]
        results.append(r)
    return results


def delete_comparison_report(conn: sqlite3.Connection, report_id: int) -> None:
    """删除对比报告"""
    conn.execute("DELETE FROM comparison_reports WHERE id = ?", (report_id,))
    conn.commit()


# ============ Batch Scheme History Operations ============

SCHEME_HISTORY_ACTION_APPLY = "apply"
SCHEME_HISTORY_ACTION_ROLLBACK = "rollback"
SCHEME_HISTORY_ACTION_DIRECT = "direct"


def add_scheme_history(conn: sqlite3.Connection, batch_id: int, config: Dict[str, Any],
                       action: str, scheme_id: int = None, scheme_name: str = None,
                       source_scheme_id: int = None, rolled_back_from_id: int = None) -> int:
    """添加批次方案历史记录，返回历史记录 ID"""
    now = datetime.now().isoformat()
    config_json = json.dumps(config, ensure_ascii=False)
    config_version = config.get("version", 1)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO batch_scheme_history (
            batch_id, scheme_id, scheme_name, source_scheme_id,
            config_version, config_json, action, rolled_back_from_id, applied_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (batch_id, scheme_id, scheme_name, source_scheme_id,
          config_version, config_json, action, rolled_back_from_id, now))
    conn.commit()
    return cursor.lastrowid


def get_scheme_history(conn: sqlite3.Connection, batch_id: int) -> List[Dict[str, Any]]:
    """获取批次的方案应用历史，按时间倒序排列"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM batch_scheme_history
        WHERE batch_id = ? ORDER BY id DESC
    """, (batch_id,))
    results = []
    for row in cursor.fetchall():
        r = dict(row)
        r["config"] = json.loads(r["config_json"])
        del r["config_json"]
        results.append(r)
    return results


def get_latest_scheme_history(conn: sqlite3.Connection, batch_id: int) -> Optional[Dict[str, Any]]:
    """获取批次最新的方案历史记录"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM batch_scheme_history
        WHERE batch_id = ? ORDER BY id DESC LIMIT 1
    """, (batch_id,))
    row = cursor.fetchone()
    if not row:
        return None
    r = dict(row)
    r["config"] = json.loads(r["config_json"])
    del r["config_json"]
    return r


def get_previous_scheme_history(conn: sqlite3.Connection, batch_id: int,
                                current_history_id: int) -> Optional[Dict[str, Any]]:
    """获取批次当前历史记录的上一条（用于回滚）"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM batch_scheme_history
        WHERE batch_id = ? AND id < ? ORDER BY id DESC LIMIT 1
    """, (batch_id, current_history_id))
    row = cursor.fetchone()
    if not row:
        return None
    r = dict(row)
    r["config"] = json.loads(r["config_json"])
    del r["config_json"]
    return r


def get_scheme_history_by_id(conn: sqlite3.Connection, history_id: int) -> Optional[Dict[str, Any]]:
    """按 ID 获取方案历史记录"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM batch_scheme_history WHERE id = ?
    """, (history_id,))
    row = cursor.fetchone()
    if not row:
        return None
    r = dict(row)
    r["config"] = json.loads(r["config_json"])
    del r["config_json"]
    return r


# ============ Scheme Audit Log Operations ============

AUDIT_ACTION_APPLY = "apply"
AUDIT_ACTION_CLONE_APPLY = "clone_apply"
AUDIT_ACTION_DERIVE_APPLY = "derive_apply"
AUDIT_ACTION_ROLLBACK = "rollback"
AUDIT_ACTION_DIRECT_MODIFY = "direct_modify"
AUDIT_ACTION_DRY_RUN = "dry_run"

AUDIT_RESULT_SUCCESS = "success"
AUDIT_RESULT_FAILED = "failed"
AUDIT_RESULT_BLOCKED = "blocked"

AUDIT_TRIGGER_CLI = "cli"
AUDIT_TRIGGER_API = "api"
AUDIT_TRIGGER_IMPORT = "import"


def add_scheme_audit_log(
    conn: sqlite3.Connection,
    batch_id: int,
    action: str,
    trigger_method: str,
    result: str,
    scheme_id: int = None,
    scheme_name: str = None,
    source_scheme_id: int = None,
    previous_config: Dict[str, Any] = None,
    new_config: Dict[str, Any] = None,
    config_diff: Dict[str, Any] = None,
    error_message: str = None
) -> int:
    """添加方案审计日志，返回日志 ID"""
    now = datetime.now().isoformat()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM batches WHERE id = ?", (batch_id,))
    if not cursor.fetchone():
        return 0

    if scheme_id is not None:
        cursor.execute("SELECT id FROM analysis_schemes WHERE id = ?", (scheme_id,))
        if not cursor.fetchone():
            scheme_id = None

    if source_scheme_id is not None:
        cursor.execute("SELECT id FROM analysis_schemes WHERE id = ?", (source_scheme_id,))
        if not cursor.fetchone():
            source_scheme_id = None

    cursor.execute("""
        INSERT INTO scheme_audit_log (
            batch_id, scheme_id, scheme_name, source_scheme_id,
            action, trigger_method,
            previous_config_json, new_config_json, config_diff_json,
            result, error_message, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        batch_id, scheme_id, scheme_name, source_scheme_id,
        action, trigger_method,
        json.dumps(previous_config, ensure_ascii=False) if previous_config else None,
        json.dumps(new_config, ensure_ascii=False) if new_config else None,
        json.dumps(config_diff, ensure_ascii=False) if config_diff else None,
        result, error_message, now
    ))
    conn.commit()
    return cursor.lastrowid


def get_scheme_audit_logs(conn: sqlite3.Connection, batch_id: int = None, scheme_id: int = None,
                          action: str = None, result: str = None,
                          limit: int = 100) -> List[Dict[str, Any]]:
    """查询方案审计日志，支持按批次、方案、操作类型、结果筛选"""
    cursor = conn.cursor()
    query = "SELECT * FROM scheme_audit_log WHERE 1=1"
    params = []
    if batch_id is not None:
        query += " AND batch_id = ?"
        params.append(batch_id)
    if scheme_id is not None:
        query += " AND scheme_id = ?"
        params.append(scheme_id)
    if action is not None:
        query += " AND action = ?"
        params.append(action)
    if result is not None:
        query += " AND result = ?"
        params.append(result)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    cursor.execute(query, params)
    results = []
    for row in cursor.fetchall():
        r = dict(row)
        if r.get("previous_config_json"):
            r["previous_config"] = json.loads(r["previous_config_json"])
            del r["previous_config_json"]
        if r.get("new_config_json"):
            r["new_config"] = json.loads(r["new_config_json"])
            del r["new_config_json"]
        if r.get("config_diff_json"):
            r["config_diff"] = json.loads(r["config_diff_json"])
            del r["config_diff_json"]
        results.append(r)
    return results


def compute_config_diff(previous_config: Dict[str, Any], new_config: Dict[str, Any]) -> Dict[str, Any]:
    """计算两个配置的差异，返回新增、修改、删除的字段"""
    diff = {
        "added": {},
        "modified": {},
        "removed": {},
        "version_change": None
    }

    def _flatten(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        flat = {}
        for k, v in d.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                flat.update(_flatten(v, key))
            else:
                flat[key] = v
        return flat

    prev_flat = _flatten(previous_config or {})
    new_flat = _flatten(new_config or {})

    all_keys = set(prev_flat.keys()) | set(new_flat.keys())
    for key in all_keys:
        if key not in prev_flat:
            diff["added"][key] = new_flat[key]
        elif key not in new_flat:
            diff["removed"][key] = prev_flat[key]
        elif prev_flat[key] != new_flat[key]:
            diff["modified"][key] = {
                "old": prev_flat[key],
                "new": new_flat[key]
            }

    if "version" in prev_flat and "version" in new_flat:
        diff["version_change"] = {
            "old": prev_flat["version"],
            "new": new_flat["version"]
        }

    return diff
