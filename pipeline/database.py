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

            CREATE INDEX IF NOT EXISTS idx_runs_batch_id ON runs(batch_id);
            CREATE INDEX IF NOT EXISTS idx_row_errors_run_id ON row_errors(run_id);
            CREATE INDEX IF NOT EXISTS idx_metrics_run_id ON metrics(run_id);
            CREATE INDEX IF NOT EXISTS idx_anomalies_run_id ON anomalies(run_id);
            CREATE INDEX IF NOT EXISTS idx_exports_batch_id ON exports(batch_id);
        """)
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


def update_batch_config(conn: sqlite3.Connection, batch_id: int, config: Dict[str, Any]) -> None:
    """更新批次配置"""
    now = datetime.now().isoformat()
    config_json = json.dumps(config, ensure_ascii=False)
    config_version = config.get("version", 1)
    conn.execute("""
        UPDATE batches SET config_version = ?, config_json = ?, updated_at = ? WHERE id = ?
    """, (config_version, config_json, now, batch_id))
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
