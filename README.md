# 实验数据处理流水线

用于处理传感器 CSV 数据的实验批次管理流水线，支持配置版本化、批次锁定、重跑保护、分析方案管理和多批次对比分析。

## 快速开始

```bash
pip install -r requirements.txt

# 创建批次并处理
python -m pipeline create exp_001 samples/sensor_data.csv
python -m pipeline process 1

# 保存当前批次配置为分析方案
python -m pipeline scheme save my_scheme --batch-id 1 --description "默认配置方案"

# 再创建一个批次用于对比
python -m pipeline create exp_002 samples/sensor_data.csv
python -m pipeline set-threshold 2 --zscore 1.5
python -m pipeline process 2

# 生成对比报告
python -m pipeline compare run compare_001 1 2 --scheme-id 1

# 导出报告
python -m pipeline compare export 1 -o exports/report_1.json --format json
python -m pipeline compare export 1 -o exports/ --format csv
```

## 核心概念

### 批次 (Batch)
一个独立的数据处理单元，关联一个 CSV 源文件和一套独立的配置。批次可以被"锁定"以防止历史结果被意外覆盖。

- **pending**: 待处理
- **processed**: 已处理（可重跑）
- **locked**: 已锁定（禁止重跑和修改配置）
- **failed**: 处理失败

### 运行 (Run)
批次的一次具体执行记录。未锁定批次每次 `process` 都会创建新的 Run，配置版本和历史结果完整保留。

### 分析方案 (Analysis Scheme)
将清洗阈值、缺失值策略、异常检测规则等配置保存为命名方案，用于跨批次复用和对比分析追溯。

方案内容包含：
- `cleaning`: 数据清洗规则（去重等）
- `missing_values`: 缺失值处理策略（interpolate/fill/drop/ffill）
- `metrics`: 指标计算开关
- `anomaly_detection`: 异常检测方法和参数（zscore/iqr）

### 对比报告 (Comparison Report)
对多个已处理批次的关键指标、异常数量、配置版本和数据来源进行汇总对比。报告与参与批次解耦，锁定批次可以安全参与对比而不会被修改。

## CLI 命令

### 基础命令

```
python -m pipeline --help
```

| 命令 | 说明 |
|------|------|
| `create NAME CSV_PATH` | 创建新批次 |
| `list` | 列出所有批次 |
| `show BATCH_ID` | 显示批次详情 |
| `process BATCH_ID` | 处理批次 / 重跑（锁定批次拒绝执行） |
| `lock BATCH_ID` | 锁定批次 |
| `unlock BATCH_ID` | 解锁批次 |
| `set-threshold BATCH_ID [--zscore N] [--iqr N]` | 修改异常检测阈值 |
| `history BATCH_ID` | 查看运行历史 |
| `run-show RUN_ID [--metrics] [--errors] [--anomalies]` | 查看运行详情 |
| `export BATCH_ID -o OUTPUT_DIR [--run-id N] [--metrics] [--errors] [--anomalies]` | 导出数据 |
| `exports BATCH_ID` | 查看导出历史 |

### 分析方案管理

```
python -m pipeline scheme --help
```

| 命令 | 说明 |
|------|------|
| `scheme save NAME [--batch-id N] [--config FILE] [--description TEXT]` | 保存方案，从批次或配置文件读取 |
| `scheme list` | 列出所有方案 |
| `scheme show SCHEME_ID` | 显示方案详情 |
| `scheme apply SCHEME_ID BATCH_ID` | 将方案应用到未锁定批次（不自动重跑） |
| `scheme clone SOURCE_SCHEME_ID NEW_NAME [--description TEXT]` | 基于已有方案克隆出新方案，可改名称和描述 |
| `scheme clone-apply SOURCE_SCHEME_ID NEW_NAME BATCH_ID [--description TEXT]` | 克隆方案并立即应用到未锁定批次（不自动重跑） |
| `scheme export SCHEME_ID -o FILE.json` | 导出方案为 JSON 文件 |
| `scheme import FILE.json [--on-conflict ask\|overwrite\|rename\|skip] [--new-name NAME]` | 从文件导入方案 |
| `scheme delete SCHEME_ID` | 删除方案 |

**导入冲突处理策略：**
- `ask`（默认）: 检测到冲突时报错，提示用户选择
- `overwrite`: 覆盖同名方案
- `rename`: 自动重命名（`name_imported` 或指定 `--new-name`）
- `skip`: 跳过冲突方案

冲突类型包括：同名、字段缺失、主版本不兼容。

### 对比分析

```
python -m pipeline compare --help
```

| 命令 | 说明 |
|------|------|
| `compare run NAME BATCH_ID1 BATCH_ID2 [...] [--scheme-id N]` | 生成多批次对比报告（至少 2 个批次） |
| `compare list` | 列出所有对比报告 |
| `compare show REPORT_ID [--metrics] [--anomalies] [--batches]` | 显示报告详情 |
| `compare export REPORT_ID -o PATH --format json\|csv` | 导出报告 |
| `compare delete REPORT_ID` | 删除报告 |

**报告内容：**
- 批次摘要（ID、名称、锁定状态、源文件、配置版本、运行号、处理行数、异常数、指标数）
- 指标差异矩阵（每个传感器/指标的绝对差和相对差百分比）
- 异常数量变化（按批次、按传感器分布）
- 关联的方案名称和版本（用于追溯）

**导出格式：**
- `json`: 单个完整 JSON 文件
- `csv`: 拆分为 4 个文件（summary/batches/metrics/anomalies）

## 锁定保护机制

1. 批次锁定后 `process` 无条件拒绝，不产生新 Run
2. 批次锁定后无法修改配置（包括 `set-threshold` 和 `scheme apply`）
3. 锁定批次可以正常参与对比分析和导出操作，历史数据不会被污染
4. 对比报告始终基于各批次锁定时的最新 Run 结果生成，与方案解耦

## 数据持久化

所有数据存储在 SQLite 数据库中（默认 `pipeline.db`，可用 `--db PATH` 指定）：

- `batches`: 批次元数据和配置
- `runs`: 运行记录（含配置快照）
- `row_errors`/`metrics`/`anomalies`: 处理结果明细
- `exports`: 导出记录
- `analysis_schemes`: 命名分析方案
- `comparison_reports`: 对比报告快照

## 日志

使用 Python `logging` 模块，Logger 名称 `pipeline.service`。关键操作（方案保存、导入、对比报告生成）会输出 INFO 级别日志，包含方案/报告 ID、名称和参与批次。

## 测试

```bash
# 验收测试
python samples/acceptance_test.py

# 回归测试（包含方案和对比分析相关用例）
python samples/regression_test.py
```

回归测试覆盖：
- 方案保存后跨重启一致性
- 方案导入冲突（同名/字段缺失/版本不兼容）处理
- 锁定批次参与对比分析（不被改写）
- 报告导出字段稳定性（JSON/CSV 表头和值一致）
