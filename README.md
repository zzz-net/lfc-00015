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

### 运行包快照 (Run Snapshot)
将处理完成的实验批次打包成可复现实验包，包含完整的源数据摘要、配置版本、指标结果、异常记录、依赖版本和校验信息。快照可以在另一份数据库或重启后导入查看，并支持用快照中的配置和样本重新运行（replay）进行结果对比。

**快照包内容（ZIP 压缩）：**
- `manifest.json`: 快照元数据，包含来源信息、配置摘要、指标摘要、异常摘要、错误摘要、依赖版本和校验和
- `config.json`: 完整的运行配置
- `metrics.json`: 所有指标结果
- `anomalies.json`: 异常检测结果
- `errors.json`: 行级错误记录
- `source_summary.json`: 源 CSV 摘要（文件哈希、样本数据、列统计、时间范围）
- `dependencies.json`: 依赖版本信息
- `checksum.json`: 所有文件的 SHA256 校验和

**冲突处理策略：**
- `reject`（默认）: 检测到同名快照时报错拒绝
- `rename`: 自动重命名（添加时间戳后缀或指定 `--new-name`）
- `skip`: 跳过冲突快照

**审计日志：**
所有快照操作（导出/导入/重放/删除）均写入审计日志，包含操作类型、快照信息、结果状态和错误原因。

### 基线批次库 (Baseline Library)
将已处理完成的批次登记为可复用基线，后续新批次可直接做指标漂移复核并给出通过/警告/阻断三级结论。基线库持久化保存配置版本、关键指标阈值、来源摘要、备注和最近一次复核结果，重启后完整可查。

**核心概念：**
- **基线 (Baseline)**：一个已处理批次的"金标准"快照，包含配置、指标阈值、来源信息
- **三级复核状态**：
  - `pass`（通过）：所有指标在警告阈值以内，可正常发布
  - `warn`（警告）：有指标超出警告阈值但未达阻断阈值，需人工关注
  - `block`（阻断）：有指标超出阻断阈值，停止发布，必须排查根因
- **指标漂移阈值**：默认 warn=±5%，block=±15%，注册时可自定义

**基线包内容（ZIP 压缩）：**
- `baseline_summary.json`: 基线元数据（名称、配置版本、来源批次、状态、格式版本）
- `config.json`: 完整的运行配置
- `metric_thresholds.json`: 关键指标阈值（每个指标的基线值、warn/block 阈值百分比）
- `source_summary.json`: 来源批次摘要（行数、列、源文件哈希、统计信息）
- `check_history.json`: 复核历史摘要（总次数、各状态计数、最近记录）
- `checksum.json`: 所有文件的 SHA256 校验和（含格式版本和文件大小）

**冲突处理策略：**
- `reject`（默认）: 检测到同名基线时报错拒绝，决定写入审计日志（result=blocked）
- `rename`: 自动重命名（添加 `_imported_N` 后缀或指定 `--new-name`），写入审计日志
- `skip`: 跳过冲突基线，不写入新记录

**审计日志：**
所有基线操作（注册/复核/导出/导入/删除）均写入 `scheme_audit_log` 表，新增字段 `baseline_id`、`baseline_name` 用于关联查询，支持按操作类型和结果筛选。

**追溯字段：**
- `original_baseline_id`: 导入基线的原始 ID（从原数据库导出时的 ID）
- `imported_from`: 导入来源（ZIP 文件名或路径哈希）

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
| `scheme apply SCHEME_ID BATCH_ID [--dry-run]` | 将方案应用到未锁定批次（不自动重跑），支持 --dry-run 预检 |
| `scheme clone SOURCE_SCHEME_ID NEW_NAME [--description TEXT]` | 基于已有方案克隆出新方案，可改名称和描述 |
| `scheme clone-apply SOURCE_SCHEME_ID NEW_NAME BATCH_ID [--description TEXT]` | 克隆方案并立即应用到未锁定批次（不自动重跑） |
| `scheme derive SOURCE_SCHEME_ID NEW_NAME [--description TEXT]` | 基于已有方案派生出新方案，记录来源关系（source_scheme_id） |
| `scheme derive-apply SOURCE_SCHEME_ID NEW_NAME BATCH_ID [--description TEXT]` | 派生方案并立即应用到未锁定批次，7步校验+步骤级日志 |
| `scheme history BATCH_ID` | 查看批次的方案应用/回滚历史记录 |
| `scheme rollback BATCH_ID [--dry-run]` | 回滚批次到上一个配置版本（撤销最近一次方案应用或修改），支持 --dry-run |
| `scheme switch TYPE BATCH_ID [--scheme-id N] [--source-scheme-id N] [--new-name NAME] [--dry-run]` | 统一切换入口：apply/clone/derive/rollback，预检→确认→执行完整流水 |
| `scheme dry-run SCHEME_ID BATCH_ID [--new-name NAME] [--source-scheme-id N]` | 预检查方案应用风险（不实际执行），含当前方案 vs 待切方案对比 |
| `scheme rollback-dry-run BATCH_ID` | 回滚预检（不实际执行），预览回滚后的配置变化 |
| `scheme audit-history [BATCH_ID] [--scheme-id N] [--action TYPE] [--result TYPE] [--limit N] [--diff]` | 查看方案应用审计历史（含配置差异、结果、失败原因） |
| `scheme export SCHEME_ID -o FILE.json` | 导出方案为 JSON 文件 |
| `scheme import FILE.json [--on-conflict ask\|overwrite\|rename\|skip] [--new-name NAME]` | 从文件导入方案 |
| `scheme import-apply FILE.json BATCH_ID [--on-conflict overwrite\|rename\|skip] [--new-name NAME]` | 从文件导入方案并一步应用到批次 |
| `scheme last-change BATCH_ID` | 查看批次最近一次方案变更结果 |
| `scheme delete SCHEME_ID` | 删除方案 |

**导入冲突处理策略：**
- `ask`（默认）: 检测到冲突时报错，提示用户选择
- `overwrite`: 覆盖同名方案
- `rename`: 自动重命名（`name_imported` 或指定 `--new-name`）
- `skip`: 跳过冲突方案

冲突类型包括：同名、字段缺失、主版本不兼容。

### 方案克隆与应用链路使用说明

```bash
# 步骤 1：先有一个源方案
python -m pipeline create exp_001 samples/sensor_data.csv
python -m pipeline process 1
python -m pipeline scheme save my_scheme --batch-id 1 --description "基础分析方案"

# 步骤 2a：仅克隆（不关联批次）
python -m pipeline scheme clone 1 my_scheme_v2 --description "方案第二版"
# 终端输出:
#   [OK] 方案克隆成功
#     源方案:   ID=1
#     新方案:   ID=2, 名称='my_scheme_v2'

# 步骤 2b：克隆 + 一步应用到新批次（推荐，省掉 export/import 往返）
python -m pipeline create exp_002 samples/sensor_data.csv
python -m pipeline scheme clone-apply 1 my_scheme_tuned 2 --description "针对批次2调整"
# 终端输出:
#   [OK] 方案克隆并应用成功
#     源方案:   ID=1, 名称='my_scheme'
#     新方案:   ID=3, 名称='my_scheme_tuned'
#     应用批次: ID=2, 配置版本升至 v2
#   请执行 process 命令以使用新配置重跑该批次。

# 后续：按提示重跑
python -m pipeline process 2
```

**`clone` 规则：**
- 新方案名称已存在 → 报错 `冲突(name_exists): 方案名称已存在: '...'`，详情含 `existing_scheme_id`、`source_scheme_id`、`source_scheme_name`
- 不自动覆盖或重命名（需要改名请先手动改 `new_name` 或用 `scheme import --on-conflict rename`）

**`clone-apply` 规则（原子操作，按以下顺序校验）：**
1. 校验源方案存在；校验批次存在
2. **锁定批次直接拒绝**（`[ERROR] 批次 N 已锁定，无法应用克隆方案。如需使用该方案进行对比分析，请使用 compare 命令...`），**不会创建新方案**
3. 校验新名称不冲突；冲突则报错同上，不创建任何东西
4. 通过以上校验后才创建新方案并 bump 配置版本应用到批次

锁定批次拒绝规则与 `scheme apply` 完全一致。

**日志定位：**
- Logger 名称：`pipeline.service`，级别：`INFO`
- `scheme clone` 产生 1 条日志：
  ```
  INFO pipeline.service: 方案已克隆: source_id=1, source_name='my_scheme', cloned_id=2, cloned_name='my_scheme_v2'
  ```
- `scheme clone-apply` 产生 2 条独立日志（克隆 + 应用各 1 条，方便筛选）：
  ```
  INFO pipeline.service: 方案已克隆(链路): source_id=1, source_name='my_scheme', cloned_id=3, cloned_name='my_scheme_tuned'
  INFO pipeline.service: 克隆方案已应用到批次: scheme_id=3, scheme_name='my_scheme_tuned', batch_id=2, new_config_version=2
  ```

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
2. 批次锁定后无法修改配置（包括 `set-threshold`、`scheme apply`、`scheme clone-apply`）。`clone-apply` 在克隆前先校验锁定，锁定时**不创建新方案**（原子性）
3. 锁定批次可以正常参与对比分析和导出操作，历史数据不会被污染
4. 对比报告始终基于各批次锁定时的最新 Run 结果生成，与方案解耦

## 数据持久化

所有数据存储在 SQLite 数据库中（默认 `pipeline.db`，可用 `--db PATH` 指定）：

- `batches`: 批次元数据和配置（含当前方案信息）
- `runs`: 运行记录（含配置快照）
- `row_errors`/`metrics`/`anomalies`: 处理结果明细
- `exports`: 导出记录
- `analysis_schemes`: 命名分析方案（含来源方案追溯）
- `batch_scheme_history`: 批次方案应用/回滚历史记录
- `comparison_reports`: 对比报告快照

## 日志

使用 Python `logging` 模块，Logger 名称 `pipeline.service`，所有方案克隆相关操作均输出 `INFO` 级别日志：

| 操作 | 日志条数 | 关键字段 |
|------|----------|----------|
| `scheme save` | 1 | id、name、batch_id |
| `scheme import`（四种分支各 1 条） | 1 | file、scheme_id、name、original/final |
| `scheme apply` | 1 | scheme_id、scheme_name、batch_id、new_config_version |
| `scheme clone` | 1 | source_id、source_name、cloned_id、cloned_name |
| `scheme clone-apply` | 2 | 克隆一条（含链路标记）+ 应用一条（含 scheme_id、scheme_name、batch_id、new_config_version） |
| `scheme derive` | 4 | 每步校验1条 + 创建1条（含 source_id、source_name、derived_id、derived_name） |
| `scheme derive-apply` | 7 | 每步校验/操作1条，含步骤编号和 result（通过/失败/成功），失败日志含失败步骤 |
| `compare run` | 1 | id、name、scheme、batch_ids |

方案克隆链路的日志样例见上文"方案克隆与应用链路使用说明 → 日志定位"。

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
- 方案克隆（仅克隆）成功克隆、同名冲突
- 方案克隆并应用链路：成功克隆应用、同名冲突、锁定批次拒绝
- 克隆后重启查询：克隆方案和应用后的配置版本持久化
- 克隆方案与导出/导入的兼容性（克隆方案可正常导出并再导入）
- **Dry-run 预检成功**：无风险时返回 can_proceed=True，配置变更预览正确
- **Dry-run 预检拦截**：锁定批次、名称冲突、方案不存在等场景正确阻止
- **执行成功后历史可查**：apply/clone-apply/derive-apply 成功后审计日志包含完整信息（配置差异、触发方式、结果）
- **执行失败后历史可查**：失败/阻止场景审计日志包含错误原因，可通过 result=failed/blocked 筛选
- **导入导出后历史连续**：方案导出再导入后，通过 original_id 保持关联，继续应用时历史不中断
- **跨重启审计持久化**：重启后审计日志完整保留，可正常查询
- **CLI 输出对齐**：dry-run、audit-history、switch 命令输出与 README 文档一致
- **回滚审计记录完整**：成功/失败/阻止三种回滚场景均有审计日志，含前后配置差异
- **switch 命令流水完整**：apply/clone/derive/rollback 四种模式均走预检→确认→执行
- **回滚前后结果变化**：配置版本、当前方案、配置值在回滚后正确回到上一版本
- **rollback dry-run 拦截生效**：锁定批次、无历史、最早版本三种场景预检阻止
- **导入冲突追溯完整**：rename/overwrite/skip 三种冲突策略均填充 original_name、final_name、original_id、imported_from；import 审计日志记录完整
- **导入后跨重启查询**：导入方案、审计日志、original_id 在重启后持久化并可查
- **import-apply 一步链路**：导入修改后 JSON → 立即应用 → 审计日志记录 import_apply 动作及配置差异
- **导入应用后回滚再处理**：回滚后阈值恢复、继续处理正常、审计包含 import_apply 和 rollback 记录
- **last-change 快速查看**：no_change/apply/threshold/rollback 各状态正确返回，跨重启持久化
- **CLI import-apply 和 last-change 命令**：帮助文本正常、执行输出包含追溯字段、last-change 显示 import_apply 动作
- **导入应用→切换回滚→继续处理→审计连续**：import-apply 后可回滚，last-change 显示回滚结果，审计历史完整
- **快照导出**：ZIP 包包含所有必需文件（manifest/config/metrics/anomalies/errors/source_summary/dependencies/checksum），校验和正确
- **快照跨重启/跨数据库导入**：导出后重启或在新数据库中导入，数据完整可查
- **快照导入冲突处理**：reject/rename/skip 三种策略均正确工作
- **源文件缺失导入**：源文件缺失时导入不阻止，记录警告，replay 时需指定替代文件
- **配置版本冲突导入**：配置版本不兼容时导入不阻止，使用快照中的配置
- **快照 replay 指标对比**：重放成功，指标对比输出差异、失败原因、是否可接受
- **replay 容忍度机制**：指标差异超出容忍度时标记为不可接受，列出失败指标
- **快照审计日志查询**：可按操作类型和结果筛选，所有快照操作均有审计记录
- **CLI snapshot 命令**：帮助文本完整，输出与文档对齐
