"""
回归测试脚本 - 修复锁定边界、导出字段、UTF-8 BOM 问题

测试清单:
1. 锁定后 process_batch 无条件拒绝，不产生新 run，历史 run 数量不变
2. 锁定后改源 CSV 再尝试 process，默认导出结果不受污染
3. 跨重启后：锁定状态、run 列表、最新 run、导出指标值完全一致
4. 未锁定批次重跑：run_number 递增，配置版本正确关联每次 run
5. 导出 metrics/errors/anomalies CSV 必须包含 run_number 和 config_version 字段
6. UTF-8 BOM 的 CSV 可正常导入，首列表头不被 \ufeff 污染
7. CLI --force 已移除，process 命令帮助文本与行为一致
"""
import os
import sys
import json
import shutil
import tempfile
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.service import (
    PipelineService, BatchLockedError, BatchServiceError,
    SchemeError, SchemeConflictError, SchemeImportResult, SchemeCloneResult,
    SchemeDeriveResult, SchemeRollbackResult, DryRunResult, DryRunRisk,
    SwitchSchemeResult,
    TicketError, TicketConflictError, TicketImportResult
)
from pipeline import database as db
from pipeline.processor import import_csv
from pipeline.config import get_default_config


SAMPLE_CSV = os.path.join(os.path.dirname(__file__), "sensor_data.csv")


class TestResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.error = None

    def ok(self):
        self.passed = True

    def fail(self, err: str):
        self.passed = False
        self.error = err


def run_tests():
    results = []
    tmpdir = tempfile.mkdtemp(prefix="pipeline_regression_")
    db_path = os.path.join(tmpdir, "test.db")
    out_dir = os.path.join(tmpdir, "exports")

    try:
        # ====== 共用准备 ======
        svc = PipelineService(db_path)
        batch_id = svc.create_batch("reg_001", SAMPLE_CSV)
        run_id1, run_n1 = svc.process_batch(batch_id)
        assert run_n1 == 1
        metrics_v1 = svc.get_run_metrics(run_id1)

        # ====== 测试 1: 锁定后无条件拒绝 process ======
        r1 = TestResult("测试1: 锁定后无条件拒绝 process，run 数量不变")
        results.append(r1)
        try:
            svc.lock_batch(batch_id)
            runs_before = svc.list_runs(batch_id)
            try:
                svc.process_batch(batch_id)
                assert False, "锁定后 process_batch 应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            runs_after = svc.list_runs(batch_id)
            assert len(runs_after) == len(runs_before), \
                f"锁定后 run 数量不应变化: before={len(runs_before)}, after={len(runs_after)}"

            batch = svc.get_batch(batch_id)
            assert batch["status"] == "locked", f"锁定后状态应为 locked, 实际 {batch['status']}"

            r1.ok()
            print(f"  [PASS] {r1.name}")
        except Exception as e:
            r1.fail(str(e))
            print(f"  [FAIL] {r1.name}: {e}")

        # ====== 测试 2: 锁定后改源 CSV 再尝试 process，导出结果不被污染 ======
        r2 = TestResult("测试2: 锁定后改源 CSV 再 process，默认导出不被污染")
        results.append(r2)
        try:
            modified_csv = os.path.join(tmpdir, "modified.csv")
            with open(SAMPLE_CSV, "r", encoding="utf-8") as f:
                content = f.read()
            modified_content = content.replace("23.5", "9999.0")
            with open(modified_csv, "w", encoding="utf-8") as f:
                f.write(modified_content)

            conn = db.get_connection(db_path)
            try:
                conn.execute("UPDATE batches SET source_file = ? WHERE id = ?", (modified_csv, batch_id))
                conn.commit()
            finally:
                conn.close()

            try:
                svc.process_batch(batch_id)
                assert False, "锁定后 process_batch 应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            path1 = svc.export_metrics(batch_id, os.path.join(out_dir, "m1.csv"))
            with open(path1, "r", encoding="utf-8-sig") as f:
                lines1 = f.readlines()

            latest_run = svc.get_latest_run(batch_id)
            assert latest_run["id"] == run_id1, \
                f"默认导出应仍指向锁定前的 run {run_id1}, 实际指向 {latest_run['id']}"

            metrics_after = svc.get_run_metrics(latest_run["id"])
            assert len(metrics_after) == len(metrics_v1), "指标数量应不变"
            for m1, m2 in zip(sorted(metrics_v1, key=lambda x: (x["sensor_name"], x["metric_name"])),
                              sorted(metrics_after, key=lambda x: (x["sensor_name"], x["metric_name"]))):
                assert m1["sensor_name"] == m2["sensor_name"]
                assert m1["metric_name"] == m2["metric_name"]
                assert abs(m1["metric_value"] - m2["metric_value"]) < 1e-9, \
                    f"指标值被污染: {m1} vs {m2}"

            r2.ok()
            print(f"  [PASS] {r2.name}  (latest_run_id={latest_run['id']})")
        except Exception as e:
            r2.fail(str(e))
            print(f"  [FAIL] {r2.name}: {e}")

        # ====== 测试 3: 跨重启一致性 ======
        r3 = TestResult("测试3: 跨重启后锁定状态、run 列表、导出指标一致")
        results.append(r3)
        try:
            del svc
            svc2 = PipelineService(db_path)

            batch2 = svc2.get_batch(batch_id)
            assert batch2["status"] == "locked", f"重启后状态应为 locked, 实际 {batch2['status']}"
            assert batch2["locked"] == 1

            runs2 = svc2.list_runs(batch_id)
            assert len(runs2) == 1, f"重启后 run 数应为 1, 实际 {len(runs2)}"

            path2 = svc2.export_metrics(batch_id, os.path.join(out_dir, "m2.csv"))
            with open(path2, "r", encoding="utf-8-sig") as f:
                lines2 = f.readlines()
            assert lines1 == lines2, "跨重启导出的 metrics CSV 应逐字节一致"

            r3.ok()
            print(f"  [PASS] {r3.name}  (status={batch2['status']}, runs={len(runs2)})")
        except Exception as e:
            r3.fail(str(e))
            print(f"  [FAIL] {r3.name}: {e}")

        # ====== 测试 4: 未锁定批次重跑，run_number 和 config_version 正确关联 ======
        r4 = TestResult("测试4: 未锁定批次重跑: run_number 递增，各 run 配置版本正确")
        results.append(r4)
        try:
            svc2.unlock_batch(batch_id)
            new_cfg = svc2.set_threshold(batch_id, zscore_threshold=1.0)
            run_id2, run_n2 = svc2.process_batch(batch_id)
            assert run_n2 == 2

            run2 = svc2.get_run(run_id2)
            assert run2["config_version"] == new_cfg["version"], \
                f"run2 配置版本应为 v{new_cfg['version']}, 实际 v{run2['config_version']}"

            run1 = svc2.get_run(run_id1)
            assert run1["config_version"] == 1, f"run1 配置版本应为 v1, 实际 v{run1['config_version']}"

            runs = svc2.list_runs(batch_id)
            assert len(runs) == 2

            r4.ok()
            print(f"  [PASS] {r4.name}  (runs={len(runs)}, run1_cfg_v={run1['config_version']}, run2_cfg_v={run2['config_version']})")
        except Exception as e:
            r4.fail(str(e))
            print(f"  [FAIL] {r4.name}: {e}")

        # ====== 测试 5: 导出 CSV 含 config_version 和 run_number 字段 ======
        r5 = TestResult("测试5: 导出 metrics/errors/anomalies CSV 含 run_number 和 config_version 字段")
        results.append(r5)
        try:
            mp = svc2.export_metrics(batch_id, os.path.join(out_dir, "f_m.csv"), run_id2)
            ep = svc2.export_errors(batch_id, os.path.join(out_dir, "f_e.csv"), run_id2)
            ap = svc2.export_anomalies(batch_id, os.path.join(out_dir, "f_a.csv"), run_id2)

            import csv as csv_mod
            with open(mp, "r", encoding="utf-8-sig") as f:
                reader = csv_mod.DictReader(f)
                headers = reader.fieldnames
                assert "config_version" in headers, f"metrics 缺 config_version，实际字段: {headers}"
                assert "run_number" in headers, f"metrics 缺 run_number，实际字段: {headers}"
                row = next(reader)
                assert int(row["config_version"]) == new_cfg["version"], \
                    f"metrics config_version 值不对: {row['config_version']}"
                assert int(row["run_number"]) == 2, f"metrics run_number 值不对: {row['run_number']}"

            with open(ep, "r", encoding="utf-8-sig") as f:
                reader = csv_mod.DictReader(f)
                headers = reader.fieldnames
                assert "config_version" in headers, f"errors 缺 config_version，实际字段: {headers}"
                assert "run_number" in headers, f"errors 缺 run_number，实际字段: {headers}"

            with open(ap, "r", encoding="utf-8-sig") as f:
                reader = csv_mod.DictReader(f)
                headers = reader.fieldnames
                assert "config_version" in headers, f"anomalies 缺 config_version，实际字段: {headers}"
                assert "run_number" in headers, f"anomalies 缺 run_number，实际字段: {headers}"

            r5.ok()
            print(f"  [PASS] {r5.name}  (metrics headers: {headers})")
        except Exception as e:
            r5.fail(str(e))
            print(f"  [FAIL] {r5.name}: {e}")

        # ====== 测试 6: UTF-8 BOM CSV 导入 ======
        r6 = TestResult("测试6: UTF-8 BOM 的 CSV 可正常导入")
        results.append(r6)
        try:
            bom_path = os.path.join(tmpdir, "with_bom.csv")
            with open(bom_path, "wb") as f:
                f.write(b"\xef\xbb\xbf")
                with open(SAMPLE_CSV, "rb") as src:
                    f.write(src.read())

            df_bom, errs_bom = import_csv(bom_path)
            assert "timestamp" in df_bom.columns, f"BOM CSV 首列表头被污染，实际列: {list(df_bom.columns)}"
            assert not df_bom.columns[0].startswith("\ufeff"), "首列不应包含 BOM 字符"
            assert len(df_bom) > 0, "BOM CSV 应有有效行"

            batch_id_bom = svc2.create_batch("bom_batch", bom_path)
            svc2.process_batch(batch_id_bom)
            batch_bom = svc2.get_batch(batch_id_bom)
            assert batch_bom["status"] == "processed", f"BOM 批次状态应为 processed, 实际 {batch_bom['status']}"

            r6.ok()
            print(f"  [PASS] {r6.name}  (columns={list(df_bom.columns)}, rows={len(df_bom)})")
        except Exception as e:
            r6.fail(str(e))
            print(f"  [FAIL] {r6.name}: {e}")

        # ====== 测试 7: CLI 帮助文本与行为一致（无 --force） ======
        r7 = TestResult("测试7: CLI process 帮助文本不含 --force，行为与文本一致")
        results.append(r7)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()
            result = runner.invoke(cli, ["process", "--help"])
            help_text = result.output
            assert "--force" not in help_text, "CLI process --help 不应出现 --force"
            assert result.exit_code == 0

            r7.ok()
            print(f"  [PASS] {r7.name}")
        except Exception as e:
            r7.fail(str(e))
            print(f"  [FAIL] {r7.name}: {e}")

        # ====== 测试 8: 方案保存后跨重启一致性 ======
        r8 = TestResult("测试8: 分析方案保存后跨重启一致")
        results.append(r8)
        try:
            svc3 = PipelineService(db_path)
            scheme_name = "scheme_for_restart_test"
            sid = svc3.save_scheme(scheme_name, batch_id=batch_id, description="重启测试方案")
            scheme_before = svc3.get_scheme(sid)
            assert scheme_before["name"] == scheme_name
            assert "cleaning" in scheme_before["config"]
            assert "missing_values" in scheme_before["config"]

            del svc3
            svc4 = PipelineService(db_path)
            scheme_after = svc4.get_scheme(sid)
            assert scheme_after is not None, "重启后方案应存在"
            assert scheme_after["id"] == sid
            assert scheme_after["name"] == scheme_before["name"]
            assert scheme_after["scheme_version"] == scheme_before["scheme_version"]
            assert scheme_after["description"] == scheme_before["description"]
            assert json.dumps(scheme_after["config"], sort_keys=True) == \
                   json.dumps(scheme_before["config"], sort_keys=True), \
                   "重启前后方案配置应完全一致"

            schemes_list = svc4.list_schemes()
            assert any(s["id"] == sid for s in schemes_list), "list_schemes 应包含保存的方案"

            r8.ok()
            print(f"  [PASS] {r8.name}  (scheme_id={sid})")
        except Exception as e:
            r8.fail(str(e))
            print(f"  [FAIL] {r8.name}: {e}")

        # ====== 测试 9: 方案导入冲突（同名、字段缺失、版本不兼容） ======
        r9 = TestResult("测试9: 方案导入冲突处理（同名/字段缺失/版本不兼容）")
        results.append(r9)
        try:
            svc5 = PipelineService(db_path)

            schemes_tmp = os.path.join(tmpdir, "schemes")
            os.makedirs(schemes_tmp, exist_ok=True)

            base_scheme_path = os.path.join(schemes_tmp, "base.json")
            svc5.export_scheme_to_file(sid, base_scheme_path)

            res_skip = svc5.import_scheme_from_file(
                base_scheme_path, on_conflict=SchemeImportResult.ACTION_SKIP)
            assert not res_skip.success, "同名 skip 应返回 success=False"
            assert res_skip.action == SchemeImportResult.ACTION_SKIP

            res_overwrite = svc5.import_scheme_from_file(
                base_scheme_path, on_conflict=SchemeImportResult.ACTION_OVERWRITE)
            assert res_overwrite.success, "同名 overwrite 应成功"
            assert res_overwrite.action == SchemeImportResult.ACTION_OVERWRITE
            assert res_overwrite.scheme_id == sid

            res_rename = svc5.import_scheme_from_file(
                base_scheme_path, on_conflict=SchemeImportResult.ACTION_RENAME,
                new_name="scheme_renamed_v1")
            assert res_rename.success, "rename 应成功"
            assert res_rename.action == SchemeImportResult.ACTION_RENAME
            assert res_rename.scheme_id != sid

            missing_field_path = os.path.join(schemes_tmp, "missing.json")
            with open(base_scheme_path, "r", encoding="utf-8") as f:
                bad_data = json.load(f)
            del bad_data["config"]["cleaning"]
            with open(missing_field_path, "w", encoding="utf-8") as f:
                json.dump(bad_data, f, ensure_ascii=False)

            try:
                svc5.import_scheme_from_file(missing_field_path, on_conflict=None)
                assert False, "字段缺失应抛出 SchemeConflictError"
            except SchemeConflictError as sce:
                assert sce.conflict_type == SchemeConflictError.CONFLICT_MISSING_FIELDS
                assert "cleaning" in sce.details.get("missing_fields", [])

            bad_version_path = os.path.join(schemes_tmp, "bad_version.json")
            with open(base_scheme_path, "r", encoding="utf-8") as f:
                bad_v = json.load(f)
            bad_v["scheme_version"] = "99.0"
            with open(bad_version_path, "w", encoding="utf-8") as f:
                json.dump(bad_v, f, ensure_ascii=False)

            try:
                svc5.import_scheme_from_file(bad_version_path, on_conflict=None)
                assert False, "版本不兼容应抛出 SchemeConflictError"
            except SchemeConflictError as sce:
                assert sce.conflict_type == SchemeConflictError.CONFLICT_VERSION

            r9.ok()
            print(f"  [PASS] {r9.name}  (skip/overwrite/rename/missing_field/version 均通过)")
        except Exception as e:
            r9.fail(str(e))
            print(f"  [FAIL] {r9.name}: {e}")

        # ====== 测试 10: 锁定批次参与对比（不被改写） ======
        r10 = TestResult("测试10: 锁定批次参与对比但不被改写，方案可关联报告")
        results.append(r10)
        try:
            svc6 = PipelineService(db_path)

            second_batch_id = svc6.create_batch("second_for_compare", SAMPLE_CSV)
            svc6.set_threshold(second_batch_id, zscore_threshold=1.0)
            svc6.process_batch(second_batch_id)

            svc6.lock_batch(batch_id)
            locked_batch = svc6.get_batch(batch_id)
            assert locked_batch["status"] == "locked"
            runs_before = svc6.list_runs(batch_id)
            runs_before_count = len(runs_before)
            metrics_before = svc6.get_run_metrics(runs_before[0]["id"])
            anomalies_before = svc6.get_run_anomalies(runs_before[0]["id"])

            report = svc6.generate_comparison_report(
                "compare_locked_batch", [batch_id, second_batch_id], scheme_id=sid)
            assert report["report_id"] > 0
            assert report["scheme"]["id"] == sid
            assert report["scheme"]["name"] is not None

            batch_summaries = report["batch_summaries"]
            locked_summary = next(b for b in batch_summaries if b["batch_id"] == batch_id)
            assert locked_summary["locked"] is True

            runs_after = svc6.list_runs(batch_id)
            assert len(runs_after) == runs_before_count, \
                f"锁定批次参与对比不应新增 run: before={runs_before_count}, after={len(runs_after)}"

            locked_batch2 = svc6.get_batch(batch_id)
            assert locked_batch2["status"] == "locked", "锁定批次状态不应改变"

            metrics_after = svc6.get_run_metrics(runs_after[0]["id"])
            assert len(metrics_after) == len(metrics_before)
            for m1, m2 in zip(sorted(metrics_before, key=lambda x: (x["sensor_name"], x["metric_name"])),
                              sorted(metrics_after, key=lambda x: (x["sensor_name"], x["metric_name"]))):
                assert abs(m1["metric_value"] - m2["metric_value"]) < 1e-9

            anomalies_after = svc6.get_run_anomalies(runs_after[0]["id"])
            assert len(anomalies_after) == len(anomalies_before)

            del svc6
            svc7 = PipelineService(db_path)
            reports_list = svc7.list_comparison_reports()
            assert any(r["id"] == report["report_id"] for r in reports_list), \
                "重启后对比报告应保留"
            report_reloaded = svc7.get_comparison_report(report["report_id"])
            assert report_reloaded is not None
            assert report_reloaded["scheme_name"] is not None
            assert batch_id in report_reloaded["batch_ids"]

            r10.ok()
            print(f"  [PASS] {r10.name}  (report_id={report['report_id']}, locked_runs={runs_before_count})")
        except Exception as e:
            r10.fail(str(e))
            print(f"  [FAIL] {r10.name}: {e}")

        # ====== 测试 11: 报告导出 JSON/CSV 字段稳定性 ======
        r11 = TestResult("测试11: 对比报告导出 JSON/CSV 字段稳定一致")
        results.append(r11)
        try:
            svc8 = PipelineService(db_path)
            existing_reports = svc8.list_comparison_reports()
            assert len(existing_reports) > 0, "应有已生成的报告"
            rid = existing_reports[0]["id"]

            json_out = os.path.join(out_dir, f"report_{rid}.json")
            svc8.export_comparison_report_json(rid, json_out)
            assert os.path.exists(json_out)
            with open(json_out, "r", encoding="utf-8") as f:
                json_data = json.load(f)
            for key in ("name", "scheme", "generated_at", "batch_summaries",
                        "metrics_diff", "anomalies_diff"):
                assert key in json_data, f"JSON 报告缺少字段: {key}"
            assert "id" in json_data["scheme"] and "name" in json_data["scheme"] and "version" in json_data["scheme"]
            assert len(json_data["batch_summaries"]) >= 2
            for bs in json_data["batch_summaries"]:
                for h in ("batch_id", "batch_name", "locked", "source_file",
                          "config_version", "run_id", "run_number",
                          "anomalies_count", "metrics_count"):
                    assert h in bs, f"batch_summary 缺少字段: {h}"
            md_summary = json_data["metrics_diff"]["summary"]
            for h in ("total_metrics_compared", "metrics_with_diff", "batch_keys"):
                assert h in md_summary, f"metrics_diff.summary 缺少字段: {h}"
            ad = json_data["anomalies_diff"]
            for h in ("per_batch", "total_anomalies_range", "sensors_with_anomalies"):
                assert h in ad, f"anomalies_diff 缺少字段: {h}"

            csv_dir = os.path.join(out_dir, f"report_csv_{rid}")
            csv_paths = svc8.export_comparison_report_csv(rid, csv_dir)
            for key in ("summary", "batches", "metrics", "anomalies"):
                assert key in csv_paths, f"CSV 导出缺少文件: {key}"
                assert os.path.exists(csv_paths[key])

            import csv as csv_mod
            with open(csv_paths["summary"], "r", encoding="utf-8-sig") as f:
                reader = csv_mod.DictReader(f)
                headers_summary = reader.fieldnames
                for h in ("report_id", "report_name", "scheme_name", "scheme_version",
                          "generated_at", "batch_count"):
                    assert h in headers_summary, f"summary.csv 缺少表头: {h}"
                row = next(reader)
                assert int(row["report_id"]) == rid

            with open(csv_paths["batches"], "r", encoding="utf-8-sig") as f:
                reader = csv_mod.DictReader(f)
                headers_b = reader.fieldnames
                for h in ("batch_id", "batch_name", "locked", "source_file",
                          "config_version", "run_id", "run_number",
                          "anomalies_count", "metrics_count"):
                    assert h in headers_b, f"batches.csv 缺少表头: {h}"
                rows_b = list(reader)
                assert len(rows_b) >= 2

            with open(csv_paths["metrics"], "r", encoding="utf-8-sig") as f:
                reader = csv_mod.DictReader(f)
                headers_m = reader.fieldnames
                assert "sensor" in headers_m and "metric" in headers_m
                assert "abs_diff" in headers_m and "rel_diff_pct" in headers_m

            with open(csv_paths["anomalies"], "r", encoding="utf-8-sig") as f:
                reader = csv_mod.DictReader(f)
                headers_a = reader.fieldnames
                for h in ("batch_id", "batch_name", "locked", "total_anomalies"):
                    assert h in headers_a, f"anomalies.csv 缺少表头: {h}"

            r11.ok()
            print(f"  [PASS] {r11.name}  (json + {len(csv_paths)} csv files 均通过)")
        except Exception as e:
            r11.fail(str(e))
            print(f"  [FAIL] {r11.name}: {e}")

        # ====== 测试 12: CLI 新增命令帮助文本完整 ======
        r12 = TestResult("测试12: CLI scheme/compare 命令帮助文本完整")
        results.append(r12)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()

            res_scheme = runner.invoke(cli, ["scheme", "--help"])
            assert res_scheme.exit_code == 0
            assert "save" in res_scheme.output
            assert "list" in res_scheme.output
            assert "import" in res_scheme.output
            assert "export" in res_scheme.output
            assert "apply" in res_scheme.output

            res_cmp = runner.invoke(cli, ["compare", "--help"])
            assert res_cmp.exit_code == 0
            assert "run" in res_cmp.output
            assert "list" in res_cmp.output
            assert "export" in res_cmp.output

            res_run = runner.invoke(cli, ["compare", "run", "--help"])
            assert res_run.exit_code == 0
            assert "scheme-id" in res_run.output

            res_imp = runner.invoke(cli, ["scheme", "import", "--help"])
            assert res_imp.exit_code == 0
            assert "on-conflict" in res_imp.output

            r12.ok()
            print(f"  [PASS] {r12.name}")
        except Exception as e:
            r12.fail(str(e))
            print(f"  [FAIL] {r12.name}: {e}")

        # ====== 测试 13: scheme save 日志输出（级别+内容+时机） ======
        r13 = TestResult("测试13: scheme save 输出 INFO 日志，含方案名和批次标识")
        results.append(r13)
        try:
            import logging

            svc_log1 = PipelineService(db_path)

            test_batch = svc_log1.create_batch("logtest_batch_save", SAMPLE_CSV)
            svc_log1.process_batch(test_batch)

            log_records = []
            class _CaptureHandler(logging.Handler):
                def emit(self, record):
                    log_records.append(record)

            handler = _CaptureHandler()
            handler.setLevel(logging.DEBUG)
            svc_logger = logging.getLogger("pipeline.service")
            original_level = svc_logger.level
            svc_logger.addHandler(handler)
            svc_logger.setLevel(logging.INFO)

            try:
                scheme_name = "logtest_scheme_save"
                sid = svc_log1.save_scheme(scheme_name, batch_id=test_batch, description="日志测试")

                save_logs = [r for r in log_records if "方案" in r.getMessage() or "scheme" in r.getMessage().lower()]
                assert len(save_logs) >= 1, f"save_scheme 应产生至少 1 条方案相关日志，实际 {len(save_logs)} 条"

                save_record = save_logs[-1]
                assert save_record.levelno == logging.INFO, \
                    f"日志级别应为 INFO，实际 {logging.getLevelName(save_record.levelno)}"

                msg = save_record.getMessage()
                assert scheme_name in msg, f"日志应包含方案名 '{scheme_name}'，实际: {msg}"
                assert str(test_batch) in msg or "batch_id" in msg, \
                    f"日志应包含批次标识，实际: {msg}"
                assert str(sid) in msg, f"日志应包含方案 ID，实际: {msg}"

                assert "pipeline.service" in save_record.name, \
                    f"日志来源应为 pipeline.service，实际 {save_record.name}"
            finally:
                svc_logger.removeHandler(handler)
                svc_logger.setLevel(original_level)

            r13.ok()
            print(f"  [PASS] {r13.name}  (scheme_id={sid}, log_level=INFO)")
        except Exception as e:
            r13.fail(str(e))
            print(f"  [FAIL] {r13.name}: {e}")

        # ====== 测试 14: scheme import 日志输出（新导入/覆盖/重命名/跳过均有对应日志） ======
        r14 = TestResult("测试14: scheme import 各分支输出对应 INFO 日志，含文件名和方案名")
        results.append(r14)
        try:
            import logging

            svc_log2 = PipelineService(db_path)

            schemes_dir = os.path.join(tmpdir, "logtest_schemes")
            os.makedirs(schemes_dir, exist_ok=True)

            base_scheme_path = os.path.join(schemes_dir, "base_logtest.json")
            svc_log2.export_scheme_to_file(sid, base_scheme_path)

            log_records = []
            class _CaptureHandler(logging.Handler):
                def emit(self, record):
                    log_records.append(record)

            handler = _CaptureHandler()
            handler.setLevel(logging.DEBUG)
            svc_logger = logging.getLogger("pipeline.service")
            original_level = svc_logger.level
            svc_logger.addHandler(handler)
            svc_logger.setLevel(logging.INFO)

            try:
                fresh_path = os.path.join(schemes_dir, "fresh.json")
                with open(base_scheme_path, "r", encoding="utf-8") as f:
                    fresh_data = json.load(f)
                fresh_data["name"] = "logtest_fresh_import"
                with open(fresh_path, "w", encoding="utf-8") as f:
                    json.dump(fresh_data, f, ensure_ascii=False)

                before_count = len(log_records)
                res_fresh = svc_log2.import_scheme_from_file(fresh_path)
                assert res_fresh.success
                fresh_logs = log_records[before_count:]
                assert len(fresh_logs) >= 1, "新导入应产生至少 1 条日志"
                assert fresh_logs[-1].levelno == logging.INFO
                assert "导入" in fresh_logs[-1].getMessage()
                assert "logtest_fresh_import" in fresh_logs[-1].getMessage()

                before_count = len(log_records)
                res_over = svc_log2.import_scheme_from_file(
                    fresh_path, on_conflict=SchemeImportResult.ACTION_OVERWRITE)
                assert res_over.success
                over_logs = log_records[before_count:]
                assert len(over_logs) >= 1, "覆盖导入应产生至少 1 条日志"
                assert over_logs[-1].levelno == logging.INFO
                assert "覆盖" in over_logs[-1].getMessage()

                before_count = len(log_records)
                res_rename = svc_log2.import_scheme_from_file(
                    fresh_path, on_conflict=SchemeImportResult.ACTION_RENAME)
                assert res_rename.success
                rename_logs = log_records[before_count:]
                assert len(rename_logs) >= 1, "重命名导入应产生至少 1 条日志"
                assert rename_logs[-1].levelno == logging.INFO
                assert "重命名" in rename_logs[-1].getMessage()

                before_count = len(log_records)
                res_skip = svc_log2.import_scheme_from_file(
                    fresh_path, on_conflict=SchemeImportResult.ACTION_SKIP)
                assert not res_skip.success
                skip_logs = log_records[before_count:]
                assert len(skip_logs) >= 1, "跳过导入应产生至少 1 条日志"
                assert skip_logs[-1].levelno == logging.INFO
                assert "跳过" in skip_logs[-1].getMessage()
            finally:
                svc_logger.removeHandler(handler)
                svc_logger.setLevel(original_level)

            r14.ok()
            print(f"  [PASS] {r14.name}  (fresh/overwrite/rename/skip 四分支均有 INFO 日志)")
        except Exception as e:
            r14.fail(str(e))
            print(f"  [FAIL] {r14.name}: {e}")

        # ====== 测试 15: compare run 日志输出（含报告名、方案名、批次列表） ======
        r15 = TestResult("测试15: compare run 输出 INFO 日志，含报告名、方案名、批次标识")
        results.append(r15)
        try:
            import logging

            svc_log3 = PipelineService(db_path)

            batch_a = svc_log3.create_batch("logtest_cmp_a", SAMPLE_CSV)
            svc_log3.process_batch(batch_a)
            batch_b = svc_log3.create_batch("logtest_cmp_b", SAMPLE_CSV)
            svc_log3.set_threshold(batch_b, zscore_threshold=1.2)
            svc_log3.process_batch(batch_b)

            log_records = []
            class _CaptureHandler(logging.Handler):
                def emit(self, record):
                    log_records.append(record)

            handler = _CaptureHandler()
            handler.setLevel(logging.DEBUG)
            svc_logger = logging.getLogger("pipeline.service")
            original_level = svc_logger.level
            svc_logger.addHandler(handler)
            svc_logger.setLevel(logging.INFO)

            try:
                report_name = "logtest_compare_report"
                report = svc_log3.generate_comparison_report(
                    report_name, [batch_a, batch_b], scheme_id=sid)

                cmp_logs = [r for r in log_records if "对比" in r.getMessage() or "compare" in r.getMessage().lower() or "报告" in r.getMessage()]
                assert len(cmp_logs) >= 1, f"generate_comparison_report 应产生至少 1 条报告相关日志，实际 {len(cmp_logs)} 条"

                cmp_record = cmp_logs[-1]
                assert cmp_record.levelno == logging.INFO, \
                    f"日志级别应为 INFO，实际 {logging.getLevelName(cmp_record.levelno)}"

                msg = cmp_record.getMessage()
                assert report_name in msg, f"日志应包含报告名 '{report_name}'，实际: {msg}"
                assert str(sid) in msg or "scheme" in msg.lower(), \
                    f"日志应包含方案标识，实际: {msg}"
                assert str(batch_a) in msg and str(batch_b) in msg, \
                    f"日志应包含所有参与批次 ID，实际: {msg}"
                assert str(report["report_id"]) in msg, \
                    f"日志应包含报告 ID，实际: {msg}"
            finally:
                svc_logger.removeHandler(handler)
                svc_logger.setLevel(original_level)

            r15.ok()
            print(f"  [PASS] {r15.name}  (report_id={report['report_id']}, log_level=INFO)")
        except Exception as e:
            r15.fail(str(e))
            print(f"  [FAIL] {r15.name}: {e}")

        # ====== 测试 16: 方案克隆（仅克隆）成功 + 同名冲突 ======
        r16 = TestResult("测试16: 方案 clone 成功克隆，同名时抛出 name_exists 冲突")
        results.append(r16)
        try:
            svc_clone1 = PipelineService(db_path)

            base_sid = svc_clone1.save_scheme(
                "clone_base_v1", batch_id=batch_id, description="克隆源方案")

            cloned_sid = svc_clone1.clone_scheme(base_sid, "clone_base_v1_copy", "克隆副本描述")
            assert cloned_sid > base_sid, "克隆方案 ID 应大于源方案"

            cloned = svc_clone1.get_scheme(cloned_sid)
            assert cloned["name"] == "clone_base_v1_copy"
            assert cloned["description"] == "克隆副本描述"
            assert cloned["scheme_version"] == svc_clone1.get_scheme(base_sid)["scheme_version"]
            assert json.dumps(cloned["config"], sort_keys=True) == \
                   json.dumps(svc_clone1.get_scheme(base_sid)["config"], sort_keys=True), \
                   "克隆配置应与源方案完全一致"

            try:
                svc_clone1.clone_scheme(base_sid, "clone_base_v1_copy")
                assert False, "同名克隆应抛出 SchemeConflictError"
            except SchemeConflictError as sce:
                assert sce.conflict_type == SchemeConflictError.CONFLICT_NAME
                assert sce.details.get("existing_scheme_id") == cloned_sid
                assert sce.details.get("source_scheme_id") == base_sid
                assert sce.details.get("source_scheme_name") == "clone_base_v1"

            schemes_list = svc_clone1.list_schemes()
            found_cloned = any(s["id"] == cloned_sid for s in schemes_list)
            assert found_cloned, "list_schemes 应包含克隆出的方案"

            r16.ok()
            print(f"  [PASS] {r16.name}  (base_sid={base_sid}, cloned_sid={cloned_sid})")
        except Exception as e:
            r16.fail(str(e))
            print(f"  [FAIL] {r16.name}: {e}")

        # ====== 测试 17: 克隆并应用链路 - 成功、同名冲突、锁定批次拒绝 ======
        r17 = TestResult("测试17: clone-and-apply 成功克隆应用、同名冲突、锁定批次拒绝")
        results.append(r17)
        try:
            svc_clone2 = PipelineService(db_path)

            second_bid = svc_clone2.create_batch("clone_apply_batch", SAMPLE_CSV)
            svc_clone2.process_batch(second_bid)

            base_scheme = svc_clone2.save_scheme(
                "clone_apply_base", batch_id=batch_id, description="克隆应用源方案")

            result = svc_clone2.clone_and_apply_scheme(
                base_scheme, "clone_apply_new", second_bid, "链路测试方案")
            assert isinstance(result, SchemeCloneResult)
            assert result.success is True
            assert result.source_scheme_id == base_scheme
            assert result.source_scheme_name == "clone_apply_base"
            assert result.cloned_scheme_id > base_scheme
            assert result.cloned_scheme_name == "clone_apply_new"
            assert result.applied_batch_id == second_bid
            assert result.new_config_version > 1

            second_batch = svc_clone2.get_batch(second_bid)
            assert second_batch["config_version"] == result.new_config_version, \
                f"批次配置版本应升至 v{result.new_config_version}, 实际 v{second_batch['config_version']}"

            applied_scheme = svc_clone2.get_scheme(result.cloned_scheme_id)
            assert applied_scheme is not None
            assert applied_scheme["name"] == "clone_apply_new"

            try:
                svc_clone2.clone_and_apply_scheme(base_scheme, "clone_apply_new", second_bid)
                assert False, "同名 clone-and-apply 应抛出 SchemeConflictError"
            except SchemeConflictError as sce:
                assert sce.conflict_type == SchemeConflictError.CONFLICT_NAME
                assert sce.details.get("existing_scheme_id") == result.cloned_scheme_id

            svc_clone2.lock_batch(second_bid)
            try:
                svc_clone2.clone_and_apply_scheme(base_scheme, "clone_apply_another", second_bid)
                assert False, "锁定批次 clone-and-apply 应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            svc_clone2.unlock_batch(second_bid)

            r17.ok()
            print(f"  [PASS] {r17.name}  (cloned_sid={result.cloned_scheme_id}, "
                  f"batch={second_bid}, new_cfg_v={result.new_config_version})")
        except Exception as e:
            r17.fail(str(e))
            print(f"  [FAIL] {r17.name}: {e}")

        # ====== 测试 18: 克隆后重启查询 - 方案和配置版本持久化 ======
        r18 = TestResult("测试18: 重启后克隆方案仍可查询，应用后的配置版本保留")
        results.append(r18)
        try:
            svc_pre = PipelineService(db_path)
            pre_sid = svc_pre.clone_scheme(sid, "restart_cloned_scheme", "重启前克隆")
            pre_batch_id = svc_pre.create_batch("restart_clone_batch", SAMPLE_CSV)
            svc_pre.process_batch(pre_batch_id)
            pre_result = svc_pre.clone_and_apply_scheme(
                pre_sid, "restart_clone_apply", pre_batch_id, "重启前克隆并应用")
            expected_cfg_v = pre_result.new_config_version

            del svc_pre

            svc_post = PipelineService(db_path)

            cloned_after = svc_post.get_scheme(pre_sid)
            assert cloned_after is not None, "重启后克隆方案应存在"
            assert cloned_after["id"] == pre_sid
            assert cloned_after["name"] == "restart_cloned_scheme"
            assert cloned_after["description"] == "重启前克隆"

            list_after = svc_post.list_schemes()
            assert any(s["id"] == pre_sid for s in list_after), "list_schemes 应包含重启前克隆的方案"

            applied_scheme_after = svc_post.get_scheme(pre_result.cloned_scheme_id)
            assert applied_scheme_after is not None
            assert applied_scheme_after["name"] == "restart_clone_apply"

            batch_after = svc_post.get_batch(pre_batch_id)
            assert batch_after["config_version"] == expected_cfg_v, \
                f"重启后批次配置版本应仍为 v{expected_cfg_v}, 实际 v{batch_after['config_version']}"

            r18.ok()
            print(f"  [PASS] {r18.name}  (cloned_sid={pre_sid}, batch_cfg_v={expected_cfg_v})")
        except Exception as e:
            r18.fail(str(e))
            print(f"  [FAIL] {r18.name}: {e}")

        # ====== 测试 19: 克隆方案与导出/导入兼容性 ======
        r19 = TestResult("测试19: 克隆方案可正常导出并再导入，配置一致")
        results.append(r19)
        try:
            svc_cmp = PipelineService(db_path)

            compat_sid = svc_cmp.clone_scheme(sid, "compat_clone_source", "兼容性测试源")

            export_dir = os.path.join(tmpdir, "compat_exports")
            os.makedirs(export_dir, exist_ok=True)
            export_path = os.path.join(export_dir, "compat_clone.json")
            svc_cmp.export_scheme_to_file(compat_sid, export_path)
            assert os.path.exists(export_path)

            import_result = svc_cmp.import_scheme_from_file(
                export_path, on_conflict=SchemeImportResult.ACTION_RENAME,
                new_name="compat_clone_imported")
            assert import_result.success
            assert import_result.action == SchemeImportResult.ACTION_RENAME

            imported_scheme = svc_cmp.get_scheme(import_result.scheme_id)
            original_cloned = svc_cmp.get_scheme(compat_sid)
            assert imported_scheme["name"] == "compat_clone_imported"
            assert json.dumps(imported_scheme["config"], sort_keys=True) == \
                   json.dumps(original_cloned["config"], sort_keys=True), \
                   "导出再导入后配置应完全一致"
            assert imported_scheme["scheme_version"] == original_cloned["scheme_version"]

            r19.ok()
            print(f"  [PASS] {r19.name}  (exported={export_path}, imported_id={import_result.scheme_id})")
        except Exception as e:
            r19.fail(str(e))
            print(f"  [FAIL] {r19.name}: {e}")

        # ====== 测试 20: 克隆和克隆应用日志输出 ======
        r20 = TestResult("测试20: clone 和 clone-and-apply 输出 INFO 日志，含源/目标方案和批次")
        results.append(r20)
        try:
            import logging

            svc_log4 = PipelineService(db_path)

            log_sid = svc_log4.save_scheme("log_clone_source", batch_id=batch_id)
            log_batch = svc_log4.create_batch("log_clone_apply_batch", SAMPLE_CSV)
            svc_log4.process_batch(log_batch)

            log_records = []

            class _CaptureHandler(logging.Handler):
                def emit(self, record):
                    log_records.append(record)

            handler = _CaptureHandler()
            handler.setLevel(logging.DEBUG)
            svc_logger = logging.getLogger("pipeline.service")
            original_level = svc_logger.level
            svc_logger.addHandler(handler)
            svc_logger.setLevel(logging.INFO)

            try:
                before_count = len(log_records)
                cloned_log_sid = svc_log4.clone_scheme(log_sid, "log_clone_target", "日志测试克隆")
                clone_logs = log_records[before_count:]
                assert len(clone_logs) >= 1, "clone_scheme 应至少产生 1 条日志"
                assert clone_logs[-1].levelno == logging.INFO
                clone_msg = clone_logs[-1].getMessage()
                assert str(log_sid) in clone_msg, f"克隆日志应含源方案 ID，实际: {clone_msg}"
                assert "log_clone_source" in clone_msg, f"克隆日志应含源方案名，实际: {clone_msg}"
                assert str(cloned_log_sid) in clone_msg, f"克隆日志应含新方案 ID，实际: {clone_msg}"
                assert "log_clone_target" in clone_msg, f"克隆日志应含新方案名，实际: {clone_msg}"

                before_count = len(log_records)
                result = svc_log4.clone_and_apply_scheme(
                    log_sid, "log_clone_apply_target", log_batch, "日志测试链路")
                apply_logs = log_records[before_count:]
                assert len(apply_logs) >= 2, f"clone_and_apply 应至少产生 2 条日志（克隆+应用），实际 {len(apply_logs)}"
                clone_apply_msgs = [r.getMessage() for r in apply_logs if r.levelno == logging.INFO]
                assert any("克隆" in m for m in clone_apply_msgs), "链路日志应含克隆相关记录"
                assert any("应用" in m for m in clone_apply_msgs), "链路日志应含应用相关记录"
                joined = " | ".join(clone_apply_msgs)
                assert str(log_sid) in joined, f"链路日志应含源方案 ID，实际: {joined}"
                assert str(result.cloned_scheme_id) in joined, f"链路日志应含新方案 ID，实际: {joined}"
                assert str(log_batch) in joined, f"链路日志应含批次 ID，实际: {joined}"

            finally:
                svc_logger.removeHandler(handler)
                svc_logger.setLevel(original_level)

            r20.ok()
            print(f"  [PASS] {r20.name}  (clone_logs={len(clone_logs)}, apply_logs={len(apply_logs)})")
        except Exception as e:
            r20.fail(str(e))
            print(f"  [FAIL] {r20.name}: {e}")

        # ====== 测试 21: CLI scheme clone / clone-apply 帮助文本完整性（含规则说明） ======
        r21 = TestResult("测试21: CLI scheme clone/clone-apply 帮助文本含规则和日志说明")
        results.append(r21)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()

            res_scheme_help = runner.invoke(cli, ["scheme", "--help"])
            assert res_scheme_help.exit_code == 0
            assert "clone" in res_scheme_help.output, "scheme --help 应包含 clone"
            assert "clone-apply" in res_scheme_help.output, "scheme --help 应包含 clone-apply"

            res_clone_help = runner.invoke(cli, ["scheme", "clone", "--help"])
            assert res_clone_help.exit_code == 0
            assert "source_scheme_id" in res_clone_help.output or "SOURCE_SCHEME_ID" in res_clone_help.output
            assert "new_name" in res_clone_help.output or "NEW_NAME" in res_clone_help.output
            assert "description" in res_clone_help.output
            assert "name_exists" in res_clone_help.output or "同名" in res_clone_help.output, \
                "clone --help 应说明同名冲突规则"
            assert "pipeline.service" in res_clone_help.output, \
                "clone --help 应标注 Logger 名称"

            res_clone_apply_help = runner.invoke(cli, ["scheme", "clone-apply", "--help"])
            assert res_clone_apply_help.exit_code == 0
            assert "source_scheme_id" in res_clone_apply_help.output or "SOURCE_SCHEME_ID" in res_clone_apply_help.output
            assert "new_name" in res_clone_apply_help.output or "NEW_NAME" in res_clone_apply_help.output
            assert "batch_id" in res_clone_apply_help.output or "BATCH_ID" in res_clone_apply_help.output
            assert "锁定" in res_clone_apply_help.output and "拒绝" in res_clone_apply_help.output, \
                "clone-apply --help 应说明锁定批次拒绝规则"
            assert "不会创建新方案" in res_clone_apply_help.output or "原子" in res_clone_apply_help.output, \
                "clone-apply --help 应说明原子性"
            assert "pipeline.service" in res_clone_apply_help.output, \
                "clone-apply --help 应标注 Logger 名称"

            r21.ok()
            print(f"  [PASS] {r21.name}")
        except Exception as e:
            r21.fail(str(e))
            print(f"  [FAIL] {r21.name}: {e}")

        # ====== 测试 22: clone-apply 成功路径（按 README 流程终端输出匹配 + 日志） ======
        r22 = TestResult("测试22: clone-apply 成功路径，终端输出含源/目标方案和批次，日志格式匹配文档")
        results.append(r22)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli
            import logging

            runner = CliRunner()
            svc_doc = PipelineService(db_path)

            bid_doc1 = svc_doc.create_batch("doc_exp_001", SAMPLE_CSV)
            svc_doc.process_batch(bid_doc1)
            sid_doc = svc_doc.save_scheme("doc_my_scheme", batch_id=bid_doc1, description="文档基础方案")
            bid_doc2 = svc_doc.create_batch("doc_exp_002", SAMPLE_CSV)
            svc_doc.process_batch(bid_doc2)

            log_records = []

            class _CaptureHandler(logging.Handler):
                def emit(self, record):
                    log_records.append(record)

            handler = _CaptureHandler()
            handler.setLevel(logging.DEBUG)
            svc_logger = logging.getLogger("pipeline.service")
            original_level = svc_logger.level
            svc_logger.addHandler(handler)
            svc_logger.setLevel(logging.INFO)

            try:
                cli_params = ["--db", db_path, "scheme", "clone-apply",
                              str(sid_doc), "doc_my_scheme_tuned", str(bid_doc2),
                              "--description", "文档链路测试"]
                res = runner.invoke(cli, cli_params)
                assert res.exit_code == 0, f"clone-apply 应成功退出，exit={res.exit_code}, output={res.output}"

                out = res.output
                assert "[OK]" in out, "终端输出应包含 [OK] 标记"
                assert "源方案" in out and "ID" in out and str(sid_doc) in out, \
                    f"终端输出应列出源方案 ID，实际输出:\n{out}"
                assert "doc_my_scheme" in out, f"终端输出应含源方案名，实际输出:\n{out}"
                assert "新方案" in out and "doc_my_scheme_tuned" in out, \
                    f"终端输出应含新方案名，实际输出:\n{out}"
                assert "应用批次" in out and str(bid_doc2) in out, \
                    f"终端输出应含应用批次 ID，实际输出:\n{out}"
                assert "配置版本" in out and "v" in out, \
                    f"终端输出应含配置版本，实际输出:\n{out}"
                assert "process" in out, "终端输出应提示执行 process 重跑"

                clone_logs = [r for r in log_records if "克隆" in r.getMessage() or "应用" in r.getMessage()]
                assert len(clone_logs) >= 2, f"clone-apply 应产生至少 2 条 INFO 日志，实际 {len(clone_logs)} 条"

                clone_msgs = [r.getMessage() for r in clone_logs if r.levelno == logging.INFO]
                joined = " | ".join(clone_msgs)
                assert str(sid_doc) in joined, f"日志应含源方案 ID，实际: {joined}"
                assert "doc_my_scheme" in joined, f"日志应含源方案名，实际: {joined}"
                assert "doc_my_scheme_tuned" in joined, f"日志应含新方案名，实际: {joined}"
                assert str(bid_doc2) in joined, f"日志应含批次 ID，实际: {joined}"
                assert "new_config_version" in joined or "配置" in joined, \
                    f"日志应含配置版本，实际: {joined}"

                source_exists = any(
                    "source_id" in m and "source_name" in m and "cloned_id" in m and "cloned_name" in m
                    for m in clone_msgs
                )
                assert source_exists, f"克隆链路日志应包含 source_id/source_name/cloned_id/cloned_name 模板字段，实际: {clone_msgs}"
            finally:
                svc_logger.removeHandler(handler)
                svc_logger.setLevel(original_level)

            batch_after = svc_doc.get_batch(bid_doc2)
            assert batch_after["config_version"] >= 2, \
                f"clone-apply 后批次配置版本应至少为 v2，实际 v{batch_after['config_version']}"

            new_scheme = svc_doc.get_scheme_by_name("doc_my_scheme_tuned")
            assert new_scheme is not None, "克隆出的新方案应存在"

            r22.ok()
            print(f"  [PASS] {r22.name}  (source_sid={sid_doc}, batch={bid_doc2}, cfg_v={batch_after['config_version']})")
        except Exception as e:
            r22.fail(str(e))
            print(f"  [FAIL] {r22.name}: {e}")

        # ====== 测试 23: clone-apply 锁定批次拒绝（不产生新方案） ======
        r23 = TestResult("测试23: clone-apply 到锁定批次拒绝，不创建新方案，终端和日志符合文档")
        results.append(r23)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli
            import logging

            runner = CliRunner()
            svc_rej = PipelineService(db_path)

            bid_lock = svc_rej.create_batch("lock_reject_batch", SAMPLE_CSV)
            svc_rej.process_batch(bid_lock)
            base_for_reject = svc_rej.create_batch("base_for_reject", SAMPLE_CSV)
            svc_rej.process_batch(base_for_reject)
            svc_rej.lock_batch(bid_lock)
            base_sid = svc_rej.save_scheme("reject_base_scheme", batch_id=base_for_reject, description="拒绝测试源")

            schemes_before = svc_rej.list_schemes()
            before_names = {s["name"] for s in schemes_before}

            log_records = []

            class _CaptureHandler(logging.Handler):
                def emit(self, record):
                    log_records.append(record)

            handler = _CaptureHandler()
            handler.setLevel(logging.DEBUG)
            svc_logger = logging.getLogger("pipeline.service")
            original_level = svc_logger.level
            svc_logger.addHandler(handler)
            svc_logger.setLevel(logging.INFO)

            try:
                new_name = "should_not_exist_scheme"
                res = runner.invoke(cli, ["--db", db_path, "scheme", "clone-apply",
                                          str(base_sid), new_name, str(bid_lock)])
                assert res.exit_code != 0, f"clone-apply 到锁定批次应 exit!=0，实际 {res.exit_code}"
                out = res.output
                assert "[ERROR]" in out, f"终端应输出 [ERROR]，实际输出:\n{out}"
                assert "锁定" in out, f"终端输出应包含 '锁定'，实际:\n{out}"
                assert "无法应用" in out or "拒绝" in out, f"终端应说明拒绝，实际:\n{out}"

                clone_logs_after = [r for r in log_records if new_name in r.getMessage()]
                assert len(clone_logs_after) == 0, \
                    f"锁定拒绝时不应产生任何含新方案名的日志，实际: {[r.getMessage() for r in clone_logs_after]}"
            finally:
                svc_logger.removeHandler(handler)
                svc_logger.setLevel(original_level)

            schemes_after = svc_rej.list_schemes()
            after_names = {s["name"] for s in schemes_after}
            assert after_names == before_names, \
                f"锁定拒绝时不应创建新方案，before={before_names}, after={after_names}"

            assert svc_rej.get_scheme_by_name("should_not_exist_scheme") is None, \
                "锁定拒绝时不应创建新方案记录"

            svc_rej.unlock_batch(bid_lock)

            r23.ok()
            print(f"  [PASS] {r23.name}  (lock_batch={bid_lock}, scheme_count={len(schemes_after)})")
        except Exception as e:
            r23.fail(str(e))
            print(f"  [FAIL] {r23.name}: {e}")

        # ====== 测试 24: derive 成功派生 + source_scheme_id 追溯 ======
        r24 = TestResult("测试24: derive 成功派生，source_scheme_id 记录正确，同名冲突抛 name_exists")
        results.append(r24)
        try:
            svc_d1 = PipelineService(db_path)

            d_base_bid = svc_d1.create_batch("derive_base_batch", SAMPLE_CSV)
            svc_d1.process_batch(d_base_bid)
            d_source_sid = svc_d1.save_scheme("derive_source", batch_id=d_base_bid, description="派生源方案")

            d_derived_sid = svc_d1.derive_scheme(d_source_sid, "derive_v2", "派生第二版")
            assert d_derived_sid > d_source_sid

            derived = svc_d1.get_scheme(d_derived_sid)
            assert derived["name"] == "derive_v2"
            assert derived["description"] == "派生第二版"
            assert derived["source_scheme_id"] == d_source_sid, \
                f"派生方案 source_scheme_id 应为 {d_source_sid}，实际 {derived.get('source_scheme_id')}"

            source_check = svc_d1.get_scheme(d_source_sid)
            assert source_check.get("source_scheme_id") is None, "原始方案 source_scheme_id 应为 None"

            try:
                svc_d1.derive_scheme(d_source_sid, "derive_v2")
                assert False, "同名派生应抛出 SchemeConflictError"
            except SchemeConflictError as sce:
                assert sce.conflict_type == SchemeConflictError.CONFLICT_NAME
                assert sce.details.get("source_scheme_id") == d_source_sid

            scheme_list = svc_d1.list_schemes()
            derived_in_list = any(s["id"] == d_derived_sid and s.get("source_scheme_id") == d_source_sid
                                  for s in scheme_list)
            assert derived_in_list, "list_schemes 应包含派生方案且 source_scheme_id 正确"

            r24.ok()
            print(f"  [PASS] {r24.name}  (source={d_source_sid}, derived={d_derived_sid})")
        except Exception as e:
            r24.fail(str(e))
            print(f"  [FAIL] {r24.name}: {e}")

        # ====== 测试 25: derive-apply 成功路径 + 步骤级日志 + 同名/锁定拒绝 ======
        r25 = TestResult("测试25: derive-apply 成功派生应用、同名冲突、锁定批次拒绝、步骤级日志可见")
        results.append(r25)
        try:
            import logging

            svc_d2 = PipelineService(db_path)

            da_base_bid = svc_d2.create_batch("da_base_batch", SAMPLE_CSV)
            svc_d2.process_batch(da_base_bid)
            da_source_sid = svc_d2.save_scheme("da_source", batch_id=da_base_bid, description="派生应用源")

            da_target_bid = svc_d2.create_batch("da_target_batch", SAMPLE_CSV)
            svc_d2.process_batch(da_target_bid)

            log_records = []

            class _CaptureHandler(logging.Handler):
                def emit(self, record):
                    log_records.append(record)

            handler = _CaptureHandler()
            handler.setLevel(logging.DEBUG)
            svc_logger = logging.getLogger("pipeline.service")
            original_level = svc_logger.level
            svc_logger.addHandler(handler)
            svc_logger.setLevel(logging.INFO)

            try:
                result = svc_d2.derive_and_apply_scheme(
                    da_source_sid, "da_derived_applied", da_target_bid, "链路派生方案")
                assert isinstance(result, SchemeDeriveResult)
                assert result.success is True
                assert result.source_scheme_id == da_source_sid
                assert result.source_scheme_name == "da_source"
                assert result.derived_scheme_id > da_source_sid
                assert result.derived_scheme_name == "da_derived_applied"
                assert result.applied_batch_id == da_target_bid
                assert result.new_config_version > 1
                assert result.failed_step is None

                derived_scheme = svc_d2.get_scheme(result.derived_scheme_id)
                assert derived_scheme["source_scheme_id"] == da_source_sid

                da_msgs = [r.getMessage() for r in log_records if r.levelno == logging.INFO]
                joined = " | ".join(da_msgs)
                assert "步骤1-校验源方案" in joined, f"日志应含步骤1，实际: {joined}"
                assert "步骤2-校验批次" in joined, f"日志应含步骤2，实际: {joined}"
                assert "步骤3-校验锁定" in joined, f"日志应含步骤3，实际: {joined}"
                assert "步骤4-校验名称冲突" in joined, f"日志应含步骤4，实际: {joined}"
                assert "步骤5-校验配置" in joined, f"日志应含步骤5，实际: {joined}"
                assert "步骤6-创建派生方案" in joined, f"日志应含步骤6，实际: {joined}"
                assert "步骤7-应用到批次" in joined, f"日志应含步骤7，实际: {joined}"
                assert str(da_source_sid) in joined
                assert str(result.derived_scheme_id) in joined
                assert str(da_target_bid) in joined
                assert "result=成功" in joined or "result=通过" in joined
            finally:
                svc_logger.removeHandler(handler)
                svc_logger.setLevel(original_level)

            try:
                svc_d2.derive_and_apply_scheme(da_source_sid, "da_derived_applied", da_target_bid)
                assert False, "同名应抛 SchemeConflictError"
            except SchemeConflictError as sce:
                assert sce.conflict_type == SchemeConflictError.CONFLICT_NAME

            svc_d2.lock_batch(da_target_bid)
            try:
                svc_d2.derive_and_apply_scheme(da_source_sid, "da_should_not_exist", da_target_bid)
                assert False, "锁定批次应抛 BatchLockedError"
            except BatchLockedError:
                pass

            assert svc_d2.get_scheme_by_name("da_should_not_exist") is None, \
                "锁定拒绝时不应创建新方案"

            svc_d2.unlock_batch(da_target_bid)

            r25.ok()
            print(f"  [PASS] {r25.name}  (derived={result.derived_scheme_id}, "
                  f"batch={da_target_bid}, cfg_v={result.new_config_version})")
        except Exception as e:
            r25.fail(str(e))
            print(f"  [FAIL] {r25.name}: {e}")

        # ====== 测试 26: derive 后重启查询 - 来源和配置版本持久化 ======
        r26 = TestResult("测试26: 重启后派生方案来源可查，应用后配置版本保留")
        results.append(r26)
        try:
            svc_pre2 = PipelineService(db_path)
            restart_src = svc_pre2.save_scheme("restart_derive_src", batch_id=batch_id, description="重启派生源")
            restart_bid = svc_pre2.create_batch("restart_derive_batch", SAMPLE_CSV)
            svc_pre2.process_batch(restart_bid)
            restart_result = svc_pre2.derive_and_apply_scheme(
                restart_src, "restart_derive_target", restart_bid, "重启前派生应用")
            expected_v = restart_result.new_config_version
            expected_derived_id = restart_result.derived_scheme_id

            del svc_pre2

            svc_post2 = PipelineService(db_path)

            derived_after = svc_post2.get_scheme(expected_derived_id)
            assert derived_after is not None
            assert derived_after["source_scheme_id"] == restart_src, \
                f"重启后 source_scheme_id 应为 {restart_src}，实际 {derived_after.get('source_scheme_id')}"
            assert derived_after["name"] == "restart_derive_target"

            batch_after = svc_post2.get_batch(restart_bid)
            assert batch_after["config_version"] == expected_v, \
                f"重启后配置版本应为 v{expected_v}，实际 v{batch_after['config_version']}"

            r26.ok()
            print(f"  [PASS] {r26.name}  (derived={expected_derived_id}, source={restart_src}, cfg_v={expected_v})")
        except Exception as e:
            r26.fail(str(e))
            print(f"  [FAIL] {r26.name}: {e}")

        # ====== 测试 27: derive 后导出再导入，source_scheme_id 保留 ======
        r27 = TestResult("测试27: 派生方案导出再导入后来源可追溯，链路可用")
        results.append(r27)
        try:
            svc_d3 = PipelineService(db_path)

            ei_src = svc_d3.save_scheme("ei_source", batch_id=batch_id, description="导出导入源")
            ei_derived = svc_d3.derive_scheme(ei_src, "ei_derived", "导出导入派生")

            ei_export_dir = os.path.join(tmpdir, "ei_exports")
            os.makedirs(ei_export_dir, exist_ok=True)
            ei_path = os.path.join(ei_export_dir, "ei_derived.json")
            svc_d3.export_scheme_to_file(ei_derived, ei_path)

            with open(ei_path, "r", encoding="utf-8") as f:
                exported = json.load(f)
            assert exported.get("source_scheme_id") == ei_src, \
                f"导出 JSON 应含 source_scheme_id={ei_src}，实际 {exported.get('source_scheme_id')}"

            import_result = svc_d3.import_scheme_from_file(
                ei_path, on_conflict=SchemeImportResult.ACTION_RENAME,
                new_name="ei_derived_imported")
            assert import_result.success

            imported = svc_d3.get_scheme(import_result.scheme_id)
            assert imported["source_scheme_id"] == ei_src, \
                f"导入后 source_scheme_id 应为 {ei_src}，实际 {imported.get('source_scheme_id')}"
            assert imported["name"] == "ei_derived_imported"
            assert json.dumps(imported["config"], sort_keys=True) == \
                   json.dumps(svc_d3.get_scheme(ei_derived)["config"], sort_keys=True)

            ei_chain_sid = svc_d3.derive_scheme(import_result.scheme_id, "ei_chain_derived", "链式派生")
            chain_scheme = svc_d3.get_scheme(ei_chain_sid)
            assert chain_scheme["source_scheme_id"] == import_result.scheme_id, \
                "链式派生来源应为导入后的方案"

            r27.ok()
            print(f"  [PASS] {r27.name}  (derived={ei_derived}, imported={import_result.scheme_id}, chain={ei_chain_sid})")
        except Exception as e:
            r27.fail(str(e))
            print(f"  [FAIL] {r27.name}: {e}")

        # ====== 测试 28: CLI derive / derive-apply / scheme show(来源) + 三边对齐 ======
        r28 = TestResult("测试28: CLI derive/derive-apply/scheme show 三边对齐")
        results.append(r28)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()

            res_scheme_help = runner.invoke(cli, ["scheme", "--help"])
            assert "derive" in res_scheme_help.output, "scheme --help 应包含 derive"
            assert "derive-apply" in res_scheme_help.output, "scheme --help 应包含 derive-apply"

            res_derive_help = runner.invoke(cli, ["scheme", "derive", "--help"])
            assert res_derive_help.exit_code == 0
            assert "source_scheme_id" in res_derive_help.output or "SOURCE_SCHEME_ID" in res_derive_help.output
            assert "pipeline.service" in res_derive_help.output

            res_da_help = runner.invoke(cli, ["scheme", "derive-apply", "--help"])
            assert res_da_help.exit_code == 0
            assert "锁定" in res_da_help.output and "拒绝" in res_da_help.output
            assert "步骤" in res_da_help.output
            assert "pipeline.service" in res_da_help.output

            svc_cli = PipelineService(db_path)
            cli_bid = svc_cli.create_batch("cli_derive_batch", SAMPLE_CSV)
            svc_cli.process_batch(cli_bid)
            cli_sid = svc_cli.save_scheme("cli_source", batch_id=cli_bid, description="CLI源")
            cli_target = svc_cli.create_batch("cli_derive_target", SAMPLE_CSV)
            svc_cli.process_batch(cli_target)

            res_derive = runner.invoke(cli, ["--db", db_path, "scheme", "derive",
                                              str(cli_sid), "cli_derived", "--description", "CLI派生"])
            assert res_derive.exit_code == 0
            assert "[OK]" in res_derive.output
            assert "派生方案" in res_derive.output
            assert "cli_derived" in res_derive.output
            assert "来源追溯" in res_derive.output

            res_da = runner.invoke(cli, ["--db", db_path, "scheme", "derive-apply",
                                          str(cli_sid), "cli_da_name", str(cli_target),
                                          "--description", "CLI派生应用"])
            assert res_da.exit_code == 0
            out = res_da.output
            assert "[OK]" in out
            assert "源方案" in out and str(cli_sid) in out
            assert "派生方案" in out and "cli_da_name" in out
            assert "应用批次" in out and str(cli_target) in out
            assert "配置版本" in out and "v" in out
            assert "来源追溯" in out

            res_show = runner.invoke(cli, ["--db", db_path, "scheme", "show", str(cli_sid)])
            assert "派生来源" in res_show.output
            assert "原始方案" in res_show.output

            r28.ok()
            print(f"  [PASS] {r28.name}")
        except Exception as e:
            r28.fail(str(e))
            print(f"  [FAIL] {r28.name}: {e}")

        # ====== 测试 29: 方案应用后批次当前方案信息 + 历史记录 ======
        r29 = TestResult("测试29: 方案应用后批次 current_scheme 正确，历史记录生成，含来源方案")
        results.append(r29)
        try:
            svc_h1 = PipelineService(db_path)

            h_base_bid = svc_h1.create_batch("hist_base_batch", SAMPLE_CSV)
            svc_h1.process_batch(h_base_bid)
            h_src_sid = svc_h1.save_scheme("hist_source", batch_id=h_base_bid, description="历史测试源")

            h_derived_sid = svc_h1.derive_scheme(h_src_sid, "hist_derived", "历史测试派生")

            h_target_bid = svc_h1.create_batch("hist_target_batch", SAMPLE_CSV)
            svc_h1.process_batch(h_target_bid)

            new_cfg = svc_h1.apply_scheme_to_batch(h_derived_sid, h_target_bid)

            batch_after = svc_h1.get_batch(h_target_bid)
            assert batch_after["current_scheme_id"] == h_derived_sid, \
                f"批次 current_scheme_id 应为 {h_derived_sid}，实际 {batch_after.get('current_scheme_id')}"
            assert batch_after["current_scheme_name"] == "hist_derived", \
                f"批次 current_scheme_name 应为 'hist_derived'，实际 {batch_after.get('current_scheme_name')}"
            assert batch_after["config_version"] == new_cfg["version"]

            history = svc_h1.get_scheme_history(h_target_bid)
            assert len(history) >= 1, f"至少应有 1 条历史记录，实际 {len(history)}"
            latest = history[0]
            assert latest["action"] == "apply", f"最新历史操作应为 apply，实际 {latest['action']}"
            assert latest["scheme_id"] == h_derived_sid, \
                f"历史记录 scheme_id 应为 {h_derived_sid}，实际 {latest.get('scheme_id')}"
            assert latest["source_scheme_id"] == h_src_sid, \
                f"历史记录 source_scheme_id 应为 {h_src_sid}，实际 {latest.get('source_scheme_id')}"
            assert latest["config_version"] == new_cfg["version"]

            r29.ok()
            print(f"  [PASS] {r29.name}  (hist_count={len(history)}, scheme_id={h_derived_sid})")
        except Exception as e:
            r29.fail(str(e))
            print(f"  [FAIL] {r29.name}: {e}")

        # ====== 测试 30: 直接修改配置（set-threshold）也记录历史 ======
        r30 = TestResult("测试30: set-threshold 直接修改配置也记录历史，标记为 direct 操作")
        results.append(r30)
        try:
            svc_h2 = PipelineService(db_path)

            th_bid = svc_h2.create_batch("thresh_hist_batch", SAMPLE_CSV)
            svc_h2.process_batch(th_bid)

            hist_before = svc_h2.get_scheme_history(th_bid)
            before_count = len(hist_before)

            new_cfg = svc_h2.set_threshold(th_bid, zscore_threshold=1.5)

            hist_after = svc_h2.get_scheme_history(th_bid)
            non_baseline_after = [h for h in hist_after if h.get("action") != "baseline"]
            non_baseline_before = [h for h in hist_before if h.get("action") != "baseline"]
            assert len(non_baseline_after) == len(non_baseline_before) + 1, \
                f"set-threshold 后应新增 1 条非 baseline 历史，before={len(non_baseline_before)}, after={len(non_baseline_after)}"

            latest = hist_after[0]
            assert latest["action"] == "direct", \
                f"直接修改操作应为 direct，实际 {latest['action']}"
            assert latest["config_version"] == new_cfg["version"]

            r30.ok()
            print(f"  [PASS] {r30.name}  (hist_count={len(hist_after)}, cfg_v={new_cfg['version']})")
        except Exception as e:
            r30.fail(str(e))
            print(f"  [FAIL] {r30.name}: {e}")

        # ====== 测试 31: 回滚功能 - 成功回滚到上一版本 ======
        r31 = TestResult("测试31: 方案应用后回滚成功，配置版本递增，方案信息回退")
        results.append(r31)
        try:
            svc_rb1 = PipelineService(db_path)

            rb_base_bid = svc_rb1.create_batch("rollback_base", SAMPLE_CSV)
            svc_rb1.process_batch(rb_base_bid)
            rb_src_sid = svc_rb1.save_scheme("rb_source", batch_id=rb_base_bid, description="回滚测试源")

            rb_target_bid = svc_rb1.create_batch("rollback_target", SAMPLE_CSV)
            svc_rb1.process_batch(rb_target_bid)

            v2_cfg = svc_rb1.apply_scheme_to_batch(rb_src_sid, rb_target_bid)
            v2_version = v2_cfg["version"]

            svc_rb1.set_threshold(rb_target_bid, zscore_threshold=2.5)

            hist_before_rb = svc_rb1.get_scheme_history(rb_target_bid)
            before_rb_count = len(hist_before_rb)
            batch_before = svc_rb1.get_batch(rb_target_bid)
            version_before_rb = batch_before["config_version"]

            result = svc_rb1.rollback_scheme(rb_target_bid)
            assert isinstance(result, SchemeRollbackResult)
            assert result.success is True
            assert result.previous_config_version == version_before_rb
            assert result.new_config_version > version_before_rb
            assert result.previous_scheme_id == rb_src_sid
            assert result.previous_scheme_name == "rb_source"

            batch_after = svc_rb1.get_batch(rb_target_bid)
            assert batch_after["config_version"] == result.new_config_version
            assert batch_after["current_scheme_id"] == rb_src_sid
            assert batch_after["current_scheme_name"] == "rb_source"

            hist_after = svc_rb1.get_scheme_history(rb_target_bid)
            assert len(hist_after) == before_rb_count + 1, \
                f"回滚后应新增 1 条历史，before={before_rb_count}, after={len(hist_after)}"
            assert hist_after[0]["action"] == "rollback", \
                f"最新历史应为 rollback，实际 {hist_after[0]['action']}"
            assert hist_after[0]["rolled_back_from_id"] == hist_before_rb[0]["id"]

            r31.ok()
            print(f"  [PASS] {r31.name}  (from_v={version_before_rb}, to_v={result.new_config_version})")
        except Exception as e:
            r31.fail(str(e))
            print(f"  [FAIL] {r31.name}: {e}")

        # ====== 测试 32: 锁定批次拒绝回滚 ======
        r32 = TestResult("测试32: 锁定批次拒绝回滚，抛出 BatchLockedError")
        results.append(r32)
        try:
            svc_rb2 = PipelineService(db_path)

            rb_lock_bid = svc_rb2.create_batch("rollback_locked", SAMPLE_CSV)
            svc_rb2.process_batch(rb_lock_bid)
            rb_lock_sid = svc_rb2.save_scheme("rb_lock_scheme", batch_id=rb_lock_bid)
            svc_rb2.apply_scheme_to_batch(rb_lock_sid, rb_lock_bid)
            svc_rb2.lock_batch(rb_lock_bid)

            try:
                svc_rb2.rollback_scheme(rb_lock_bid)
                assert False, "锁定批次回滚应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            batch_after = svc_rb2.get_batch(rb_lock_bid)
            assert batch_after["locked"] == 1
            assert batch_after["status"] == "locked"

            svc_rb2.unlock_batch(rb_lock_bid)

            r32.ok()
            print(f"  [PASS] {r32.name}  (batch={rb_lock_bid})")
        except Exception as e:
            r32.fail(str(e))
            print(f"  [FAIL] {r32.name}: {e}")

        # ====== 测试 33: 回滚后重新处理、重新应用走通 ======
        r33 = TestResult("测试33: 回滚后重新 process 正常，再次应用其他方案也正常")
        results.append(r33)
        try:
            svc_rb3 = PipelineService(db_path)

            rrp_bid = svc_rb3.create_batch("rb_reprocess_batch", SAMPLE_CSV)
            svc_rb3.process_batch(rrp_bid)
            rrp_sid1 = svc_rb3.save_scheme("rrp_scheme_v1", batch_id=rrp_bid, description="方案v1")

            rrp_sid2 = svc_rb3.derive_scheme(rrp_sid1, "rrp_scheme_v2", "方案v2")

            svc_rb3.apply_scheme_to_batch(rrp_sid2, rrp_bid)
            run1_id, run1_n = svc_rb3.process_batch(rrp_bid)
            metrics_run1 = svc_rb3.get_run_metrics(run1_id)

            svc_rb3.set_threshold(rrp_bid, zscore_threshold=1.2)

            rb_result = svc_rb3.rollback_scheme(rrp_bid)
            assert rb_result.success

            run2_id, run2_n = svc_rb3.process_batch(rrp_bid)
            assert run2_n == run1_n + 1
            metrics_run2 = svc_rb3.get_run_metrics(run2_id)
            assert len(metrics_run2) == len(metrics_run1)

            svc_rb3.apply_scheme_to_batch(rrp_sid2, rrp_bid)
            run3_id, run3_n = svc_rb3.process_batch(rrp_bid)
            assert run3_n == run2_n + 1

            r33.ok()
            print(f"  [PASS] {r33.name}  (runs={run3_n}, rollback={rb_result.new_config_version})")
        except Exception as e:
            r33.fail(str(e))
            print(f"  [FAIL] {r33.name}: {e}")

        # ====== 测试 34: 重启后历史记录和当前方案信息保留 ======
        r34 = TestResult("测试34: 重启后历史记录和当前方案信息持久化，不丢失")
        results.append(r34)
        try:
            svc_pre = PipelineService(db_path)

            restart_h_bid = svc_pre.create_batch("restart_hist_batch", SAMPLE_CSV)
            svc_pre.process_batch(restart_h_bid)
            restart_h_sid = svc_pre.save_scheme("restart_hist_scheme", batch_id=restart_h_bid)
            svc_pre.apply_scheme_to_batch(restart_h_sid, restart_h_bid)
            svc_pre.set_threshold(restart_h_bid, zscore_threshold=1.8)
            svc_pre.rollback_scheme(restart_h_bid)

            hist_before = svc_pre.get_scheme_history(restart_h_bid)
            batch_before = svc_pre.get_batch(restart_h_bid)
            expected_scheme_id = batch_before["current_scheme_id"]
            expected_scheme_name = batch_before["current_scheme_name"]
            expected_cfg_version = batch_before["config_version"]

            del svc_pre

            svc_post = PipelineService(db_path)

            batch_after = svc_post.get_batch(restart_h_bid)
            assert batch_after["current_scheme_id"] == expected_scheme_id, \
                f"重启后 current_scheme_id 应不变，before={expected_scheme_id}, after={batch_after.get('current_scheme_id')}"
            assert batch_after["current_scheme_name"] == expected_scheme_name
            assert batch_after["config_version"] == expected_cfg_version

            hist_after = svc_post.get_scheme_history(restart_h_bid)
            assert len(hist_after) == len(hist_before), \
                f"重启后历史记录数应不变，before={len(hist_before)}, after={len(hist_after)}"
            assert hist_after[0]["action"] == "rollback"

            r34.ok()
            print(f"  [PASS] {r34.name}  (hist_count={len(hist_after)}, scheme_id={expected_scheme_id})")
        except Exception as e:
            r34.fail(str(e))
            print(f"  [FAIL] {r34.name}: {e}")

        # ====== 测试 35: 导入导出后的副本继续应用，历史记录正确 ======
        r35 = TestResult("测试35: 导入导出后的派生方案继续应用到批次，历史记录含来源信息")
        results.append(r35)
        try:
            svc_ie = PipelineService(db_path)

            ie_src_bid = svc_ie.create_batch("ie_src_batch", SAMPLE_CSV)
            svc_ie.process_batch(ie_src_bid)
            ie_src_sid = svc_ie.save_scheme("ie_source", batch_id=ie_src_bid, description="导入导出源")
            ie_derived_sid = svc_ie.derive_scheme(ie_src_sid, "ie_derived", "导入导出派生")

            ie_export_dir = os.path.join(tmpdir, "ie_hist_exports")
            os.makedirs(ie_export_dir, exist_ok=True)
            ie_export_path = os.path.join(ie_export_dir, "ie_derived.json")
            svc_ie.export_scheme_to_file(ie_derived_sid, ie_export_path)

            import_result = svc_ie.import_scheme_from_file(
                ie_export_path, on_conflict=SchemeImportResult.ACTION_RENAME,
                new_name="ie_imported_apply")
            assert import_result.success

            imported_scheme = svc_ie.get_scheme(import_result.scheme_id)
            assert imported_scheme["source_scheme_id"] == ie_src_sid

            ie_target_bid = svc_ie.create_batch("ie_target_batch", SAMPLE_CSV)
            svc_ie.process_batch(ie_target_bid)

            new_cfg = svc_ie.apply_scheme_to_batch(import_result.scheme_id, ie_target_bid)

            batch_after = svc_ie.get_batch(ie_target_bid)
            assert batch_after["current_scheme_id"] == import_result.scheme_id
            assert batch_after["current_scheme_name"] == "ie_imported_apply"

            history = svc_ie.get_scheme_history(ie_target_bid)
            assert len(history) >= 1
            assert history[0]["scheme_id"] == import_result.scheme_id
            assert history[0]["source_scheme_id"] == ie_src_sid

            r35.ok()
            print(f"  [PASS] {r35.name}  (imported_id={import_result.scheme_id}, source={ie_src_sid})")
        except Exception as e:
            r35.fail(str(e))
            print(f"  [FAIL] {r35.name}: {e}")

        # ====== 测试 36: CLI history / rollback 命令输出完整 ======
        r36 = TestResult("测试36: CLI scheme history / rollback 命令输出符合文档，含必要字段")
        results.append(r36)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()
            svc_cli2 = PipelineService(db_path)

            cli_h_bid = svc_cli2.create_batch("cli_hist_batch", SAMPLE_CSV)
            svc_cli2.process_batch(cli_h_bid)
            cli_h_sid = svc_cli2.save_scheme("cli_hist_scheme", batch_id=cli_h_bid)
            svc_cli2.apply_scheme_to_batch(cli_h_sid, cli_h_bid)
            svc_cli2.set_threshold(cli_h_bid, zscore_threshold=1.8)

            res_history = runner.invoke(cli, ["--db", db_path, "scheme", "history", str(cli_h_bid)])
            assert res_history.exit_code == 0
            assert "操作" in res_history.output or "apply" in res_history.output
            assert "方案" in res_history.output
            assert "配置版本" in res_history.output

            res_rollback = runner.invoke(cli, ["--db", db_path, "scheme", "rollback", str(cli_h_bid)])
            assert res_rollback.exit_code == 0
            assert "[OK]" in res_rollback.output
            assert "配置回滚成功" in res_rollback.output
            assert "原版本" in res_rollback.output
            assert "新版本" in res_rollback.output
            assert "回滚到方案" in res_rollback.output
            assert "process" in res_rollback.output

            r36.ok()
            print(f"  [PASS] {r36.name}  (batch={cli_h_bid})")
        except Exception as e:
            r36.fail(str(e))
            print(f"  [FAIL] {r36.name}: {e}")

        # ====== 测试 37: 回滚前后处理结果变化验证 ======
        r37 = TestResult("测试37: 回滚前后 process 结果有差异（异常数量随阈值变化），回滚可撤销修改")
        results.append(r37)
        try:
            svc_diff = PipelineService(db_path)

            diff_bid = svc_diff.create_batch("diff_rollback_batch", SAMPLE_CSV)
            svc_diff.process_batch(diff_bid)

            svc_diff.set_threshold(diff_bid, zscore_threshold=2.0)
            base_run_id, _ = svc_diff.process_batch(diff_bid)
            base_anomalies = svc_diff.get_run_anomalies(base_run_id)
            base_count = len(base_anomalies)

            svc_diff.set_threshold(diff_bid, zscore_threshold=0.5)
            tight_run_id, _ = svc_diff.process_batch(diff_bid)
            tight_anomalies = svc_diff.get_run_anomalies(tight_run_id)
            tight_count = len(tight_anomalies)
            assert tight_count > base_count, \
                f"收紧阈值后异常应增多，base={base_count}, tight={tight_count}"

            rb_result = svc_diff.rollback_scheme(diff_bid)
            assert rb_result.success

            rb_run_id, _ = svc_diff.process_batch(diff_bid)
            rb_anomalies = svc_diff.get_run_anomalies(rb_run_id)
            rb_count = len(rb_anomalies)
            assert rb_count == base_count, \
                f"回滚后异常数应回到基线，base={base_count}, rollback={rb_count}"

            r37.ok()
            print(f"  [PASS] {r37.name}  (base={base_count}, tight={tight_count}, rollback={rb_count})")
        except Exception as e:
            r37.fail(str(e))
            print(f"  [FAIL] {r37.name}: {e}")

        # ====== 测试 38: 回滚日志输出完整（含来源方案和版本变化） ======
        r38 = TestResult("测试38: rollback 输出 INFO 日志，含 batch_id、版本变化、方案信息")
        results.append(r38)
        try:
            import logging

            svc_log_rb = PipelineService(db_path)

            log_rb_bid = svc_log_rb.create_batch("log_rollback_batch", SAMPLE_CSV)
            svc_log_rb.process_batch(log_rb_bid)
            log_rb_sid = svc_log_rb.save_scheme("log_rb_scheme", batch_id=log_rb_bid)
            svc_log_rb.apply_scheme_to_batch(log_rb_sid, log_rb_bid)
            svc_log_rb.set_threshold(log_rb_bid, zscore_threshold=1.5)

            log_records = []

            class _CaptureHandler(logging.Handler):
                def emit(self, record):
                    log_records.append(record)

            handler = _CaptureHandler()
            handler.setLevel(logging.DEBUG)
            svc_logger = logging.getLogger("pipeline.service")
            original_level = svc_logger.level
            svc_logger.addHandler(handler)
            svc_logger.setLevel(logging.INFO)

            try:
                before_count = len(log_records)
                result = svc_log_rb.rollback_scheme(log_rb_bid)
                assert result.success

                rb_logs = log_records[before_count:]
                assert len(rb_logs) >= 1, f"rollback 应至少产生 1 条日志，实际 {len(rb_logs)}"
                assert rb_logs[-1].levelno == logging.INFO

                msg = rb_logs[-1].getMessage()
                assert "回滚" in msg or "rollback" in msg.lower(), \
                    f"日志应含回滚标识，实际: {msg}"
                assert str(log_rb_bid) in msg, \
                    f"日志应含批次 ID，实际: {msg}"
                assert "from_version" in msg or "to_version" in msg or "版本" in msg, \
                    f"日志应含版本变化，实际: {msg}"
                assert str(log_rb_sid) in msg or "scheme_id" in msg, \
                    f"日志应含方案信息，实际: {msg}"
            finally:
                svc_logger.removeHandler(handler)
                svc_logger.setLevel(original_level)

            r38.ok()
            print(f"  [PASS] {r38.name}  (log_count={len(rb_logs)}, level=INFO)")
        except Exception as e:
            r38.fail(str(e))
            print(f"  [FAIL] {r38.name}: {e}")

        # ====== 测试 39: Dry-Run 预检成功 ======
        r39 = TestResult("测试39: dry-run 预检成功，无风险，can_proceed=True，配置变更预览正确")
        results.append(r39)
        try:
            svc_dry1 = PipelineService(db_path)

            dr_base_bid = svc_dry1.create_batch("dryrun_base", SAMPLE_CSV)
            svc_dry1.process_batch(dr_base_bid)
            dr_src_sid = svc_dry1.save_scheme("dryrun_source", batch_id=dr_base_bid, description="预检源方案")

            dr_target_bid = svc_dry1.create_batch("dryrun_target", SAMPLE_CSV)
            svc_dry1.process_batch(dr_target_bid)

            result = svc_dry1.dry_run_apply_scheme(dr_src_sid, dr_target_bid)
            assert isinstance(result, DryRunResult)
            assert result.can_proceed is True, "无风险时 can_proceed 应为 True"
            assert len(result.risks) == 0, f"无风险时 risks 应为空，实际 {len(result.risks)}"
            assert result.scheme_id == dr_src_sid
            assert result.scheme_name == "dryrun_source"
            assert result.batch_id == dr_target_bid

            assert result.config_diff is not None, "预检成功时应返回配置差异"
            assert "version_change" in result.config_diff
            assert result.config_diff["version_change"]["old"] >= 1
            assert result.config_diff["version_change"]["new"] > result.config_diff["version_change"]["old"]

            audit_logs = svc_dry1.get_scheme_audit_logs(batch_id=dr_target_bid, action="dry_run")
            assert len(audit_logs) >= 1, "dry-run 应记录审计日志"
            latest_audit = audit_logs[0]
            assert latest_audit["action"] == "dry_run"
            assert latest_audit["trigger_method"] == "cli"
            assert latest_audit["result"] == "success"
            assert latest_audit["scheme_id"] == dr_src_sid
            assert latest_audit["batch_id"] == dr_target_bid

            r39.ok()
            print(f"  [PASS] {r39.name}  (version_change=v{result.config_diff['version_change']['old']}→v{result.config_diff['version_change']['new']})")
        except Exception as e:
            r39.fail(str(e))
            print(f"  [FAIL] {r39.name}: {e}")

        # ====== 测试 40: Dry-Run 预检拦截（锁定批次、名称冲突、方案不存在） ======
        r40 = TestResult("测试40: dry-run 预检拦截，锁定批次/名称冲突/方案不存在等场景正确阻止")
        results.append(r40)
        try:
            svc_dry2 = PipelineService(db_path)

            dr_lock_bid = svc_dry2.create_batch("dryrun_locked", SAMPLE_CSV)
            svc_dry2.process_batch(dr_lock_bid)
            svc_dry2.lock_batch(dr_lock_bid)
            dr_src2_sid = svc_dry2.save_scheme("dryrun_source2", batch_id=dr_lock_bid)

            result_lock = svc_dry2.dry_run_apply_scheme(dr_src2_sid, dr_lock_bid)
            assert result_lock.can_proceed is False, "锁定批次时 can_proceed 应为 False"
            lock_risks = [r for r in result_lock.risks if r.risk_type == DryRunRisk.RISK_LOCKED]
            assert len(lock_risks) >= 1, "应检测到批次锁定风险"
            assert lock_risks[0].severity == DryRunRisk.SEVERITY_BLOCKER

            svc_dry2.unlock_batch(dr_lock_bid)

            exist_sid = svc_dry2.save_scheme("existing_scheme", batch_id=dr_lock_bid)
            result_name = svc_dry2.dry_run_apply_scheme(
                dr_src2_sid, dr_lock_bid,
                new_scheme_name="existing_scheme",
                source_scheme_id=dr_src2_sid
            )
            assert result_name.can_proceed is False, "名称冲突时 can_proceed 应为 False"
            name_risks = [r for r in result_name.risks if r.risk_type == DryRunRisk.RISK_NAME_CONFLICT]
            assert len(name_risks) >= 1, "应检测到名称冲突风险"
            assert name_risks[0].details["new_name"] == "existing_scheme"
            assert name_risks[0].details["existing_scheme_id"] == exist_sid

            result_scheme_missing = svc_dry2.dry_run_apply_scheme(99999, dr_lock_bid)
            assert result_scheme_missing.can_proceed is False, "方案不存在时 can_proceed 应为 False"
            scheme_risks = [r for r in result_scheme_missing.risks if r.risk_type == DryRunRisk.RISK_SCHEME_NOT_FOUND]
            assert len(scheme_risks) >= 1, "应检测到方案不存在风险"

            result_source_missing = svc_dry2.dry_run_apply_scheme(
                dr_src2_sid, dr_lock_bid,
                new_scheme_name="new_scheme_ok",
                source_scheme_id=99999
            )
            assert result_source_missing.can_proceed is False, "源方案不存在时 can_proceed 应为 False"
            source_risks = [r for r in result_source_missing.risks if r.risk_type == DryRunRisk.RISK_SOURCE_MISSING]
            assert len(source_risks) >= 1, "应检测到源方案缺失风险"

            result_batch_missing = svc_dry2.dry_run_apply_scheme(dr_src2_sid, 99999)
            assert result_batch_missing.can_proceed is False, "批次不存在时 can_proceed 应为 False"
            batch_risks = [r for r in result_batch_missing.risks if r.risk_type == DryRunRisk.RISK_BATCH_NOT_FOUND]
            assert len(batch_risks) >= 1, "应检测到批次不存在风险"

            blocked_logs = svc_dry2.get_scheme_audit_logs(batch_id=dr_lock_bid, result="blocked")
            assert len(blocked_logs) >= 2, "阻止的预检应记录审计日志"
            for bl in blocked_logs:
                assert bl["result"] == "blocked"
                assert bl["error_message"] is not None and len(bl["error_message"]) > 0

            r40.ok()
            print(f"  [PASS] {r40.name}  (locked/conflict/missing 全部拦截)")
        except Exception as e:
            r40.fail(str(e))
            print(f"  [FAIL] {r40.name}: {e}")

        # ====== 测试 41: 执行成功后历史可查 ======
        r41 = TestResult("测试41: apply/clone-apply/derive-apply 成功后审计日志完整可查")
        results.append(r41)
        try:
            svc_audit1 = PipelineService(db_path)

            audit1_base_bid = svc_audit1.create_batch("audit1_base", SAMPLE_CSV)
            svc_audit1.process_batch(audit1_base_bid)
            audit1_src_sid = svc_audit1.save_scheme("audit1_source", batch_id=audit1_base_bid)

            audit1_target1 = svc_audit1.create_batch("audit1_target1", SAMPLE_CSV)
            svc_audit1.process_batch(audit1_target1)
            cfg1 = svc_audit1.apply_scheme_to_batch(audit1_src_sid, audit1_target1)

            apply_logs = svc_audit1.get_scheme_audit_logs(batch_id=audit1_target1, action="apply")
            assert len(apply_logs) >= 1, "apply 成功应记录审计日志"
            al = apply_logs[0]
            assert al["action"] == "apply"
            assert al["result"] == "success"
            assert al["trigger_method"] == "cli"
            assert al["scheme_id"] == audit1_src_sid
            assert al["batch_id"] == audit1_target1
            assert al["previous_config"] is not None
            assert al["new_config"] is not None
            assert al["config_diff"] is not None
            assert al["error_message"] is None
            assert al["config_diff"]["version_change"]["new"] == cfg1["version"]

            audit1_target2 = svc_audit1.create_batch("audit1_target2", SAMPLE_CSV)
            svc_audit1.process_batch(audit1_target2)
            clone_result = svc_audit1.clone_and_apply_scheme(
                audit1_src_sid, "audit1_cloned", audit1_target2)

            clone_logs = svc_audit1.get_scheme_audit_logs(batch_id=audit1_target2, action="clone_apply")
            assert len(clone_logs) >= 1, "clone-apply 成功应记录审计日志"
            cl = clone_logs[0]
            assert cl["action"] == "clone_apply"
            assert cl["result"] == "success"
            assert cl["scheme_id"] == clone_result.cloned_scheme_id
            assert cl["source_scheme_id"] == audit1_src_sid
            assert cl["config_diff"] is not None

            audit1_target3 = svc_audit1.create_batch("audit1_target3", SAMPLE_CSV)
            svc_audit1.process_batch(audit1_target3)
            derive_result = svc_audit1.derive_and_apply_scheme(
                audit1_src_sid, "audit1_derived", audit1_target3)

            derive_logs = svc_audit1.get_scheme_audit_logs(batch_id=audit1_target3, action="derive_apply")
            assert len(derive_logs) >= 1, "derive-apply 成功应记录审计日志"
            dl = derive_logs[0]
            assert dl["action"] == "derive_apply"
            assert dl["result"] == "success"
            assert dl["scheme_id"] == derive_result.derived_scheme_id
            assert dl["source_scheme_id"] == audit1_src_sid

            all_logs = svc_audit1.get_scheme_audit_logs(scheme_id=audit1_src_sid)
            assert len(all_logs) >= 1, "按方案 ID 筛选应返回相关日志"

            success_logs = svc_audit1.get_scheme_audit_logs(result="success", limit=10)
            assert len(success_logs) >= 3, "按成功筛选应返回至少 3 条"

            r41.ok()
            print(f"  [PASS] {r41.name}  (apply/clone/derive 审计完整)")
        except Exception as e:
            r41.fail(str(e))
            print(f"  [FAIL] {r41.name}: {e}")

        # ====== 测试 42: 执行失败后历史可查 ======
        r42 = TestResult("测试42: 执行失败/阻止后审计日志可查，含错误原因，可按 result 筛选")
        results.append(r42)
        try:
            svc_audit2 = PipelineService(db_path)

            audit2_src_sid = svc_audit2.save_scheme("audit2_source", batch_id=batch_id)

            audit2_lock_bid = svc_audit2.create_batch("audit2_locked", SAMPLE_CSV)
            svc_audit2.process_batch(audit2_lock_bid)
            svc_audit2.lock_batch(audit2_lock_bid)

            try:
                svc_audit2.apply_scheme_to_batch(audit2_src_sid, audit2_lock_bid)
                assert False, "锁定批次 apply 应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            fail_logs = svc_audit2.get_scheme_audit_logs(batch_id=audit2_lock_bid, result="blocked")
            assert len(fail_logs) >= 1, "失败的 apply 应记录审计日志"
            fl = fail_logs[0]
            assert fl["action"] == "apply"
            assert fl["result"] == "blocked"
            assert fl["error_message"] is not None
            assert "锁定" in fl["error_message"] or "locked" in fl["error_message"].lower()
            assert fl["previous_config"] is not None
            assert fl.get("new_config") is None

            try:
                svc_audit2.clone_and_apply_scheme(
                    audit2_src_sid, "audit2_should_not_exist", audit2_lock_bid)
                assert False, "锁定批次 clone-apply 应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            clone_fail_logs = svc_audit2.get_scheme_audit_logs(
                batch_id=audit2_lock_bid, action="clone_apply", result="blocked")
            assert len(clone_fail_logs) >= 1, "失败的 clone-apply 应记录审计日志"
            cfl = clone_fail_logs[0]
            assert cfl["result"] == "blocked"
            assert cfl["error_message"] is not None

            try:
                svc_audit2.derive_and_apply_scheme(
                    audit2_src_sid, "audit2_derive_should_not_exist", audit2_lock_bid)
                assert False, "锁定批次 derive-apply 应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            derive_fail_logs = svc_audit2.get_scheme_audit_logs(
                batch_id=audit2_lock_bid, action="derive_apply", result="blocked")
            assert len(derive_fail_logs) >= 1, "失败的 derive-apply 应记录审计日志"

            blocked_all = svc_audit2.get_scheme_audit_logs(result="blocked")
            assert len(blocked_all) >= 3, "按 blocked 筛选应返回所有阻止记录"

            svc_audit2.unlock_batch(audit2_lock_bid)

            r42.ok()
            print(f"  [PASS] {r42.name}  (apply/clone/derive 失败均记录，含错误原因)")
        except Exception as e:
            r42.fail(str(e))
            print(f"  [FAIL] {r42.name}: {e}")

        # ====== 测试 43: 导入导出后历史连续 ======
        r43 = TestResult("测试43: 方案导出再导入后，通过 original_id 保持关联，继续应用时历史不中断")
        results.append(r43)
        try:
            svc_ie2 = PipelineService(db_path)

            ie2_src_bid = svc_ie2.create_batch("ie2_src", SAMPLE_CSV)
            svc_ie2.process_batch(ie2_src_bid)
            ie2_src_sid = svc_ie2.save_scheme("ie2_source", batch_id=ie2_src_bid, description="导入导出审计源")

            ie2_derive_sid = svc_ie2.derive_scheme(ie2_src_sid, "ie2_derive", "审计用派生方案")
            ie2_apply_bid = svc_ie2.create_batch("ie2_apply", SAMPLE_CSV)
            svc_ie2.process_batch(ie2_apply_bid)
            svc_ie2.apply_scheme_to_batch(ie2_derive_sid, ie2_apply_bid)

            ie2_export_dir = os.path.join(tmpdir, "ie2_exports")
            os.makedirs(ie2_export_dir, exist_ok=True)
            ie2_export_path = os.path.join(ie2_export_dir, "ie2_derive.json")
            svc_ie2.export_scheme_to_file(ie2_derive_sid, ie2_export_path)

            with open(ie2_export_path, "r", encoding="utf-8") as f:
                exported_data = json.load(f)
            assert exported_data.get("original_id") == ie2_derive_sid, "导出数据应包含 original_id"
            assert exported_data.get("source_scheme_id") == ie2_src_sid, "导出数据应包含 source_scheme_id"

            import_result = svc_ie2.import_scheme_from_file(
                ie2_export_path, on_conflict=SchemeImportResult.ACTION_RENAME,
                new_name="ie2_imported")
            assert import_result.success

            imported_scheme = svc_ie2.get_scheme(import_result.scheme_id)
            assert imported_scheme["original_id"] == ie2_derive_sid, "导入后应保留 original_id"
            assert imported_scheme["source_scheme_id"] == ie2_src_sid, "导入后应保留 source_scheme_id"
            assert imported_scheme["imported_from"] is not None, "导入后应记录 imported_from"

            found_by_original = svc_ie2.get_scheme_by_original_id(ie2_derive_sid)
            assert found_by_original is not None, "应能通过 original_id 找到导入的方案"
            assert found_by_original["id"] == import_result.scheme_id

            ie2_apply2_bid = svc_ie2.create_batch("ie2_apply2", SAMPLE_CSV)
            svc_ie2.process_batch(ie2_apply2_bid)
            cfg_ie2 = svc_ie2.apply_scheme_to_batch(import_result.scheme_id, ie2_apply2_bid)

            ie2_logs = svc_ie2.get_scheme_audit_logs(batch_id=ie2_apply2_bid)
            assert len(ie2_logs) >= 1, "导入方案的应用应记录审计日志"
            ie2_audit = ie2_logs[0]
            assert ie2_audit["scheme_id"] == import_result.scheme_id
            assert ie2_audit["source_scheme_id"] == ie2_src_sid

            original_logs = svc_ie2.get_scheme_audit_logs(scheme_id=ie2_derive_sid)
            new_logs = svc_ie2.get_scheme_audit_logs(scheme_id=import_result.scheme_id)
            assert len(original_logs) >= 1 and len(new_logs) >= 1, "原始和导入方案的历史都可查询"

            r43.ok()
            print(f"  [PASS] {r43.name}  (original_id={ie2_derive_sid}, imported_id={import_result.scheme_id}, history_continuous)")
        except Exception as e:
            r43.fail(str(e))
            print(f"  [FAIL] {r43.name}: {e}")

        # ====== 测试 44: 跨重启审计持久化 ======
        r44 = TestResult("测试44: 重启后审计日志完整保留，可正常查询")
        results.append(r44)
        try:
            svc_restart_audit = PipelineService(db_path)

            restart_audit_bid = svc_restart_audit.create_batch("restart_audit", SAMPLE_CSV)
            svc_restart_audit.process_batch(restart_audit_bid)
            restart_audit_sid = svc_restart_audit.save_scheme("restart_audit_scheme", batch_id=restart_audit_bid)
            svc_restart_audit.apply_scheme_to_batch(restart_audit_sid, restart_audit_bid)
            svc_restart_audit.dry_run_apply_scheme(restart_audit_sid, restart_audit_bid)

            before_logs = svc_restart_audit.get_scheme_audit_logs(batch_id=restart_audit_bid)
            before_count = len(before_logs)
            assert before_count >= 2, "重启前应有至少 2 条审计记录（apply + dry_run）"

            del svc_restart_audit

            svc_restart_audit2 = PipelineService(db_path)

            after_logs = svc_restart_audit2.get_scheme_audit_logs(batch_id=restart_audit_bid)
            after_count = len(after_logs)
            assert after_count == before_count, f"重启后审计记录数应不变，before={before_count}, after={after_count}"

            apply_log = [l for l in after_logs if l["action"] == "apply"]
            assert len(apply_log) == 1, "重启后 apply 记录仍存在"
            assert apply_log[0]["result"] == "success"

            dryrun_log = [l for l in after_logs if l["action"] == "dry_run"]
            assert len(dryrun_log) == 1, "重启后 dry_run 记录仍存在"

            r44.ok()
            print(f"  [PASS] {r44.name}  (audit_count={after_count}, persist_ok)")
        except Exception as e:
            r44.fail(str(e))
            print(f"  [FAIL] {r44.name}: {e}")

        # ====== 测试 45: CLI dry-run / audit-history 输出对齐 ======
        r45 = TestResult("测试45: CLI scheme dry-run / audit-history 命令输出与 README 文档对齐")
        results.append(r45)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli
            import logging

            runner = CliRunner()
            svc_cli3 = PipelineService(db_path)

            cli_dry_bid = svc_cli3.create_batch("cli_dry_batch", SAMPLE_CSV)
            svc_cli3.process_batch(cli_dry_bid)
            cli_dry_sid = svc_cli3.save_scheme("cli_dry_scheme", batch_id=cli_dry_bid)

            res_help1 = runner.invoke(cli, ["scheme", "--help"])
            assert "dry-run" in res_help1.output, "scheme --help 应包含 dry-run"
            assert "audit-history" in res_help1.output, "scheme --help 应包含 audit-history"

            res_dry_help = runner.invoke(cli, ["scheme", "dry-run", "--help"])
            assert res_dry_help.exit_code == 0
            assert "SCHEME_ID" in res_dry_help.output
            assert "BATCH_ID" in res_dry_help.output
            assert "--new-name" in res_dry_help.output
            assert "--source-scheme-id" in res_dry_help.output

            res_audit_help = runner.invoke(cli, ["scheme", "audit-history", "--help"])
            assert res_audit_help.exit_code == 0
            assert "BATCH_ID" in res_audit_help.output
            assert "--scheme-id" in res_audit_help.output
            assert "--action" in res_audit_help.output
            assert "--result" in res_audit_help.output
            assert "--diff" in res_audit_help.output

            res_dry_ok = runner.invoke(cli, ["--db", db_path, "scheme", "dry-run",
                                             str(cli_dry_sid), str(cli_dry_bid)])
            assert res_dry_ok.exit_code == 0, f"dry-run 成功应 exit=0，实际 {res_dry_ok.exit_code}"
            assert "[OK]" in res_dry_ok.output, "dry-run 成功输出应包含 [OK]"
            assert "Dry-Run 检查结果" in res_dry_ok.output, "输出应包含标题"
            assert "目标批次" in res_dry_ok.output, "输出应包含目标批次"
            assert "待应用方案" in res_dry_ok.output, "输出应包含待应用方案"
            assert "风险数量" in res_dry_ok.output, "输出应包含风险数量"
            assert "配置变更预览" in res_dry_ok.output, "预检成功应包含配置变更预览"
            assert "版本变化" in res_dry_ok.output, "应包含版本变化"

            svc_cli3.lock_batch(cli_dry_bid)
            res_dry_blocked = runner.invoke(cli, ["--db", db_path, "scheme", "dry-run",
                                                  str(cli_dry_sid), str(cli_dry_bid)])
            assert res_dry_blocked.exit_code != 0, "dry-run 阻止应 exit!=0"
            assert "[ERROR]" in res_dry_blocked.output, "阻止输出应包含 [ERROR]"
            assert "检查未通过" in res_dry_blocked.output, "应说明检查未通过"
            assert "风险详情" in res_dry_blocked.output, "应包含风险详情"
            assert "锁定" in res_dry_blocked.output, "应说明锁定原因"

            svc_cli3.unlock_batch(cli_dry_bid)
            svc_cli3.apply_scheme_to_batch(cli_dry_sid, cli_dry_bid)

            res_audit = runner.invoke(cli, ["--db", db_path, "scheme", "audit-history",
                                            str(cli_dry_bid)])
            assert res_audit.exit_code == 0
            assert "ID" in res_audit.output
            assert "时间" in res_audit.output
            assert "操作" in res_audit.output
            assert "触发" in res_audit.output
            assert "批次" in res_audit.output
            assert "方案" in res_audit.output
            assert "结果" in res_audit.output

            res_audit_diff = runner.invoke(cli, ["--db", db_path, "scheme", "audit-history",
                                                 str(cli_dry_bid), "--diff"])
            assert res_audit_diff.exit_code == 0
            assert "配置差异详情" in res_audit_diff.output, "--diff 应显示配置差异"

            res_audit_filter = runner.invoke(cli, ["--db", db_path, "scheme", "audit-history",
                                                   str(cli_dry_bid), "--result", "success",
                                                   "--action", "apply"])
            assert res_audit_filter.exit_code == 0

            r45.ok()
            print(f"  [PASS] {r45.name}  (CLI help + success + blocked + audit 全部对齐)")
        except Exception as e:
            r45.fail(str(e))
            print(f"  [FAIL] {r45.name}: {e}")

        # ====== 测试 46: 回滚审计记录完整（成功/失败/阻止三种场景） ======
        r46 = TestResult("测试46: 回滚成功/失败/阻止三种场景审计日志完整，含配置差异")
        results.append(r46)
        try:
            svc_rb = PipelineService(db_path)

            rb_base_bid = svc_rb.create_batch("rb46_base", SAMPLE_CSV)
            svc_rb.process_batch(rb_base_bid)
            rb_src_sid = svc_rb.save_scheme("rb46_source", batch_id=rb_base_bid)

            rb_bid = svc_rb.create_batch("rb46_target", SAMPLE_CSV)
            svc_rb.process_batch(rb_bid)
            rb_cfg1 = svc_rb.apply_scheme_to_batch(rb_src_sid, rb_bid)

            rb_result = svc_rb.rollback_scheme(rb_bid)
            assert rb_result.success, "回滚应成功"

            rb_success_logs = svc_rb.get_scheme_audit_logs(
                batch_id=rb_bid, action="rollback", result="success")
            assert len(rb_success_logs) >= 1, "成功回滚应记录审计日志"
            rbsl = rb_success_logs[0]
            assert rbsl["action"] == "rollback"
            assert rbsl["result"] == "success"
            assert rbsl["trigger_method"] == "cli"
            assert rbsl["previous_config"] is not None
            assert rbsl["new_config"] is not None
            assert rbsl["config_diff"] is not None
            assert rbsl["error_message"] is None
            assert rbsl["config_diff"]["version_change"]["new"] == rb_result.new_config_version
            assert rb_result.config_diff is not None, "rollback 返回值应含 config_diff"

            rb_lock_bid = svc_rb.create_batch("rb46_lock_target", SAMPLE_CSV)
            svc_rb.process_batch(rb_lock_bid)
            svc_rb.apply_scheme_to_batch(rb_src_sid, rb_lock_bid)
            svc_rb.lock_batch(rb_lock_bid)

            try:
                svc_rb.rollback_scheme(rb_lock_bid)
                assert False, "锁定回滚应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            rb_blocked_logs = svc_rb.get_scheme_audit_logs(
                batch_id=rb_lock_bid, action="rollback", result="blocked")
            assert len(rb_blocked_logs) >= 1, "锁定回滚阻止应记录审计日志"
            rbbl = rb_blocked_logs[0]
            assert rbbl["result"] == "blocked"
            assert rbbl["error_message"] is not None
            assert "锁定" in rbbl["error_message"]
            assert rbbl["previous_config"] is not None
            assert rbbl.get("new_config") is None

            svc_rb.unlock_batch(rb_lock_bid)

            rb_no_hist_bid = svc_rb.create_batch("rb46_no_hist", SAMPLE_CSV)
            svc_rb.process_batch(rb_no_hist_bid)
            rb_no_hist_result = svc_rb.rollback_scheme(rb_no_hist_bid)
            assert not rb_no_hist_result.success, "无历史回滚应失败"

            rb_failed_logs = svc_rb.get_scheme_audit_logs(
                batch_id=rb_no_hist_bid, action="rollback", result="failed")
            assert len(rb_failed_logs) >= 1, "失败回滚应记录审计日志"
            rbfl = rb_failed_logs[0]
            assert rbfl["result"] == "failed"
            assert rbfl["error_message"] is not None
            assert ("历史记录" in rbfl["error_message"] or "最早" in rbfl["error_message"])

            r46.ok()
            print(f"  [PASS] {r46.name}  (成功/阻止/失败 3 场景均有审计)")
        except Exception as e:
            r46.fail(str(e))
            print(f"  [FAIL] {r46.name}: {e}")

        # ====== 测试 47: switch_scheme 四种模式（apply/clone/derive/rollback）流水完整 ======
        r47 = TestResult("测试47: switch_scheme apply/clone/derive/rollback 四种模式预检→执行流水完整")
        results.append(r47)
        try:
            svc_sw = PipelineService(db_path)

            sw_base_bid = svc_sw.create_batch("sw_base", SAMPLE_CSV)
            svc_sw.process_batch(sw_base_bid)
            sw_src_sid = svc_sw.save_scheme("sw_source", batch_id=sw_base_bid)

            # 1) apply 模式
            sw_apply_bid = svc_sw.create_batch("sw_apply", SAMPLE_CSV)
            svc_sw.process_batch(sw_apply_bid)
            sw_apply_dr = svc_sw.switch_scheme(
                SwitchSchemeResult.SWITCH_TYPE_APPLY, sw_apply_bid,
                scheme_id=sw_src_sid, dry_run_only=True)
            assert sw_apply_dr.success == sw_apply_dr.dry_run.can_proceed
            assert sw_apply_dr.dry_run is not None
            assert sw_apply_dr.switch_type == SwitchSchemeResult.SWITCH_TYPE_APPLY

            sw_apply_ex = svc_sw.switch_scheme(
                SwitchSchemeResult.SWITCH_TYPE_APPLY, sw_apply_bid,
                scheme_id=sw_src_sid, dry_run_only=False)
            assert sw_apply_ex.success
            assert sw_apply_ex.new_config is not None
            assert sw_apply_ex.new_config_version > sw_apply_ex.dry_run.current_config_version

            # 2) clone 模式
            sw_clone_bid = svc_sw.create_batch("sw_clone", SAMPLE_CSV)
            svc_sw.process_batch(sw_clone_bid)
            sw_clone_result = svc_sw.switch_scheme(
                SwitchSchemeResult.SWITCH_TYPE_CLONE, sw_clone_bid,
                source_scheme_id=sw_src_sid, new_scheme_name="sw_cloned_scheme")
            assert sw_clone_result.success
            assert sw_clone_result.new_scheme_id is not None
            assert sw_clone_result.new_scheme_name == "sw_cloned_scheme"
            assert sw_clone_result.new_config is not None

            # 3) derive 模式
            sw_derive_bid = svc_sw.create_batch("sw_derive", SAMPLE_CSV)
            svc_sw.process_batch(sw_derive_bid)
            sw_derive_result = svc_sw.switch_scheme(
                SwitchSchemeResult.SWITCH_TYPE_DERIVE, sw_derive_bid,
                source_scheme_id=sw_src_sid, new_scheme_name="sw_derived_scheme")
            assert sw_derive_result.success
            assert sw_derive_result.new_scheme_id is not None

            # 4) rollback 模式
            sw_rb_result = svc_sw.switch_scheme(
                SwitchSchemeResult.SWITCH_TYPE_ROLLBACK, sw_apply_bid)
            assert sw_rb_result.success
            assert sw_rb_result.rollback_result is not None
            assert sw_rb_result.rollback_result.success
            assert sw_rb_result.rollback_result.previous_config_version == sw_apply_ex.new_config["version"]

            # 5) 预检拦截：apply 到锁定批次
            sw_block_bid = svc_sw.create_batch("sw_block", SAMPLE_CSV)
            svc_sw.process_batch(sw_block_bid)
            svc_sw.lock_batch(sw_block_bid)
            sw_block_result = svc_sw.switch_scheme(
                SwitchSchemeResult.SWITCH_TYPE_APPLY, sw_block_bid,
                scheme_id=sw_src_sid, dry_run_only=True)
            assert not sw_block_result.success
            assert not sw_block_result.dry_run.can_proceed
            lock_risk = [r for r in sw_block_result.dry_run.risks
                         if r.risk_type == DryRunRisk.RISK_LOCKED]
            assert len(lock_risk) >= 1

            svc_sw.unlock_batch(sw_block_bid)

            # 6) 4 种模式均有审计日志
            for (bid, act) in [(sw_apply_bid, "apply"), (sw_clone_bid, "clone_apply"),
                               (sw_derive_bid, "derive_apply"), (sw_apply_bid, "rollback")]:
                logs = svc_sw.get_scheme_audit_logs(batch_id=bid, action=act, result="success")
                assert len(logs) >= 1, f"switch {act} 应有审计日志"

            r47.ok()
            print(f"  [PASS] {r47.name}  (apply/clone/derive/rollback + 拦截 + 审计)")
        except Exception as e:
            r47.fail(str(e))
            print(f"  [FAIL] {r47.name}: {e}")

        # ====== 测试 48: 回滚前后结果变化正确（配置版本/当前方案/配置值） ======
        r48 = TestResult("测试48: 回滚前后配置版本、当前方案、配置值正确变化")
        results.append(r48)
        try:
            svc_rb2 = PipelineService(db_path)

            rb2_base_bid = svc_rb2.create_batch("rb48_base", SAMPLE_CSV)
            svc_rb2.process_batch(rb2_base_bid)
            rb2_scheme1_sid = svc_rb2.save_scheme("rb48_scheme1", batch_id=rb2_base_bid)

            rb2_target_bid = svc_rb2.create_batch("rb48_target", SAMPLE_CSV)
            svc_rb2.process_batch(rb2_target_bid)
            rb2_before_cfg = svc_rb2.get_batch(rb2_target_bid)
            rb2_before_version = json.loads(rb2_before_cfg["config_json"])["version"]
            rb2_before_scheme_id = rb2_before_cfg.get("current_scheme_id")

            svc_rb2.apply_scheme_to_batch(rb2_scheme1_sid, rb2_target_bid)
            rb2_after_apply_cfg = svc_rb2.get_batch(rb2_target_bid)
            rb2_after_apply_json = json.loads(rb2_after_apply_cfg["config_json"])
            rb2_after_apply_version = rb2_after_apply_json["version"]
            rb2_after_apply_scheme_id = rb2_after_apply_cfg["current_scheme_id"]
            rb2_after_apply_threshold = rb2_after_apply_json["anomaly_detection"]["zscore_threshold"]
            assert rb2_after_apply_version > rb2_before_version
            assert rb2_after_apply_scheme_id == rb2_scheme1_sid

            rb2_rollback_result = svc_rb2.rollback_scheme(rb2_target_bid)
            assert rb2_rollback_result.success
            assert rb2_rollback_result.new_config_version > rb2_after_apply_version
            assert rb2_rollback_result.previous_config_version == rb2_after_apply_version
            assert rb2_rollback_result.previous_scheme_id == rb2_scheme1_sid

            rb2_after_rb_cfg = svc_rb2.get_batch(rb2_target_bid)
            rb2_after_rb_json = json.loads(rb2_after_rb_cfg["config_json"])
            assert rb2_after_rb_json["version"] == rb2_rollback_result.new_config_version
            assert rb2_after_rb_cfg["current_scheme_id"] == rb2_before_scheme_id
            assert rb2_after_rb_cfg["current_scheme_name"] == rb2_before_cfg.get("current_scheme_name")

            rb2_rb_audit = svc_rb2.get_scheme_audit_logs(
                batch_id=rb2_target_bid, action="rollback", result="success")
            assert len(rb2_rb_audit) >= 1
            cd = rb2_rb_audit[0]["config_diff"]
            assert cd["version_change"]["old"] == rb2_after_apply_version
            assert cd["version_change"]["new"] == rb2_rollback_result.new_config_version

            r48.ok()
            print(f"  [PASS] {r48.name}  (v{rb2_before_version}→v{rb2_after_apply_version}→v{rb2_rollback_result.new_config_version})")
        except Exception as e:
            r48.fail(str(e))
            print(f"  [FAIL] {r48.name}: {e}")

        # ====== 测试 49: rollback dry-run 拦截生效（锁定/无历史/批次不存在） ======
        r49 = TestResult("测试49: rollback dry-run 预检拦截（锁定批次、无历史、批次不存在）")
        results.append(r49)
        try:
            svc_rb3 = PipelineService(db_path)

            # 锁定批次
            rb3_lock_bid = svc_rb3.create_batch("rb3_lock", SAMPLE_CSV)
            svc_rb3.process_batch(rb3_lock_bid)
            rb3_lock_sid = svc_rb3.save_scheme("rb3_lock_scheme", batch_id=rb3_lock_bid)
            svc_rb3.apply_scheme_to_batch(rb3_lock_sid, rb3_lock_bid)
            svc_rb3.lock_batch(rb3_lock_bid)

            dr1 = svc_rb3.dry_run_rollback_scheme(rb3_lock_bid)
            assert not dr1.can_proceed, "锁定批次 rollback dry-run 应阻止"
            lock_risks = [r for r in dr1.risks if r.risk_type == DryRunRisk.RISK_LOCKED]
            assert len(lock_risks) >= 1

            svc_rb3.unlock_batch(rb3_lock_bid)

            # 无历史（无 scheme_history）
            rb3_nohist_bid = svc_rb3.create_batch("rb3_nohist", SAMPLE_CSV)
            svc_rb3.process_batch(rb3_nohist_bid)
            dr2 = svc_rb3.dry_run_rollback_scheme(rb3_nohist_bid)
            assert not dr2.can_proceed, "无历史时 rollback dry-run 应阻止"

            # 批次不存在
            dr3 = svc_rb3.dry_run_rollback_scheme(999999)
            assert not dr3.can_proceed, "批次不存在时 rollback dry-run 应阻止"
            not_found_risks = [r for r in dr3.risks if r.risk_type == DryRunRisk.RISK_BATCH_NOT_FOUND]
            assert len(not_found_risks) >= 1

            # 正常可回滚：先 apply 方案再 set_threshold，rollback 目标就是 apply 记录（含方案）
            rb3_ok_bid = svc_rb3.create_batch("rb3_ok", SAMPLE_CSV)
            svc_rb3.process_batch(rb3_ok_bid)
            rb3_ok_sid = svc_rb3.save_scheme("rb3_ok_scheme", batch_id=rb3_ok_bid)
            svc_rb3.apply_scheme_to_batch(rb3_ok_sid, rb3_ok_bid)
            svc_rb3.set_threshold(rb3_ok_bid, zscore_threshold=2.5)
            dr4 = svc_rb3.dry_run_rollback_scheme(rb3_ok_bid)
            assert dr4.can_proceed, "有历史时 rollback dry-run 应通过"
            assert dr4.config_diff is not None
            assert dr4.new_config_version is not None
            assert dr4.current_config_version is not None
            assert dr4.scheme_id is not None or dr4.scheme_name is not None

            r49.ok()
            print(f"  [PASS] {r49.name}  (锁定/无历史/批次不存在 3 种场景均拦截，有历史时通过)")
        except Exception as e:
            r49.fail(str(e))
            print(f"  [FAIL] {r49.name}: {e}")

        # ====== 测试 50: CLI switch、rollback --dry-run、rollback-dry-run 输出对齐 ======
        r50 = TestResult("测试50: CLI scheme switch / rollback --dry-run / rollback-dry-run 输出对齐")
        results.append(r50)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli
            runner = CliRunner()
            svc_cli5 = PipelineService(db_path)

            cli5_bid = svc_cli5.create_batch("cli5_switch", SAMPLE_CSV)
            svc_cli5.process_batch(cli5_bid)
            cli5_sid = svc_cli5.save_scheme("cli5_scheme", batch_id=cli5_bid)

            sw_help = runner.invoke(cli, ["scheme", "switch", "--help"])
            assert sw_help.exit_code == 0
            assert "SWITCH_TYPE" in sw_help.output
            assert "apply" in sw_help.output
            assert "rollback" in sw_help.output
            assert "--dry-run" in sw_help.output

            sw_apply_dr = runner.invoke(cli, ["--db", db_path, "scheme", "switch",
                                              "apply", str(cli5_bid),
                                              "--scheme-id", str(cli5_sid), "--dry-run"])
            assert sw_apply_dr.exit_code == 0
            assert "方案切换" in sw_apply_dr.output
            assert "[预检]" in sw_apply_dr.output
            assert "目标批次" in sw_apply_dr.output
            assert "待应用方案" in sw_apply_dr.output
            assert "预检通过" in sw_apply_dr.output

            sw_apply_ex = runner.invoke(cli, ["--db", db_path, "scheme", "switch",
                                              "apply", str(cli5_bid),
                                              "--scheme-id", str(cli5_sid)])
            assert sw_apply_ex.exit_code == 0
            assert "切换成功" in sw_apply_ex.output
            assert "配置变更预览" in sw_apply_ex.output
            assert "process 命令" in sw_apply_ex.output

            rb_dry = runner.invoke(cli, ["--db", db_path, "scheme", "rollback",
                                         str(cli5_bid), "--dry-run"])
            assert rb_dry.exit_code == 0
            assert "[预检]" in rb_dry.output or "预检通过" in rb_dry.output or "方案切换" in rb_dry.output
            assert "rollback" in rb_dry.output.lower() or "回滚" in rb_dry.output

            rb_dry_cmd = runner.invoke(cli, ["--db", db_path, "scheme", "rollback-dry-run",
                                              str(cli5_bid)])
            assert rb_dry_cmd.exit_code == 0
            assert "Dry-Run" in rb_dry_cmd.output or "预检结果" in rb_dry_cmd.output

            rb_exec = runner.invoke(cli, ["--db", db_path, "scheme", "rollback",
                                           str(cli5_bid)])
            assert rb_exec.exit_code == 0
            assert "切换成功" in rb_exec.output or "回滚详情" in rb_exec.output or "rollback" in rb_exec.output.lower()

            svc_cli5.lock_batch(cli5_bid)
            rb_blocked = runner.invoke(cli, ["--db", db_path, "scheme", "rollback",
                                              str(cli5_bid), "--dry-run"])
            assert rb_blocked.exit_code != 0, "锁定时 rollback --dry-run 应 exit!=0"
            assert "检查未通过" in rb_blocked.output or "预检未通过" in rb_blocked.output or "ERROR" in rb_blocked.output
            svc_cli5.unlock_batch(cli5_bid)

            r50.ok()
            print(f"  [PASS] {r50.name}  (switch/rollback 6 种 CLI 调用全部对齐)")
        except Exception as e:
            r50.fail(str(e))
            print(f"  [FAIL] {r50.name}: {e}")

        # ====== 测试 51: 锁定批次 dry-run 编码容错（UnicodeEncodeError 根因复现） ======
        r51 = TestResult("测试51: 锁定批次 dry-run 编码容错（GBK 下不抛 UnicodeEncodeError）")
        results.append(r51)
        try:
            from click.testing import CliRunner
            from pipeline import cli as cli_mod

            svc_cli6 = PipelineService(db_path)
            cli6_bid = svc_cli6.create_batch("cli6_enc_lock", SAMPLE_CSV)
            svc_cli6.process_batch(cli6_bid)
            cli6_sid = svc_cli6.save_scheme("cli6_enc_scheme", batch_id=cli6_bid)
            svc_cli6.apply_scheme_to_batch(cli6_sid, cli6_bid)
            svc_cli6.lock_batch(cli6_bid)

            runner6 = CliRunner()

            # 场景 1: 正常调用 CliRunner（UTF-8），验证锁定拦截
            dr_normal = runner6.invoke(cli, ["--db", db_path, "scheme", "dry-run",
                                             str(cli6_sid), str(cli6_bid)])
            assert dr_normal.exit_code != 0, "锁定时 dry-run 应 exit!=0"
            assert "[ERROR]" in dr_normal.output, "锁定 dry-run 输出应含 [ERROR]"
            assert "锁定" in dr_normal.output, "锁定 dry-run 输出应含锁定信息"
            assert "风险详情" in dr_normal.output, "锁定 dry-run 输出应含风险详情"
            assert "批次已锁定" in dr_normal.output, "锁定 dry-run 输出应含批次已锁定"

            # 场景 2: 模拟 GBK 编码环境，注入 ⚠ 字符触发 UnicodeEncodeError，验证容错
            saved_stdout_encoding = None
            saved_stderr_encoding = None
            original_stdout = sys.stdout
            original_stderr = sys.stderr
            try:
                import io
                fake_gbk_stdout = io.TextIOWrapper(
                    io.BytesIO(), encoding="gbk", errors="strict", write_through=True
                )
                fake_gbk_stderr = io.TextIOWrapper(
                    io.BytesIO(), encoding="gbk", errors="strict", write_through=True
                )

                sys.stdout = fake_gbk_stdout
                sys.stderr = fake_gbk_stderr

                try:
                    fake_gbk_stdout.write("\u26a0 测试危险字符")
                    fake_gbk_stdout.flush()
                    raise RuntimeError("未触发 UnicodeEncodeError，GBK mock 失败")
                except UnicodeEncodeError:
                    pass

                cli_mod._configure_terminal_encoding()

                try:
                    fake_gbk_stdout.write("\u26a0 测试危险字符")
                    fake_gbk_stdout.flush()
                    encode_ok = True
                except UnicodeEncodeError:
                    encode_ok = False
                assert encode_ok, "_configure_terminal_encoding 后 Unicode 字符不应抛异常"

            finally:
                sys.stdout = original_stdout
                sys.stderr = original_stderr

            # 场景 3: 锁定批次 dry-run 在编码容错环境下完整输出
            dr_encoded = runner6.invoke(cli, ["--db", db_path, "scheme", "dry-run",
                                              str(cli6_sid), str(cli6_bid)])
            assert dr_encoded.exit_code != 0
            assert "[ERROR]" in dr_encoded.output
            assert "批次已锁定" in dr_encoded.output or "[!]" in dr_encoded.output
            assert "风险数量" in dr_encoded.output or "风险详情" in dr_encoded.output

            svc_cli6.unlock_batch(cli6_bid)

            r51.ok()
            print(f"  [PASS] {r51.name}  (编码容错生效，Unicode 字符不崩)")
        except Exception as e:
            r51.fail(str(e))
            print(f"  [FAIL] {r51.name}: {e}")

        # ====== 测试 52: 锁定批次 dry-run 完整行为 + 正常链路未受影响 ======
        r52 = TestResult("测试52: 锁定 dry-run 拦截完整行为 + 正常链路未退化")
        results.append(r52)
        try:
            from click.testing import CliRunner
            runner7 = CliRunner()

            svc_cli7 = PipelineService(db_path)

            # Part A: 正常链路 dry-run（不锁定）- 验证正常通过
            cli7_ok_bid = svc_cli7.create_batch("cli7_ok", SAMPLE_CSV)
            svc_cli7.process_batch(cli7_ok_bid)
            cli7_ok_sid = svc_cli7.save_scheme("cli7_ok_scheme", batch_id=cli7_ok_bid)

            dr_ok = runner7.invoke(cli, ["--db", db_path, "scheme", "dry-run",
                                         str(cli7_ok_sid), str(cli7_ok_bid)])
            assert dr_ok.exit_code == 0, f"正常 dry-run 应 exit=0，实际={dr_ok.exit_code}: {dr_ok.output}"
            assert "[OK]" in dr_ok.output or "检查通过" in dr_ok.output or "预检通过" in dr_ok.output
            assert "目标批次" in dr_ok.output
            assert "待应用方案" in dr_ok.output
            assert "配置变更预览" in dr_ok.output or "风险数量: 0" in dr_ok.output
            assert "UnicodeEncodeError" not in dr_ok.output

            # Part B: 锁定后 dry-run - 验证被拦截、退出码正确、信息完整
            svc_cli7.lock_batch(cli7_ok_bid)
            dr_blocked = runner7.invoke(cli, ["--db", db_path, "scheme", "dry-run",
                                               str(cli7_ok_sid), str(cli7_ok_bid)])
            assert dr_blocked.exit_code != 0, f"锁定 dry-run 应 exit!=0，实际={dr_blocked.exit_code}"
            assert "[ERROR]" in dr_blocked.output, "锁定 dry-run 应输出 [ERROR]"
            assert "检查未通过" in dr_blocked.output or "预检未通过" in dr_blocked.output
            assert "锁定" in dr_blocked.output, "锁定 dry-run 应说明锁定原因"
            assert "风险详情" in dr_blocked.output, "锁定 dry-run 应展示风险详情"
            assert "风险数量: 1" in dr_blocked.output, "锁定 dry-run 应有 1 条风险"
            assert "[!]" in dr_blocked.output or "批次已锁定" in dr_blocked.output
            assert "UnicodeEncodeError" not in dr_blocked.output

            svc_cli7.unlock_batch(cli7_ok_bid)

            r52.ok()
            print(f"  [PASS] {r52.name}  (正常链路未退化，锁定拦截完整)")
        except Exception as e:
            r52.fail(str(e))
            print(f"  [FAIL] {r52.name}: {e}")

        # ====== 测试 53: 导入冲突追溯（rename 落地后 original_name/final_name/original_id/imported_from 齐全） ======
        r53 = TestResult("测试53: 导入 rename 冲突后追溯字段齐全（original_name/final_name/original_id/imported_from）")
        results.append(r53)
        try:
            svc_imp1 = PipelineService(db_path)

            imp1_bid = svc_imp1.create_batch("imp1_batch", SAMPLE_CSV)
            svc_imp1.process_batch(imp1_bid)
            imp1_sid = svc_imp1.save_scheme("imp1_scheme", batch_id=imp1_bid, description="导入追溯源")

            imp1_dir = os.path.join(tmpdir, "imp1_exports")
            os.makedirs(imp1_dir, exist_ok=True)
            imp1_path = os.path.join(imp1_dir, "imp1_scheme.json")
            svc_imp1.export_scheme_to_file(imp1_sid, imp1_path)

            res_rename = svc_imp1.import_scheme_from_file(
                imp1_path, on_conflict=SchemeImportResult.ACTION_RENAME,
                new_name="imp1_renamed")
            assert res_rename.success
            assert res_rename.action == SchemeImportResult.ACTION_RENAME
            assert res_rename.original_name == "imp1_scheme"
            assert res_rename.final_name == "imp1_renamed"
            assert res_rename.original_id == imp1_sid
            assert res_rename.imported_from is not None
            assert "imp1_scheme.json" in res_rename.imported_from or "imp1_exports" in res_rename.imported_from

            imported_scheme = svc_imp1.get_scheme(res_rename.scheme_id)
            assert imported_scheme["name"] == "imp1_renamed"
            assert imported_scheme["original_id"] == imp1_sid
            assert imported_scheme["imported_from"] is not None

            res_overwrite = svc_imp1.import_scheme_from_file(
                imp1_path, on_conflict=SchemeImportResult.ACTION_OVERWRITE)
            assert res_overwrite.success
            assert res_overwrite.original_name == "imp1_scheme"
            assert res_overwrite.final_name == "imp1_scheme"
            assert res_overwrite.original_id == imp1_sid

            res_skip = svc_imp1.import_scheme_from_file(
                imp1_path, on_conflict=SchemeImportResult.ACTION_SKIP)
            assert not res_skip.success
            assert res_skip.original_name == "imp1_scheme"
            assert res_skip.final_name is None
            assert res_skip.original_id == imp1_sid

            import_audit = svc_imp1.get_scheme_audit_logs(action="import")
            assert len(import_audit) >= 3, f"导入操作应记录审计日志，实际 {len(import_audit)} 条"
            for al in import_audit:
                assert al["action"] == "import"
                assert al["trigger_method"] == "import"

            r53.ok()
            print(f"  [PASS] {r53.name}  (rename/overwrite/skip 追溯字段+审计日志齐全)")
        except Exception as e:
            r53.fail(str(e))
            print(f"  [FAIL] {r53.name}: {e}")

        # ====== 测试 54: 导入后跨重启查询（方案和审计日志持久化） ======
        r54 = TestResult("测试54: 导入方案后跨重启查询，方案/审计日志/original_id 保留")
        results.append(r54)
        try:
            svc_pre = PipelineService(db_path)

            pre_imp_bid = svc_pre.create_batch("pre_imp_batch", SAMPLE_CSV)
            svc_pre.process_batch(pre_imp_bid)
            pre_imp_sid = svc_pre.save_scheme("pre_imp_scheme", batch_id=pre_imp_bid)

            pre_imp_dir = os.path.join(tmpdir, "pre_imp_exports")
            os.makedirs(pre_imp_dir, exist_ok=True)
            pre_imp_path = os.path.join(pre_imp_dir, "pre_imp.json")
            svc_pre.export_scheme_to_file(pre_imp_sid, pre_imp_path)

            res = svc_pre.import_scheme_from_file(
                pre_imp_path, on_conflict=SchemeImportResult.ACTION_RENAME,
                new_name="pre_imp_imported")
            assert res.success
            imported_id = res.scheme_id

            pre_audit = svc_pre.get_scheme_audit_logs(action="import")

            del svc_pre

            svc_post = PipelineService(db_path)

            post_scheme = svc_post.get_scheme(imported_id)
            assert post_scheme is not None, "重启后导入方案应存在"
            assert post_scheme["name"] == "pre_imp_imported"
            assert post_scheme["original_id"] == pre_imp_sid
            assert post_scheme["imported_from"] is not None

            post_audit = svc_post.get_scheme_audit_logs(action="import")
            assert len(post_audit) == len(pre_audit), "重启后导入审计日志数量应不变"

            r54.ok()
            print(f"  [PASS] {r54.name}  (imported_id={imported_id}, original_id={pre_imp_sid})")
        except Exception as e:
            r54.fail(str(e))
            print(f"  [FAIL] {r54.name}: {e}")

        # ====== 测试 55: import_and_apply_scheme 一步链路 ======
        r55 = TestResult("测试55: import_and_apply_scheme 一步导入并应用，审计日志含 import_apply")
        results.append(r55)
        try:
            svc_ia = PipelineService(db_path)

            ia_src_bid = svc_ia.create_batch("ia_src_batch", SAMPLE_CSV)
            svc_ia.process_batch(ia_src_bid)
            ia_src_sid = svc_ia.save_scheme("ia_source", batch_id=ia_src_bid, description="导入应用源")

            ia_dir = os.path.join(tmpdir, "ia_exports")
            os.makedirs(ia_dir, exist_ok=True)
            ia_path = os.path.join(ia_dir, "ia_source.json")
            svc_ia.export_scheme_to_file(ia_src_sid, ia_path)

            with open(ia_path, "r", encoding="utf-8") as f:
                exported = json.load(f)
            exported["config"]["anomaly_detection"]["zscore_threshold"] = 0.8
            modified_path = os.path.join(ia_dir, "ia_modified.json")
            with open(modified_path, "w", encoding="utf-8") as f:
                json.dump(exported, f, ensure_ascii=False)

            ia_target_bid = svc_ia.create_batch("ia_target_batch", SAMPLE_CSV)
            svc_ia.process_batch(ia_target_bid)

            result = svc_ia.import_and_apply_scheme(modified_path, ia_target_bid,
                                                     on_conflict=SchemeImportResult.ACTION_RENAME,
                                                     new_name="ia_imported_applied")
            assert result["import_result"].success
            assert result["apply_config"] is not None
            assert result["import_result"].final_name == "ia_imported_applied"
            assert result["import_result"].original_name == "ia_source"

            batch_after = svc_ia.get_batch(ia_target_bid)
            assert batch_after["current_scheme_id"] == result["import_result"].scheme_id
            assert batch_after["current_scheme_name"] == "ia_imported_applied"
            cfg_after = json.loads(batch_after["config_json"])
            assert cfg_after["anomaly_detection"]["zscore_threshold"] == 0.8

            ia_audit = svc_ia.get_scheme_audit_logs(batch_id=ia_target_bid, action="import_apply")
            assert len(ia_audit) >= 1, "import_and_apply 应记录 import_apply 审计日志"
            ia_al = ia_audit[0]
            assert ia_al["action"] == "import_apply"
            assert ia_al["result"] == "success"
            assert ia_al["trigger_method"] == "import"
            assert ia_al["scheme_id"] == result["import_result"].scheme_id
            assert ia_al["config_diff"] is not None

            r55.ok()
            print(f"  [PASS] {r55.name}  (scheme_id={result['import_result'].scheme_id}, cfg_v={cfg_after['version']})")
        except Exception as e:
            r55.fail(str(e))
            print(f"  [FAIL] {r55.name}: {e}")

        # ====== 测试 56: 导入应用后回滚，再继续处理 ======
        r56 = TestResult("测试56: 导入应用后回滚成功，回滚后继续 process 正常，审计历史连续")
        results.append(r56)
        try:
            svc_iar = PipelineService(db_path)

            iar_src_bid = svc_iar.create_batch("iar_src", SAMPLE_CSV)
            svc_iar.process_batch(iar_src_bid)
            iar_src_sid = svc_iar.save_scheme("iar_source", batch_id=iar_src_bid)

            iar_dir = os.path.join(tmpdir, "iar_exports")
            os.makedirs(iar_dir, exist_ok=True)
            iar_path = os.path.join(iar_dir, "iar_source.json")
            svc_iar.export_scheme_to_file(iar_src_sid, iar_path)

            with open(iar_path, "r", encoding="utf-8") as f:
                iar_data = json.load(f)
            iar_data["config"]["anomaly_detection"]["zscore_threshold"] = 0.3
            iar_mod_path = os.path.join(iar_dir, "iar_modified.json")
            with open(iar_mod_path, "w", encoding="utf-8") as f:
                json.dump(iar_data, f, ensure_ascii=False)

            iar_target_bid = svc_iar.create_batch("iar_target", SAMPLE_CSV)
            svc_iar.process_batch(iar_target_bid)
            cfg_before = json.loads(svc_iar.get_batch(iar_target_bid)["config_json"])
            threshold_before = cfg_before["anomaly_detection"]["zscore_threshold"]

            result = svc_iar.import_and_apply_scheme(iar_mod_path, iar_target_bid,
                                                      on_conflict=SchemeImportResult.ACTION_RENAME,
                                                      new_name="iar_imported")
            assert result["import_result"].success
            cfg_after_import = json.loads(svc_iar.get_batch(iar_target_bid)["config_json"])
            assert cfg_after_import["anomaly_detection"]["zscore_threshold"] == 0.3

            rb_result = svc_iar.rollback_scheme(iar_target_bid)
            assert rb_result.success

            cfg_after_rb = json.loads(svc_iar.get_batch(iar_target_bid)["config_json"])
            assert cfg_after_rb["anomaly_detection"]["zscore_threshold"] == threshold_before, \
                f"回滚后阈值应恢复到 {threshold_before}，实际 {cfg_after_rb['anomaly_detection']['zscore_threshold']}"

            run_id, run_n = svc_iar.process_batch(iar_target_bid)
            assert run_n >= 2, "回滚后 process 应正常执行"

            all_audit = svc_iar.get_scheme_audit_logs(batch_id=iar_target_bid)
            non_dry = [a for a in all_audit if a["action"] != "dry_run"]
            actions = [a["action"] for a in non_dry]
            assert "import_apply" in actions, "审计历史应包含 import_apply"
            assert "rollback" in actions, "审计历史应包含 rollback"

            r56.ok()
            print(f"  [PASS] {r56.name}  (rollback_ok, process_after_rb=run#{run_n})")
        except Exception as e:
            r56.fail(str(e))
            print(f"  [FAIL] {r56.name}: {e}")

        # ====== 测试 57: get_latest_scheme_change 快查 ======
        r57 = TestResult("测试57: get_latest_scheme_change 返回最近变更详情，跨重启保留")
        results.append(r57)
        try:
            svc_lc = PipelineService(db_path)

            lc_bid = svc_lc.create_batch("lc_batch", SAMPLE_CSV)
            svc_lc.process_batch(lc_bid)

            lc_no_change = svc_lc.get_latest_scheme_change(lc_bid)
            assert lc_no_change["batch_id"] == lc_bid
            assert lc_no_change["latest_change"] is None

            lc_sid = svc_lc.save_scheme("lc_scheme", batch_id=lc_bid, description="变更测试")
            svc_lc.apply_scheme_to_batch(lc_sid, lc_bid)

            lc_after_apply = svc_lc.get_latest_scheme_change(lc_bid)
            assert lc_after_apply["latest_change"] is not None
            lc_info = lc_after_apply["latest_change"]
            assert lc_info["action"] == "apply"
            assert lc_info["result"] == "success"
            assert lc_info["scheme_id"] == lc_sid
            assert lc_info["version_change"] is not None
            assert lc_info["trigger_method"] == "cli"

            assert lc_after_apply["scheme_detail"] is not None
            sd = lc_after_apply["scheme_detail"]
            assert sd["scheme_id"] == lc_sid
            assert sd["scheme_name"] == "lc_scheme"

            svc_lc.set_threshold(lc_bid, zscore_threshold=1.2)

            lc_after_thresh = svc_lc.get_latest_scheme_change(lc_bid)
            assert lc_after_thresh["latest_change"]["action"] == "direct_modify"

            svc_lc.rollback_scheme(lc_bid)
            lc_after_rb = svc_lc.get_latest_scheme_change(lc_bid)
            assert lc_after_rb["latest_change"]["action"] == "rollback"
            assert lc_after_rb["rollback_info"] is not None
            assert lc_after_rb["rollback_info"]["rolled_back_to_scheme_id"] == lc_sid

            del svc_lc
            svc_lc2 = PipelineService(db_path)
            lc_restart = svc_lc2.get_latest_scheme_change(lc_bid)
            assert lc_restart["latest_change"]["action"] == "rollback"
            assert lc_restart["latest_change"]["version_change"] is not None

            r57.ok()
            print(f"  [PASS] {r57.name}  (no_change/apply/threshold/rollback/restart 全通过)")
        except Exception as e:
            r57.fail(str(e))
            print(f"  [FAIL] {r57.name}: {e}")

        # ====== 测试 58: CLI scheme import-apply / last-change 命令 ======
        r58 = TestResult("测试58: CLI scheme import-apply / last-change 命令输出完整")
        results.append(r58)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()
            svc_cli = PipelineService(db_path)

            cli_imp_bid = svc_cli.create_batch("cli_imp_batch", SAMPLE_CSV)
            svc_cli.process_batch(cli_imp_bid)
            cli_imp_sid = svc_cli.save_scheme("cli_imp_scheme", batch_id=cli_imp_bid, description="CLI导入应用源")

            cli_imp_dir = os.path.join(tmpdir, "cli_imp_exports")
            os.makedirs(cli_imp_dir, exist_ok=True)
            cli_imp_path = os.path.join(cli_imp_dir, "cli_imp.json")
            svc_cli.export_scheme_to_file(cli_imp_sid, cli_imp_path)

            cli_imp_target = svc_cli.create_batch("cli_imp_target", SAMPLE_CSV)
            svc_cli.process_batch(cli_imp_target)

            res_imp_apply = runner.invoke(cli, ["--db", db_path, "scheme", "import-apply",
                                                 cli_imp_path, str(cli_imp_target),
                                                 "--on-conflict", "rename",
                                                 "--new-name", "cli_imported_applied"])
            assert res_imp_apply.exit_code == 0, f"import-apply 应成功，output={res_imp_apply.output}"
            assert "[OK]" in res_imp_apply.output
            assert "方案导入并应用成功" in res_imp_apply.output
            assert "方案ID" in res_imp_apply.output
            assert "配置版本" in res_imp_apply.output

            res_last_change = runner.invoke(cli, ["--db", db_path, "scheme", "last-change",
                                                   str(cli_imp_target)])
            assert res_last_change.exit_code == 0, f"last-change 应成功，output={res_last_change.output}"
            assert "方案变更结果" in res_last_change.output
            assert "最近一次变更" in res_last_change.output
            assert "导入应用" in res_last_change.output or "import_apply" in res_last_change.output.lower()
            assert "版本变化" in res_last_change.output
            assert "成功" in res_last_change.output

            res_last_change_no = runner.invoke(cli, ["--db", db_path, "scheme", "last-change",
                                                      str(cli_imp_bid)])
            assert res_last_change_no.exit_code == 0
            assert "尚无方案变更记录" in res_last_change_no.output or "方案变更结果" in res_last_change_no.output

            res_help = runner.invoke(cli, ["scheme", "--help"])
            assert "import-apply" in res_help.output
            assert "last-change" in res_help.output

            res_ia_help = runner.invoke(cli, ["scheme", "import-apply", "--help"])
            assert res_ia_help.exit_code == 0
            assert "FILE_PATH" in res_ia_help.output
            assert "BATCH_ID" in res_ia_help.output
            assert "on-conflict" in res_ia_help.output

            res_lc_help = runner.invoke(cli, ["scheme", "last-change", "--help"])
            assert res_lc_help.exit_code == 0
            assert "BATCH_ID" in res_lc_help.output

            r58.ok()
            print(f"  [PASS] {r58.name}  (import-apply + last-change CLI 输出完整)")
        except Exception as e:
            r58.fail(str(e))
            print(f"  [FAIL] {r58.name}: {e}")

        # ====== 测试 59: 导入应用后 switch/rollback 链路完整 + 审计历史连续 ======
        r59 = TestResult("测试59: 导入应用后用 switch rollback 回滚，再继续 process，审计历史不断")
        results.append(r59)
        try:
            svc_chain = PipelineService(db_path)

            ch_src_bid = svc_chain.create_batch("ch_src", SAMPLE_CSV)
            svc_chain.process_batch(ch_src_bid)
            ch_src_sid = svc_chain.save_scheme("ch_source", batch_id=ch_src_bid, description="链路源")

            ch_dir = os.path.join(tmpdir, "ch_exports")
            os.makedirs(ch_dir, exist_ok=True)
            ch_path = os.path.join(ch_dir, "ch_source.json")
            svc_chain.export_scheme_to_file(ch_src_sid, ch_path)

            ch_target_bid = svc_chain.create_batch("ch_target", SAMPLE_CSV)
            svc_chain.process_batch(ch_target_bid)

            ch_result = svc_chain.import_and_apply_scheme(ch_path, ch_target_bid,
                                                           on_conflict=SchemeImportResult.ACTION_RENAME,
                                                           new_name="ch_imported")
            assert ch_result["import_result"].success

            ch_batch_before = svc_chain.get_batch(ch_target_bid)
            assert ch_batch_before["current_scheme_id"] == ch_result["import_result"].scheme_id

            sw_result = svc_chain.switch_scheme(
                SwitchSchemeResult.SWITCH_TYPE_ROLLBACK, ch_target_bid)
            assert sw_result.success
            assert sw_result.rollback_result is not None
            assert sw_result.rollback_result.success

            ch_batch_after = svc_chain.get_batch(ch_target_bid)
            assert ch_batch_after["current_scheme_id"] != ch_result["import_result"].scheme_id

            ch_run_id, ch_run_n = svc_chain.process_batch(ch_target_bid)
            assert ch_run_n >= 2

            all_audit = svc_chain.get_scheme_audit_logs(batch_id=ch_target_bid)
            non_dry = [a for a in all_audit if a["action"] != "dry_run"]
            actions = [a["action"] for a in non_dry]
            assert "import_apply" in actions
            assert "rollback" in actions

            lc = svc_chain.get_latest_scheme_change(ch_target_bid)
            assert lc["latest_change"]["action"] == "rollback"
            assert lc["rollback_info"] is not None

            r59.ok()
            print(f"  [PASS] {r59.name}  (import_apply→switch_rollback→process→audit 连续)")
        except Exception as e:
            r59.fail(str(e))
            print(f"  [FAIL] {r59.name}: {e}")

        # ====== 测试 60: 快照导出成功，ZIP 包结构完整
        r60 = TestResult("测试60: 快照导出成功，ZIP 包含所有必需文件和校验和")
        results.append(r60)
        try:
            svc_snap1 = PipelineService(db_path)

            snap_batch_id = svc_snap1.create_batch("snap_export_test", SAMPLE_CSV)
            svc_snap1.process_batch(snap_batch_id)

            snap_dir = os.path.join(tmpdir, "snapshots")
            os.makedirs(snap_dir, exist_ok=True)
            snap_zip = os.path.join(snap_dir, "test_snapshot.zip")

            result = svc_snap1.export_snapshot(
                "test_snap_001", snap_batch_id, output_path=snap_zip)

            assert result["snapshot_id"] > 0
            assert os.path.exists(snap_zip), "ZIP 文件应存在"
            assert os.path.getsize(snap_zip) > 0, "ZIP 文件不应为空"

            import zipfile
            with zipfile.ZipFile(snap_zip, "r") as zf:
                file_list = zf.namelist()
                required_files = ["manifest.json", "config.json", "metrics.json",
                              "anomalies.json", "errors.json", "source_summary.json",
                              "dependencies.json", "checksum.json"]
                for req_file in required_files:
                    assert req_file in file_list, f"ZIP 缺少必需文件: {req_file}"

                manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
                assert manifest["format_version"] == "1.0"
                assert manifest["snapshot_type"] == "run"
                assert "source" in manifest
                assert "config" in manifest
                assert "source_summary" in manifest
                assert "metrics_summary" in manifest
                assert "anomalies_summary" in manifest
                assert "errors_summary" in manifest
                assert "dependencies" in manifest
                assert "checksums" in manifest

                checksum_data = json.loads(zf.read("checksum.json").decode("utf-8"))
                assert "files" in checksum_data
                for fname in required_files:
                    if fname != "checksum.json":
                        assert fname in checksum_data["files"]

            snap_record = svc_snap1.get_snapshot(result["snapshot_id"])
            assert snap_record is not None
            assert snap_record["name"] == "test_snap_001"
            assert snap_record["status"] == "available"
            assert snap_record["checksum_sha256"] == result["checksum_sha256"]
            assert "python" in snap_record["manifest"]["dependencies"]
            assert "pandas" in snap_record["manifest"]["dependencies"]
            assert "snapshot_format" in snap_record["manifest"]["dependencies"]

            r60.ok()
            print(f"  [PASS] {r60.name}  (snapshot_id={result['snapshot_id']}, zip_size={os.path.getsize(snap_zip)})")
        except Exception as e:
            r60.fail(str(e))
            print(f"  [FAIL] {r60.name}: {e}")

        # ====== 测试 61: 导出后重启导入（跨数据库/重启后导入查看）
        r61 = TestResult("测试61: 快照导出后重启导入，数据完整可查")
        results.append(r61)
        try:
            svc_snap2 = PipelineService(db_path)

            snap2_batch_id = svc_snap2.create_batch("snap_restart_src", SAMPLE_CSV)
            svc_snap2.process_batch(snap2_batch_id)

            snap2_zip = os.path.join(tmpdir, "snap_restart.zip")
            export_result = svc_snap2.export_snapshot(
                "snap_restart_test", snap2_batch_id, output_path=snap2_zip)
            original_snap_id = export_result["snapshot_id"]

            del svc_snap2

            new_db_path = os.path.join(tmpdir, "restart_import.db")
            svc_import = PipelineService(new_db_path)

            import_result = svc_import.import_snapshot(snap2_zip)
            assert import_result.success
            assert import_result.snapshot_id > 0
            assert import_result.imported_from is not None

            imported_snap = svc_import.get_snapshot(import_result.snapshot_id)
            assert imported_snap is not None
            assert imported_snap["name"] == "snap_restart_src_snapshot" or import_result.final_name
            assert imported_snap["original_batch_id"] == snap2_batch_id
            assert imported_snap["imported_from"] is not None

            manifest = imported_snap["manifest"]
            assert manifest["source"]["original_batch_id"] == snap2_batch_id
            assert manifest["source"]["original_batch_name"] == "snap_restart_src"
            assert manifest["metrics_summary"]["total_metrics"] > 0

            snap_list = svc_import.list_snapshots()
            assert len(snap_list) == 1
            assert snap_list[0]["id"] == import_result.snapshot_id

            r61.ok()
            print(f"  [PASS] {r61.name}  (original_id={original_snap_id}, imported_id={import_result.snapshot_id})")
        except Exception as e:
            r61.fail(str(e))
            print(f"  [FAIL] {r61.name}: {e}")

        # ====== 测试 62: 导入冲突 - reject 策略
        r62 = TestResult("测试62: 导入冲突 - reject 策略正确拒绝同名快照")
        results.append(r62)
        try:
            svc_snap3 = PipelineService(db_path)

            snap3_batch_id = svc_snap3.create_batch("snap_reject_src", SAMPLE_CSV)
            svc_snap3.process_batch(snap3_batch_id)

            snap3_zip = os.path.join(tmpdir, "snap_reject.zip")
            svc_snap3.export_snapshot("snap_reject_test", snap3_batch_id, output_path=snap3_zip)

            import_result_ok = svc_snap3.import_snapshot(snap3_zip, on_conflict="reject")
            assert import_result_ok.success, "首次导入应成功"

            import_result_reject = svc_snap3.import_snapshot(snap3_zip, on_conflict="reject")
            assert not import_result_reject.success, "reject 策略应返回 success=False"
            assert import_result_reject.action == "reject"

            audit_reject = svc_snap3.get_snapshot_audit_logs(action="snapshot_import", result="blocked")
            assert len(audit_reject) >= 1, "reject 应记录审计日志"
            assert "拒绝" in audit_reject[0]["error_message"] or "reject" in audit_reject[0]["error_message"].lower()

            r62.ok()
            print(f"  [PASS] {r62.name}  (reject 策略生效)")
        except Exception as e:
            r62.fail(str(e))
            print(f"  [FAIL] {r62.name}: {e}")

        # ====== 测试 63: 导入冲突 - rename 策略
        r63 = TestResult("测试63: 导入冲突 - rename 策略自动重命名")
        results.append(r63)
        try:
            svc_snap4 = PipelineService(db_path)

            snap4_batch_id = svc_snap4.create_batch("snap_rename_src", SAMPLE_CSV)
            svc_snap4.process_batch(snap4_batch_id)

            snap4_zip = os.path.join(tmpdir, "snap_rename.zip")
            svc_snap4.export_snapshot("snap_rename_test", snap4_batch_id, output_path=snap4_zip)

            svc_snap4.import_snapshot(snap4_zip)

            import_rename = svc_snap4.import_snapshot(snap4_zip, on_conflict="rename", new_name="renamed_snap_v2")
            assert import_rename.success
            assert import_rename.action == "rename"
            assert import_rename.final_name == "renamed_snap_v2"
            assert import_rename.original_name != import_rename.final_name

            snap4_list = svc_snap4.list_snapshots()
            snap4_names = [s["name"] for s in snap4_list]
            assert "snap_rename_src_snapshot" in snap4_names
            assert "renamed_snap_v2" in snap4_names

            r63.ok()
            print(f"  [PASS] {r63.name}  (自动重命名成功, final_name={import_rename.final_name})")
        except Exception as e:
            r63.fail(str(e))
            print(f"  [FAIL] {r63.name}: {e}")

        # ====== 测试 64: 导入冲突 - skip 策略
        r64 = TestResult("测试64: 导入冲突 - skip 策略正确跳过")
        results.append(r64)
        try:
            svc_snap5 = PipelineService(db_path)

            snap5_batch_id = svc_snap5.create_batch("snap_skip_src", SAMPLE_CSV)
            svc_snap5.process_batch(snap5_batch_id)

            snap5_zip = os.path.join(tmpdir, "snap_skip.zip")
            svc_snap5.export_snapshot("snap_skip_test", snap5_batch_id, output_path=snap5_zip)

            svc_snap5.import_snapshot(snap5_zip)

            count_before = len(svc_snap5.list_snapshots())

            import_skip = svc_snap5.import_snapshot(snap5_zip, on_conflict="skip")
            assert not import_skip.success
            assert import_skip.action == "skip"

            count_after = len(svc_snap5.list_snapshots())
            assert count_after == count_before, "skip 不应增加快照数量"

            audit_skip = svc_snap5.get_snapshot_audit_logs(action="snapshot_import", result="blocked")
            skip_logs = [l for l in audit_skip if "跳过" in (l["error_message"] or "") or "skip" in (l["error_message"] or "").lower()]
            assert len(skip_logs) >= 1, "skip 应记录审计日志"

            r64.ok()
            print(f"  [PASS] {r64.name}  (skip 策略生效)")
        except Exception as e:
            r64.fail(str(e))
            print(f"  [FAIL] {r64.name}: {e}")

        # ====== 测试 65: 源文件缺失时导入（不阻止但记录警告）
        r65 = TestResult("测试65: 源文件缺失时导入不阻止，记录警告，replay 时需指定替代文件")
        results.append(r65)
        try:
            svc_snap6 = PipelineService(db_path)

            temp_csv = os.path.join(tmpdir, "temp_source.csv")
            import shutil
            shutil.copy(SAMPLE_CSV, temp_csv)

            snap6_batch_id = svc_snap6.create_batch("snap_missing_src", temp_csv)
            svc_snap6.process_batch(snap6_batch_id)

            snap6_zip = os.path.join(tmpdir, "snap_missing.zip")
            export6 = svc_snap6.export_snapshot("snap_missing_test", snap6_batch_id, output_path=snap6_zip)

            os.remove(temp_csv)
            assert not os.path.exists(temp_csv), "临时源文件应已删除"

            import_missing = svc_snap6.import_snapshot(snap6_zip, on_conflict="rename", new_name="imported_missing_test")
            assert import_missing.success

            imported6 = svc_snap6.get_snapshot(import_missing.snapshot_id)
            assert imported6 is not None
            assert imported6["manifest"]["source"]["original_source_file"] == temp_csv

            try:
                svc_snap6.replay_snapshot(import_missing.snapshot_id)
                assert False, "源文件缺失时 replay 应抛出异常"
            except Exception as e:
                assert "源文件不存在" in str(e) or "missing" in str(e).lower()

            replay_ok = svc_snap6.replay_snapshot(
                import_missing.snapshot_id, csv_path=SAMPLE_CSV, new_batch_name="replay_with_alt_csv")
            assert replay_ok.success
            assert replay_ok.new_batch_id > 0

            r65.ok()
            print(f"  [PASS] {r65.name}  (导入允许缺失源文件，replay 指定替代文件成功)")
        except Exception as e:
            r65.fail(str(e))
            print(f"  [FAIL] {r65.name}: {e}")

        # ====== 测试 66: 配置版本冲突导入（不阻止，使用快照配置）
        r66 = TestResult("测试66: 配置版本不兼容时导入不阻止，使用快照中的配置")
        results.append(r66)
        try:
            svc_snap7 = PipelineService(db_path)

            snap7_batch_id = svc_snap7.create_batch("snap_cfg_src", SAMPLE_CSV)
            svc_snap7.process_batch(snap7_batch_id)
            svc_snap7.set_threshold(snap7_batch_id, zscore_threshold=1.0)
            svc_snap7.process_batch(snap7_batch_id)

            snap7_zip = os.path.join(tmpdir, "snap_cfg.zip")
            svc_snap7.export_snapshot("snap_cfg_test", snap7_batch_id, output_path=snap7_zip)

            new_db7 = os.path.join(tmpdir, "cfg_conflict.db")
            svc_import7 = PipelineService(new_db7)

            import7 = svc_import7.import_snapshot(snap7_zip)
            assert import7.success

            imported7 = svc_import7.get_snapshot(import7.snapshot_id)
            assert imported7["config_version"] >= 2

            manifest7 = imported7["manifest"]
            assert manifest7["config"]["version"] >= 2
            assert manifest7["config"]["config"]["anomaly_detection"]["zscore_threshold"] == 1.0

            r66.ok()
            print(f"  [PASS] {r66.name}  (config_version={imported7['config_version']})")
        except Exception as e:
            r66.fail(str(e))
            print(f"  [FAIL] {r66.name}: {e}")

        # ====== 测试 67: 快照 replay 成功，指标对比正确
        r67 = TestResult("测试67: 快照 replay 成功，指标对比输出差异、失败原因、是否可接受")
        results.append(r67)
        try:
            svc_snap8 = PipelineService(db_path)

            snap8_batch_id = svc_snap8.create_batch("snap_replay_src", SAMPLE_CSV)
            svc_snap8.process_batch(snap8_batch_id)

            snap8_zip = os.path.join(tmpdir, "snap_replay.zip")
            export8 = svc_snap8.export_snapshot("snap_replay_test", snap8_batch_id, output_path=snap8_zip)
            snap8_id = export8["snapshot_id"]

            replay8 = svc_snap8.replay_snapshot(
                snap8_id, new_batch_name="replay_identical_data", csv_path=SAMPLE_CSV, tolerance_pct=1.0)
            assert replay8.success
            assert replay8.new_batch_id > 0
            assert replay8.new_run_id > 0
            assert replay8.snapshot_id == snap8_id

            mc = replay8.metrics_comparison
            assert mc["total_metrics_compared"] > 0
            assert mc["tolerance_pct"] == 1.0

            for d in replay8.differences:
                assert "sensor" in d
                assert "metric" in d
                assert "original" in d
                assert "new" in d
                assert "within_tolerance" in d

            assert isinstance(replay8.acceptable, bool)

            audit_replay = svc_snap8.get_snapshot_audit_logs(action="snapshot_replay")
            assert len(audit_replay) >= 1
            assert audit_replay[0]["result"] == "success"
            assert audit_replay[0]["snapshot_id"] == snap8_id
            assert audit_replay[0]["batch_id"] == replay8.new_batch_id

            r67.ok()
            print(f"  [PASS] {r67.name}  (differences={len(replay8.differences)}, acceptable={replay8.acceptable})")
        except Exception as e:
            r67.fail(str(e))
            print(f"  [FAIL] {r67.name}: {e}")

        # ====== 测试 68: 快照 replay 差异容忍度验证
        r68 = TestResult("测试68: replay 指标差异超出容忍度时标记为不可接受")
        results.append(r68)
        try:
            svc_snap9 = PipelineService(db_path)

            snap9_batch_id = svc_snap9.create_batch("snap_replay_diff", SAMPLE_CSV)
            svc_snap9.process_batch(snap9_batch_id)

            snap9_zip = os.path.join(tmpdir, "snap_replay_diff.zip")
            export9 = svc_snap9.export_snapshot("snap_replay_diff_test", snap9_batch_id, output_path=snap9_zip)
            snap9_id = export9["snapshot_id"]

            modified_csv9 = os.path.join(tmpdir, "modified_for_replay.csv")
            with open(SAMPLE_CSV, "r", encoding="utf-8") as f:
                content9 = f.read()
            modified_content9 = content9.replace("23.5", "9999.0")
            with open(modified_csv9, "w", encoding="utf-8") as f:
                f.write(modified_content9)

            replay9 = svc_snap9.replay_snapshot(
                snap9_id,
                new_batch_name="replay_with_modified_data",
                csv_path=modified_csv9,
                tolerance_pct=0.01)
            assert replay9.success

            mc9 = replay9.metrics_comparison
            assert mc9["metrics_out_of_tolerance"] > 0, "修改数据后应有指标超出容忍度"
            assert replay9.acceptable is False, "超出容忍度时 acceptable 应为 False"
            assert len(replay9.failures) > 0, "应有失败的指标列表"

            for failure in replay9.failures:
                assert "%" in failure or "tolerance" in failure.lower()

            r68.ok()
            print(f"  [PASS] {r68.name}  (out_of_tolerance={mc9['metrics_out_of_tolerance']}, failures={len(replay9.failures)})")
        except Exception as e:
            r68.fail(str(e))
            print(f"  [FAIL] {r68.name}: {e}")

        # ====== 测试 69: 快照审计日志查询
        r69 = TestResult("测试69: 快照审计日志查询，可按操作类型和结果筛选")
        results.append(r69)
        try:
            svc_snap10 = PipelineService(db_path)

            all_audit = svc_snap10.get_snapshot_audit_logs()
            assert len(all_audit) >= 1, "应有审计日志"

            export_audit = svc_snap10.get_snapshot_audit_logs(action="snapshot_export")
            assert len(export_audit) >= 1
            for ea in export_audit:
                assert ea["action"] == "snapshot_export"
                assert ea["result"] == "success"

            import_audit = svc_snap10.get_snapshot_audit_logs(action="snapshot_import")
            assert len(import_audit) >= 1

            replay_audit = svc_snap10.get_snapshot_audit_logs(action="snapshot_replay")
            assert len(replay_audit) >= 1

            success_audit = svc_snap10.get_snapshot_audit_logs(result="success")
            assert len(success_audit) >= 1

            blocked_audit = svc_snap10.get_snapshot_audit_logs(result="blocked")
            assert len(blocked_audit) >= 1

            for log in all_audit:
                assert "id" in log
                assert "created_at" in log
                assert "action" in log
                assert "result" in log
                assert "snapshot_id" in log or "snapshot_name" in log

            r69.ok()
            print(f"  [PASS] {r69.name}  (export={len(export_audit)}, import={len(import_audit)}, replay={len(replay_audit)})")
        except Exception as e:
            r69.fail(str(e))
            print(f"  [FAIL] {r69.name}: {e}")

        # ====== 测试 70: CLI snapshot 命令帮助文本完整
        r70 = TestResult("测试70: CLI snapshot 命令帮助文本完整")
        results.append(r70)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()

            res_snap_help = runner.invoke(cli, ["snapshot", "--help"])
            assert res_snap_help.exit_code == 0
            assert "export" in res_snap_help.output
            assert "import" in res_snap_help.output
            assert "list" in res_snap_help.output
            assert "show" in res_snap_help.output
            assert "replay" in res_snap_help.output
            assert "delete" in res_snap_help.output
            assert "audit-history" in res_snap_help.output

            res_export_help = runner.invoke(cli, ["snapshot", "export", "--help"])
            assert res_export_help.exit_code == 0
            assert "NAME" in res_export_help.output or "name" in res_export_help.output
            assert "BATCH_ID" in res_export_help.output or "batch_id" in res_export_help.output
            assert "--run-id" in res_export_help.output
            assert "--output" in res_export_help.output
            assert "--type" in res_export_help.output

            res_import_help = runner.invoke(cli, ["snapshot", "import", "--help"])
            assert res_import_help.exit_code == 0
            assert "FILE_PATH" in res_import_help.output or "file_path" in res_import_help.output
            assert "--on-conflict" in res_import_help.output
            assert "--new-name" in res_import_help.output
            assert "reject" in res_import_help.output
            assert "rename" in res_import_help.output
            assert "skip" in res_import_help.output

            res_replay_help = runner.invoke(cli, ["snapshot", "replay", "--help"])
            assert res_replay_help.exit_code == 0
            assert "SNAPSHOT_ID" in res_replay_help.output or "snapshot_id" in res_replay_help.output
            assert "--new-batch-name" in res_replay_help.output
            assert "--csv-path" in res_replay_help.output
            assert "--tolerance" in res_replay_help.output

            res_audit_help = runner.invoke(cli, ["snapshot", "audit-history", "--help"])
            assert res_audit_help.exit_code == 0
            assert "--action" in res_audit_help.output
            assert "--result" in res_audit_help.output
            assert "--limit" in res_audit_help.output

            r70.ok()
            print(f"  [PASS] {r70.name}")
        except Exception as e:
            r70.fail(str(e))
            print(f"  [FAIL] {r70.name}: {e}")

        # ====== 测试 71: CLI snapshot export/import/list/show/replay 输出对齐
        r71 = TestResult("测试71: CLI snapshot 命令输出与文档对齐")
        results.append(r71)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()
            svc_cli_snap = PipelineService(db_path)

            cli_snap_batch = svc_cli_snap.create_batch("cli_snap_test_batch", SAMPLE_CSV)
            svc_cli_snap.process_batch(cli_snap_batch)

            cli_snap_zip = os.path.join(tmpdir, "cli_snap.zip")
            res_export = runner.invoke(cli, [
                "--db", db_path, "snapshot", "export",
                "cli_test_snapshot", str(cli_snap_batch),
                "--output", cli_snap_zip
            ])
            assert res_export.exit_code == 0, f"export 应成功，exit={res_export.exit_code}, output={res_export.output}"
            assert "[OK]" in res_export.output
            assert "快照导出成功" in res_export.output
            assert "快照ID" in res_export.output
            assert "SHA256" in res_export.output
            assert "配置版本" in res_export.output
            assert "指标数量" in res_export.output

            res_list = runner.invoke(cli, ["--db", db_path, "snapshot", "list"])
            assert res_list.exit_code == 0
            assert "ID" in res_list.output
            assert "名称" in res_list.output
            assert "cli_test_snapshot" in res_list.output

            snap_id_for_show = None
            for line in res_list.output.split("\n"):
                if "cli_test_snapshot" in line:
                    parts = line.split()
                    if parts and parts[0].isdigit():
                        snap_id_for_show = int(parts[0])
                        break
            assert snap_id_for_show is not None

            res_show = runner.invoke(cli, ["--db", db_path, "snapshot", "show", str(snap_id_for_show)])
            assert res_show.exit_code == 0
            assert "快照 #" in res_show.output
            assert "来源信息" in res_show.output
            assert "处理摘要" in res_show.output
            assert "依赖版本" in res_show.output
            assert "校验和" in res_show.output
            assert "源数据摘要" in res_show.output

            res_replay = runner.invoke(cli, [
                "--db", db_path, "snapshot", "replay",
                str(snap_id_for_show),
                "--new-batch-name", "cli_replay_test",
                "--csv-path", SAMPLE_CSV,
                "--tolerance", "1.0"
            ])
            assert res_replay.exit_code == 0, f"replay 应成功，exit={res_replay.exit_code}, output={res_replay.output}"
            assert "[OK]" in res_replay.output
            assert "快照重放完成" in res_replay.output
            assert "新批次ID" in res_replay.output
            assert "可接受" in res_replay.output
            assert "指标对比" in res_replay.output

            res_audit = runner.invoke(cli, [
                "--db", db_path, "snapshot", "audit-history",
                str(snap_id_for_show)
            ])
            assert res_audit.exit_code == 0
            assert "ID" in res_audit.output
            assert "操作" in res_audit.output
            assert "导出" in res_audit.output or "导入" in res_audit.output or "重放" in res_audit.output

            r71.ok()
            print(f"  [PASS] {r71.name}  (export/list/show/replay/audit 全部对齐)")
        except Exception as e:
            r71.fail(str(e))
            print(f"  [FAIL] {r71.name}: {e}")

        # ====== 测试 72: 基线注册 + 重启后查询 ======
        r72 = TestResult("测试72: 基线注册 + 重启后查询（持久化验证）")
        results.append(r72)
        try:
            svc_bl = PipelineService(db_path)
            bl_batch = svc_bl.create_batch("baseline_register_batch", SAMPLE_CSV)
            bl_run_id, bl_run_num = svc_bl.process_batch(bl_batch)
            assert bl_run_id > 0
            assert bl_run_num >= 1

            reg_result = svc_bl.register_baseline(
                "test_baseline_v1", bl_batch,
                description="测试基线 v1", warn_pct=5.0, block_pct=15.0
            )
            assert reg_result["baseline_id"] > 0
            assert reg_result["name"] == "test_baseline_v1"
            assert reg_result["config_version"] >= 1
            assert reg_result["metrics_count"] > 0

            bl_list = svc_bl.list_baselines()
            assert len(bl_list) >= 1
            names = [b["name"] for b in bl_list]
            assert "test_baseline_v1" in names

            del svc_bl
            import gc; gc.collect()
            svc_bl2 = PipelineService(db_path)
            bl_reloaded = svc_bl2.get_baseline_by_name("test_baseline_v1")
            assert bl_reloaded is not None, "重启后基线应存在"
            assert bl_reloaded["name"] == "test_baseline_v1"
            assert bl_reloaded["config_version"] >= 1
            assert bl_reloaded["status"] == "active"
            assert "metric_thresholds" in bl_reloaded
            assert "config" in bl_reloaded
            assert bl_reloaded["description"] == "测试基线 v1"
            assert "source_batch_name" in bl_reloaded
            del svc_bl2

            r72.ok()
            print(f"  [PASS] {r72.name}  (持久化+重启查询 OK)")
        except Exception as e:
            r72.fail(str(e))
            print(f"  [FAIL] {r72.name}: {e}")

        # ====== 测试 73: 基线复核 - 通过(pass) ======
        r73 = TestResult("测试73: 基线复核 - 通过（同数据同配置）")
        results.append(r73)
        try:
            svc_pass = PipelineService(db_path)
            pass_batch = svc_pass.create_batch("baseline_pass_batch", SAMPLE_CSV)
            svc_pass.process_batch(pass_batch)

            pass_bl = svc_pass.register_baseline(
                "pass_baseline", pass_batch,
                description="通过测试基线", warn_pct=100.0, block_pct=200.0
            )

            check_pass = svc_pass.check_baseline(pass_bl["baseline_id"], pass_batch)
            assert check_pass.overall_status == "pass", f"期望 pass, 实际 {check_pass.overall_status}"
            assert check_pass.block_count == 0
            assert check_pass.warn_count == 0
            assert check_pass.pass_count == check_pass.total_metrics
            assert check_pass.total_metrics > 0
            assert "通过" in check_pass.recommended_action or "监控" in check_pass.recommended_action or "容忍范围" in check_pass.recommended_action

            bl_updated = svc_pass.get_baseline(pass_bl["baseline_id"])
            assert bl_updated["last_check_status"] == "pass"
            assert bl_updated["last_checked_at"] is not None

            r73.ok()
            print(f"  [PASS] {r73.name}  (status=pass, counts OK)")
        except Exception as e:
            r73.fail(str(e))
            print(f"  [FAIL] {r73.name}: {e}")

        # ====== 测试 74: 基线复核 - 阻断(block) ======
        r74 = TestResult("测试74: 基线复核 - 阻断结果（差异极大）")
        results.append(r74)
        try:
            svc_block = PipelineService(db_path)
            block_batch = svc_block.create_batch("baseline_block_batch", SAMPLE_CSV)
            svc_block.process_batch(block_batch)

            block_bl = svc_block.register_baseline(
                "block_baseline", block_batch,
                description="阻断测试基线", warn_pct=0.01, block_pct=0.02
            )

            diff_csv_path = os.path.join(tmpdir, "diff_data.csv")
            with open(diff_csv_path, "w", encoding="utf-8") as f:
                f.write("timestamp,sensor_temp,sensor_pressure,sensor_humidity\n")
                for i in range(200):
                    ts = f"2025-01-01 {i//60:02d}:{i%60:02d}:00"
                    f.write(f"{ts},{i*100.0 + 500.0},{i*50.0 + 200.0},{i*10.0 + 30.0}\n")

            diff_batch = svc_block.create_batch("baseline_block_target", diff_csv_path)
            svc_block.process_batch(diff_batch)

            check_block = svc_block.check_baseline(block_bl["baseline_id"], diff_batch)
            assert check_block.overall_status == "block", f"期望 block, 实际 {check_block.overall_status}"
            assert check_block.block_count > 0
            assert check_block.total_metrics >= check_block.block_count

            has_block_metric = False
            for mr in check_block.metric_results:
                if mr.status == "block":
                    has_block_metric = True
                    assert abs(mr.relative_pct) >= 0.02
                    assert mr.metric_name
                    break
            assert has_block_metric, "应存在阻断级别的指标"

            assert "停止发布" in check_block.recommended_action or "阻断" in check_block.recommended_action or "排查" in check_block.recommended_action

            bl_block_updated = svc_block.get_baseline(block_bl["baseline_id"])
            assert bl_block_updated["last_check_status"] == "block"

            check_history = svc_block.get_baseline_checks(baseline_id=block_bl["baseline_id"])
            assert len(check_history) >= 1
            assert check_history[0]["check_status"] == "block"
            assert check_history[0]["block_count"] > 0

            r74.ok()
            print(f"  [PASS] {r74.name}  (status=block, 阻断计数={check_block.block_count})")
        except Exception as e:
            r74.fail(str(e))
            print(f"  [FAIL] {r74.name}: {e}")

        # ====== 测试 75: 基线导出+导入-冲突reject ======
        r75 = TestResult("测试75: 基线导出+导入（同名冲突 reject 策略）")
        results.append(r75)
        try:
            svc_imp = PipelineService(db_path)
            imp_batch = svc_imp.create_batch("baseline_imp_batch", SAMPLE_CSV)
            svc_imp.process_batch(imp_batch)

            imp_bl = svc_imp.register_baseline(
                "import_conflict_baseline", imp_batch,
                description="导入冲突测试基线", warn_pct=3.0, block_pct=10.0
            )

            bl_zip = os.path.join(tmpdir, "baseline_export.zip")
            exp_result = svc_imp.export_baseline(imp_bl["baseline_id"], output_path=bl_zip)
            assert os.path.exists(exp_result.file_path)
            assert exp_result.file_size > 0
            assert exp_result.checksum_sha256
            assert exp_result.baseline_name == "import_conflict_baseline"

            imp_reject = svc_imp.import_baseline(bl_zip, on_conflict="reject")
            assert imp_reject.success is False
            assert imp_reject.conflict_action == "reject"
            assert "已存在" in imp_reject.error_message or "冲突" in imp_reject.error_message
            assert imp_reject.original_name == "import_conflict_baseline"

            audit_logs = svc_imp.get_baseline_audit_logs(baseline_id=imp_bl["baseline_id"])
            action_names = [log["action"] for log in audit_logs]
            assert "baseline_register" in action_names
            assert "baseline_export" in action_names

            imp_audit = svc_imp.get_baseline_audit_logs(action="baseline_import")
            assert len(imp_audit) >= 1
            has_reject_log = any(
                log["result"] == "blocked" and (
                    "拒绝" in (log.get("error_message") or "") or
                    "已存在" in (log.get("error_message") or "") or
                    "reject" in (log.get("error_message") or "").lower()
                )
                for log in imp_audit
            )
            assert has_reject_log, "reject 决定应写入审计日志（result=blocked，含拒绝/已存在描述）"

            r75.ok()
            print(f"  [PASS] {r75.name}  (导出 OK, reject 触发审计)")
        except Exception as e:
            r75.fail(str(e))
            print(f"  [FAIL] {r75.name}: {e}")

        # ====== 测试 76: 基线导入冲突-重命名(rename)策略 ======
        r76 = TestResult("测试76: 基线导入同名冲突（rename 策略）")
        results.append(r76)
        try:
            svc_rn = PipelineService(db_path)

            rn_zip = os.path.join(tmpdir, "baseline_rn.zip")
            rn_src = svc_rn.get_baseline_by_name("import_conflict_baseline")
            svc_rn.export_baseline(rn_src["id"], output_path=rn_zip)

            imp_rn = svc_rn.import_baseline(rn_zip, on_conflict="rename", new_name="renamed_baseline_v2")
            assert imp_rn.success is True
            assert imp_rn.conflict_action == "rename"
            assert imp_rn.original_name == "import_conflict_baseline"
            assert imp_rn.final_name == "renamed_baseline_v2"
            assert imp_rn.baseline_id > 0
            assert imp_rn.baseline_id != rn_src["id"]

            rn_reloaded = svc_rn.get_baseline_by_name("renamed_baseline_v2")
            assert rn_reloaded is not None
            assert rn_reloaded["original_baseline_id"] == rn_src["id"]
            assert rn_reloaded["imported_from"] is not None
            assert rn_reloaded["config_version"] == rn_src["config_version"]
            assert rn_reloaded["status"] == "active"

            rn_audit = svc_rn.get_baseline_audit_logs(action="baseline_import", result="success")
            assert len(rn_audit) >= 1
            has_rename_log = any(
                (
                    "rename" in (log.get("error_message") or "").lower() or
                    "renamed" in (log.get("error_message") or "").lower() or
                    (
                        isinstance(log.get("details"), dict) and
                        log.get("details", {}).get("conflict_action") == "rename"
                    )
                )
                for log in rn_audit
            )

            r76.ok()
            print(f"  [PASS] {r76.name}  (rename OK, original_id追溯 OK)")
        except Exception as e:
            r76.fail(str(e))
            print(f"  [FAIL] {r76.name}: {e}")

        # ====== 测试 77: 基线复核历史追溯 ======
        r77 = TestResult("测试77: 基线复核历史追溯（多次复核记录）")
        results.append(r77)
        try:
            svc_hist = PipelineService(db_path)
            hist_batch = svc_hist.create_batch("baseline_hist_batch", SAMPLE_CSV)
            svc_hist.process_batch(hist_batch)

            hist_bl = svc_hist.register_baseline(
                "history_baseline", hist_batch,
                description="历史追溯测试基线", warn_pct=50.0, block_pct=90.0
            )

            svc_hist.check_baseline(hist_bl["baseline_id"], hist_batch)
            svc_hist.check_baseline(hist_bl["baseline_id"], hist_batch)

            checks = svc_hist.get_baseline_checks(baseline_id=hist_bl["baseline_id"])
            assert len(checks) >= 2, f"至少应有2条复核记录，实际 {len(checks)}"
            for c in checks:
                assert c["baseline_id"] == hist_bl["baseline_id"]
                assert c["check_status"] in ["pass", "warn", "block"]
                assert c["total_metrics"] >= c["pass_count"] + c["warn_count"] + c["block_count"]
                assert "details" in c or "details_json" in c or c.get("details") is not None

            last_check = checks[0]
            assert "checked_at" in last_check and last_check["checked_at"]
            assert "recommended_action" in last_check

            bl_hist = svc_hist.get_baseline(hist_bl["baseline_id"])
            assert bl_hist["last_check_status"] is not None
            assert bl_hist.get("last_check_summary") is not None or bl_hist.get("last_check_summary_json") is not None

            r77.ok()
            print(f"  [PASS] {r77.name}  ({len(checks)} 条复核记录, 字段完整)")
        except Exception as e:
            r77.fail(str(e))
            print(f"  [FAIL] {r77.name}: {e}")

        # ====== 测试 78: 基线ZIP内容完整性验证 ======
        r78 = TestResult("测试78: 基线ZIP内容完整性（6个必需文件+校验和）")
        results.append(r78)
        try:
            import zipfile, hashlib
            svc_zip = PipelineService(db_path)
            zip_batch = svc_zip.create_batch("baseline_zip_batch", SAMPLE_CSV)
            svc_zip.process_batch(zip_batch)

            zip_bl = svc_zip.register_baseline(
                "zip_integrity_baseline", zip_batch,
                description="ZIP完整性测试", warn_pct=4.0, block_pct=12.0
            )

            zip_path = os.path.join(tmpdir, "baseline_integrity.zip")
            svc_zip.export_baseline(zip_bl["baseline_id"], output_path=zip_path)

            required_files = [
                "baseline_summary.json", "config.json",
                "metric_thresholds.json", "source_summary.json",
                "check_history.json", "checksum.json"
            ]
            with zipfile.ZipFile(zip_path, "r") as zf:
                zip_names = zf.namelist()
                for rf in required_files:
                    assert rf in zip_names, f"ZIP 中缺少必需文件: {rf}"

                checksum_data = json.loads(zf.read("checksum.json").decode("utf-8"))
                assert "files" in checksum_data
                assert "baseline_format_version" in checksum_data

                for fname, meta in checksum_data["files"].items():
                    assert "sha256" in meta, f"{fname} 缺少 sha256"
                    assert "file_size" in meta, f"{fname} 缺少 file_size"
                    content = zf.read(fname)
                    calc_sha = hashlib.sha256(content).hexdigest()
                    assert calc_sha == meta["sha256"], f"{fname} 校验和不匹配"

                summary = json.loads(zf.read("baseline_summary.json").decode("utf-8"))
                assert summary["baseline_name"] == "zip_integrity_baseline"
                assert summary["format_version"] == "1.0"
                assert "config_version" in summary
                assert "source" in summary
                assert "source_batch_name" in summary["source"]

                thresholds = json.loads(zf.read("metric_thresholds.json").decode("utf-8"))
                assert "default_warn_pct" in thresholds
                assert "default_block_pct" in thresholds
                assert "metrics" in thresholds

                check_hist = json.loads(zf.read("check_history.json").decode("utf-8"))
                assert "total_checks" in check_hist
                assert "checks" in check_hist

            r78.ok()
            print(f"  [PASS] {r78.name}  (6文件+校验和全部 OK)")
        except Exception as e:
            r78.fail(str(e))
            print(f"  [FAIL] {r78.name}: {e}")

        # ====== 测试 79: 基线三级状态判定逻辑 ======
        r79 = TestResult("测试79: 基线三级状态判定（block>warn>pass优先级）")
        results.append(r79)
        try:
            from pipeline import baseline as bl_mod
            from pipeline import database as db

            svc_tier = PipelineService(db_path)
            tier_batch = svc_tier.create_batch("tier_batch", SAMPLE_CSV)
            svc_tier.process_batch(tier_batch)

            tier_bl = svc_tier.register_baseline(
                "tier_priority_baseline", tier_batch,
                description="三级优先级测试", warn_pct=5.0, block_pct=15.0
            )

            conn = svc_tier._conn()
            try:
                full_bl = db.get_baseline(conn, tier_bl["baseline_id"])
                thresholds = full_bl["metric_thresholds"]
                metrics_keys = list(thresholds["metrics"].keys())
                assert len(metrics_keys) >= 3, "至少需要3个指标测试三级状态"

                metrics_test = {
                    metrics_keys[0]: {"status": db.BASELINE_CHECK_PASS, "diff_pct": 1.0},
                    metrics_keys[1]: {"status": db.BASELINE_CHECK_WARN, "diff_pct": 10.0},
                    metrics_keys[2]: {"status": db.BASELINE_CHECK_BLOCK, "diff_pct": 25.0},
                }

                block_metrics = [k for k, v in metrics_test.items() if v["status"] == db.BASELINE_CHECK_BLOCK]
                warn_metrics = [k for k, v in metrics_test.items() if v["status"] == db.BASELINE_CHECK_WARN]
                pass_metrics = [k for k, v in metrics_test.items() if v["status"] == db.BASELINE_CHECK_PASS]

                conn.close()

                tier_check = svc_tier.check_baseline(tier_bl["baseline_id"], tier_batch)
                assert tier_check.block_count >= 0
                assert tier_check.warn_count >= 0
                assert tier_check.pass_count >= 0
                assert tier_check.total_metrics == tier_check.pass_count + tier_check.warn_count + tier_check.block_count

                if tier_check.block_count > 0:
                    assert tier_check.overall_status == db.BASELINE_CHECK_BLOCK, "有阻断指标时应为 block"
                elif tier_check.warn_count > 0:
                    assert tier_check.overall_status == db.BASELINE_CHECK_WARN, "无阻断但有警告时应为 warn"
                else:
                    assert tier_check.overall_status == db.BASELINE_CHECK_PASS, "全部通过应为 pass"

                for mr in tier_check.metric_results:
                    assert mr.status in ["pass", "warn", "block"]
                    assert mr.baseline_value is not None
                    assert mr.actual_value is not None

            finally:
                try:
                    conn.close()
                except:
                    pass

            r79.ok()
            print(f"  [PASS] {r79.name}  (三级判定逻辑: pass={tier_check.pass_count}, warn={tier_check.warn_count}, block={tier_check.block_count})")
        except Exception as e:
            r79.fail(str(e))
            print(f"  [FAIL] {r79.name}: {e}")

        # ====== 测试 80: 基线审计日志完整链路 ======
        r80 = TestResult("测试80: 基线审计日志完整链路（注册/复核/导出/导入/删除）")
        results.append(r80)
        try:
            svc_audit = PipelineService(db_path)
            audit_batch = svc_audit.create_batch("audit_bl_batch", SAMPLE_CSV)
            svc_audit.process_batch(audit_batch)

            audit_bl = svc_audit.register_baseline(
                "audit_chain_baseline", audit_batch,
                description="审计链路测试基线", warn_pct=5.0, block_pct=15.0
            )

            svc_audit.check_baseline(audit_bl["baseline_id"], audit_batch)

            audit_zip = os.path.join(tmpdir, "audit_bl.zip")
            svc_audit.export_baseline(audit_bl["baseline_id"], output_path=audit_zip)

            svc_audit.import_baseline(audit_zip, on_conflict="rename", new_name="audit_chain_imported")

            svc_audit.delete_baseline(audit_bl["baseline_id"])

            all_logs = svc_audit.get_baseline_audit_logs(limit=100)
            actions_found = set(log["action"] for log in all_logs)

            required_actions = {
                db.AUDIT_ACTION_BASELINE_REGISTER,
                db.AUDIT_ACTION_BASELINE_CHECK,
                db.AUDIT_ACTION_BASELINE_EXPORT,
                db.AUDIT_ACTION_BASELINE_IMPORT,
                db.AUDIT_ACTION_BASELINE_DELETE,
            }
            missing = required_actions - actions_found
            assert not missing, f"缺少审计日志动作: {missing}"

            del_bl = svc_audit.get_baseline(audit_bl["baseline_id"])
            assert del_bl["status"] == db.BASELINE_STATUS_DELETED, "软删除后状态应为 deleted"

            delete_logs = svc_audit.get_baseline_audit_logs(
                baseline_id=audit_bl["baseline_id"],
                action=db.AUDIT_ACTION_BASELINE_DELETE
            )
            assert len(delete_logs) >= 1
            assert delete_logs[0]["result"] == db.AUDIT_RESULT_SUCCESS

            r80.ok()
            print(f"  [PASS] {r80.name}  (5种动作审计齐全, 软删除 OK)")
        except Exception as e:
            r80.fail(str(e))
            print(f"  [FAIL] {r80.name}: {e}")

        # ====== 测试 81: CLI baseline 命令输出与文档对齐 ======
        r81 = TestResult("测试81: CLI baseline 命令输出与文档对齐")
        results.append(r81)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()
            svc_cli_bl = PipelineService(db_path)

            cli_bl_batch = svc_cli_bl.create_batch("cli_baseline_batch", SAMPLE_CSV)
            svc_cli_bl.process_batch(cli_bl_batch)

            res_reg = runner.invoke(cli, [
                "--db", db_path, "baseline", "register",
                "cli_test_baseline", str(cli_bl_batch),
                "--description", "CLI测试基线"
            ])
            assert res_reg.exit_code == 0, f"register 应成功，exit={res_reg.exit_code}, output={res_reg.output}"
            assert "[OK]" in res_reg.output
            assert "基线已注册" in res_reg.output
            assert "基线ID" in res_reg.output
            assert "基线名称" in res_reg.output
            assert "配置版本" in res_reg.output
            assert "指标数量" in res_reg.output
            assert "警告阈值" in res_reg.output
            assert "阻断阈值" in res_reg.output

            res_check = runner.invoke(cli, [
                "--db", db_path, "baseline", "check",
                "1", str(cli_bl_batch)
            ])
            if res_check.exit_code == 0:
                assert "基线复核完成" in res_check.output
                assert "总体结论" in res_check.output
                assert "通过" in res_check.output or "警告" in res_check.output or "阻断" in res_check.output
                assert "建议动作" in res_check.output

            cli_bl_zip = os.path.join(tmpdir, "cli_bl.zip")
            cli_bl_id = None
            for line in res_reg.output.split("\n"):
                if "基线ID" in line:
                    import re
                    m = re.search(r"基线ID:\s*(\d+)", line)
                    if m:
                        cli_bl_id = int(m.group(1))
                        break
            assert cli_bl_id is not None, "无法从register输出解析基线ID"

            res_export = runner.invoke(cli, [
                "--db", db_path, "baseline", "export",
                str(cli_bl_id), "-o", cli_bl_zip
            ])
            assert res_export.exit_code == 0, f"export 应成功，exit={res_export.exit_code}, output={res_export.output}"
            assert "[OK]" in res_export.output
            assert "基线已导出" in res_export.output
            assert "SHA256" in res_export.output
            assert "文件大小" in res_export.output

            res_list = runner.invoke(cli, ["--db", db_path, "baseline", "list"])
            assert res_list.exit_code == 0
            assert "ID" in res_list.output
            assert "名称" in res_list.output
            assert "状态" in res_list.output
            assert "cli_test_baseline" in res_list.output

            res_show = runner.invoke(cli, ["--db", db_path, "baseline", "show", str(cli_bl_id), "--thresholds"])
            assert res_show.exit_code == 0
            assert "基线 #" in res_show.output
            assert "配置版本" in res_show.output
            assert "来源批次" in res_show.output
            assert "指标阈值" in res_show.output

            res_imp_reject = runner.invoke(cli, [
                "--db", db_path, "baseline", "import",
                cli_bl_zip, "--on-conflict", "reject"
            ])
            assert res_imp_reject.exit_code == 0
            assert "导入拒绝" in res_imp_reject.output or "导入" in res_imp_reject.output

            res_hist = runner.invoke(cli, [
                "--db", db_path, "baseline", "history"
            ])
            assert res_hist.exit_code == 0
            assert "ID" in res_hist.output
            assert "操作" in res_hist.output
            assert "注册" in res_hist.output or "复核" in res_hist.output or "导出" in res_hist.output

            r81.ok()
            print(f"  [PASS] {r81.name}  (register/check/export/list/show/import/history 对齐)")
        except Exception as e:
            r81.fail(str(e))
            print(f"  [FAIL] {r81.name}: {e}")

        # ====== 复核工单测试 ======

        r82 = TestResult("测试82: 工单创建后重启持久化（工单和备注时间线完整可查）")
        results.append(r82)
        try:
            ticket_db = os.path.join(tmpdir, "ticket_test.db")
            svc_t = PipelineService(ticket_db)
            svc_t.create_batch("ticket_batch_001", SAMPLE_CSV)

            tid1 = svc_t.create_ticket("复核失败-批次001", source_batch_id=1,
                                       trigger_rule="baseline_block", assignee="zhangsan")
            svc_t.resolve_ticket(tid1, "已修正阈值配置")

            tid2 = svc_t.create_ticket("告警过多-批次002", source_batch_id=1,
                                       trigger_rule="alert_overflow")
            svc_t.assign_ticket(tid2, "lisi")
            svc_t.add_ticket_note(tid2, "正在排查告警来源", author="lisi")

            del svc_t
            svc_t2 = PipelineService(ticket_db)

            t1 = svc_t2.get_ticket(tid1)
            assert t1 is not None, "重启后工单1不存在"
            assert t1["title"] == "复核失败-批次001"
            assert t1["status"] == db.TICKET_STATUS_RESOLVED
            assert t1["resolution"] == "已修正阈值配置"
            assert t1["assignee"] == "zhangsan"
            assert t1["trigger_rule"] == "baseline_block"

            notes1 = svc_t2.get_ticket_notes(tid1)
            assert len(notes1) >= 2, f"工单1至少有2条备注(创建+关闭), 实际{len(notes1)}"
            note_types = [n["note_type"] for n in notes1]
            assert db.TICKET_NOTE_ASSIGN in note_types or db.TICKET_NOTE_STATUS_CHANGE in note_types
            assert db.TICKET_NOTE_RESOLVE in note_types

            t2 = svc_t2.get_ticket(tid2)
            assert t2 is not None, "重启后工单2不存在"
            assert t2["status"] == db.TICKET_STATUS_ASSIGNED
            assert t2["assignee"] == "lisi"

            notes2 = svc_t2.get_ticket_notes(tid2)
            assert len(notes2) >= 2, f"工单2至少有2条备注, 实际{len(notes2)}"
            comment_notes = [n for n in notes2 if n["note_type"] == db.TICKET_NOTE_COMMENT]
            assert len(comment_notes) >= 1, "工单2应有手动备注"
            assert "排查" in comment_notes[0]["content"]

            r82.ok()
            print(f"  [PASS] {r82.name}")
        except Exception as e:
            r82.fail(str(e))
            print(f"  [FAIL] {r82.name}: {e}")

        r83 = TestResult("测试83: 责任人筛选（list --assignee 过滤正确）")
        results.append(r83)
        try:
            ticket_db2 = os.path.join(tmpdir, "ticket_filter.db")
            svc_f = PipelineService(ticket_db2)
            svc_f.create_batch("filter_batch", SAMPLE_CSV)

            svc_f.create_ticket("工单A", source_batch_id=1, assignee="alice")
            svc_f.create_ticket("工单B", source_batch_id=1, assignee="bob")
            svc_f.create_ticket("工单C", source_batch_id=1, assignee="alice")

            alice_tickets = svc_f.list_tickets(assignee="alice")
            assert len(alice_tickets) == 2, f"alice应有2个工单, 实际{len(alice_tickets)}"
            assert all(t["assignee"] == "alice" for t in alice_tickets)

            bob_tickets = svc_f.list_tickets(assignee="bob")
            assert len(bob_tickets) == 1, f"bob应有1个工单, 实际{len(bob_tickets)}"
            assert bob_tickets[0]["title"] == "工单B"

            all_tickets = svc_f.list_tickets()
            assert len(all_tickets) == 3, f"总共应有3个工单, 实际{len(all_tickets)}"

            r83.ok()
            print(f"  [PASS] {r83.name}")
        except Exception as e:
            r83.fail(str(e))
            print(f"  [FAIL] {r83.name}: {e}")

        r84 = TestResult("测试84: 关闭后重新打开（resolve → reopen 流程完整）")
        results.append(r84)
        try:
            ticket_db3 = os.path.join(tmpdir, "ticket_reopen.db")
            svc_r = PipelineService(ticket_db3)
            svc_r.create_batch("reopen_batch", SAMPLE_CSV)

            tid = svc_r.create_ticket("基线阻断-批次003", source_batch_id=1,
                                      trigger_rule="baseline_block")
            svc_r.assign_ticket(tid, "wangwu")
            svc_r.resolve_ticket(tid, "已修复数据源问题")

            t = svc_r.get_ticket(tid)
            assert t["status"] == db.TICKET_STATUS_RESOLVED
            assert t["resolution"] == "已修复数据源问题"

            svc_r.reopen_ticket(tid, "问题再次出现，告警仍未消除")
            t2 = svc_r.get_ticket(tid)
            assert t2["status"] == db.TICKET_STATUS_REOPENED

            notes = svc_r.get_ticket_notes(tid)
            reopen_notes = [n for n in notes if n["note_type"] == db.TICKET_NOTE_REOPEN]
            assert len(reopen_notes) >= 1, "应有重开备注"
            assert "再次出现" in reopen_notes[0]["content"]

            svc_r.assign_ticket(tid, "zhaoliu")
            svc_r.resolve_ticket(tid, "二次修复完成")
            t3 = svc_r.get_ticket(tid)
            assert t3["status"] == db.TICKET_STATUS_RESOLVED
            assert t3["resolution"] == "二次修复完成"

            all_notes = svc_r.get_ticket_notes(tid)
            resolve_notes = [n for n in all_notes if n["note_type"] == db.TICKET_NOTE_RESOLVE]
            assert len(resolve_notes) == 2, f"应有2条关闭备注, 实际{len(resolve_notes)}"

            svc_r.reopen_ticket(tid, "二次修复仍不够")
            t_again = svc_r.get_ticket(tid)
            assert t_again["status"] == db.TICKET_STATUS_REOPENED, "二次resolve后应能再次reopen"

            r84.ok()
            print(f"  [PASS] {r84.name}")
        except Exception as e:
            r84.fail(str(e))
            print(f"  [FAIL] {r84.name}: {e}")

        r85 = TestResult("测试85: 冲突导入-reject（同标题时拒绝，审计日志记录）")
        results.append(r85)
        try:
            ticket_db4 = os.path.join(tmpdir, "ticket_reject.db")
            svc_rej = PipelineService(ticket_db4)
            svc_rej.create_batch("reject_batch", SAMPLE_CSV)

            svc_rej.create_ticket("重复标题工单", source_batch_id=1)

            export_path = os.path.join(tmpdir, "ticket_reject_export.json")
            svc_rej.export_ticket(1, export_path)

            result = svc_rej.import_ticket(export_path, on_conflict=TicketImportResult.ACTION_REJECT)
            assert not result.success, "reject策略应返回success=False"
            assert result.action == TicketImportResult.ACTION_REJECT
            assert "拒绝" in result.message or "已存在" in result.message

            logs = svc_rej.get_ticket_audit_logs(action=db.AUDIT_ACTION_TICKET_IMPORT)
            blocked_logs = [l for l in logs if l.get("result") == db.AUDIT_RESULT_BLOCKED]
            assert len(blocked_logs) >= 1, "reject策略应有blocked审计日志"

            r85.ok()
            print(f"  [PASS] {r85.name}")
        except Exception as e:
            r85.fail(str(e))
            print(f"  [FAIL] {r85.name}: {e}")

        r86 = TestResult("测试86: 冲突导入-rename（同标题时重命名成功，追溯字段完整）")
        results.append(r86)
        try:
            ticket_db5 = os.path.join(tmpdir, "ticket_rename.db")
            svc_rn = PipelineService(ticket_db5)
            svc_rn.create_batch("rename_batch", SAMPLE_CSV)

            svc_rn.create_ticket("重复标题工单B", source_batch_id=1)

            export_path2 = os.path.join(tmpdir, "ticket_rename_export.json")
            svc_rn.export_ticket(1, export_path2)

            result = svc_rn.import_ticket(export_path2, on_conflict=TicketImportResult.ACTION_RENAME)
            assert result.success, "rename策略应成功"
            assert result.action == TicketImportResult.ACTION_RENAME
            assert result.final_title != result.original_title
            assert result.original_title == "重复标题工单B"
            assert "imported" in result.final_title
            assert result.original_ticket_id == 1

            renamed_ticket = svc_rn.get_ticket(result.ticket_id)
            assert renamed_ticket is not None
            assert renamed_ticket["original_ticket_id"] == 1
            assert renamed_ticket["imported_from"] is not None

            logs = svc_rn.get_ticket_audit_logs(action=db.AUDIT_ACTION_TICKET_IMPORT)
            success_logs = [l for l in logs if l.get("result") == db.AUDIT_RESULT_SUCCESS]
            assert len(success_logs) >= 1, "rename导入应有成功审计日志"

            r86.ok()
            print(f"  [PASS] {r86.name}")
        except Exception as e:
            r86.fail(str(e))
            print(f"  [FAIL] {r86.name}: {e}")

        r87 = TestResult("测试87: 导入后继续处理（导入后可分配、关闭、重开）")
        results.append(r87)
        try:
            ticket_db6 = os.path.join(tmpdir, "ticket_continue.db")
            svc_c = PipelineService(ticket_db6)
            svc_c.create_batch("continue_batch", SAMPLE_CSV)

            tid_src = svc_c.create_ticket("待导入工单", source_batch_id=1,
                                          trigger_rule="alert_overflow", assignee="alice")
            svc_c.resolve_ticket(tid_src, "初步修复")

            export_path3 = os.path.join(tmpdir, "ticket_continue_export.json")
            svc_c.export_ticket(tid_src, export_path3)

            ticket_db6b = os.path.join(tmpdir, "ticket_continue_target.db")
            svc_c2 = PipelineService(ticket_db6b)
            svc_c2.create_batch("target_batch", SAMPLE_CSV)

            result = svc_c2.import_ticket(export_path3)
            assert result.success
            imported_tid = result.ticket_id

            imported = svc_c2.get_ticket(imported_tid)
            assert imported is not None

            notes_imported = svc_c2.get_ticket_notes(imported_tid)
            assert len(notes_imported) >= 2, f"导入工单应含原始备注, 实际{len(notes_imported)}"

            svc_c2.reopen_ticket(imported_tid, "导入后发现新问题")
            svc_c2.assign_ticket(imported_tid, "bob")
            t_assigned = svc_c2.get_ticket(imported_tid)
            assert t_assigned["assignee"] == "bob"
            assert t_assigned["status"] == db.TICKET_STATUS_ASSIGNED

            svc_c2.resolve_ticket(imported_tid, "彻底修复")
            t_resolved = svc_c2.get_ticket(imported_tid)
            assert t_resolved["status"] == db.TICKET_STATUS_RESOLVED
            assert t_resolved["resolution"] == "彻底修复"

            final_notes = svc_c2.get_ticket_notes(imported_tid)
            note_types = [n["note_type"] for n in final_notes]
            assert db.TICKET_NOTE_REOPEN in note_types, "应有重开备注"
            assert db.TICKET_NOTE_RESOLVE in note_types, "应有关闭备注"
            assert note_types.count(db.TICKET_NOTE_RESOLVE) >= 2, "应有2条关闭备注(原始+新)"

            r87.ok()
            print(f"  [PASS] {r87.name}")
        except Exception as e:
            r87.fail(str(e))
            print(f"  [FAIL] {r87.name}: {e}")

        r88 = TestResult("测试88: 历史追溯（notes 时间线和审计日志完整追溯全生命周期）")
        results.append(r88)
        try:
            ticket_db7 = os.path.join(tmpdir, "ticket_trace.db")
            svc_tr = PipelineService(ticket_db7)
            svc_tr.create_batch("trace_batch", SAMPLE_CSV)

            tid = svc_tr.create_ticket("追溯测试工单", source_batch_id=1,
                                       trigger_rule="baseline_block")
            svc_tr.assign_ticket(tid, "user1")
            svc_tr.add_ticket_note(tid, "检查了基线配置", author="user1")
            svc_tr.resolve_ticket(tid, "基线配置已修正")
            svc_tr.reopen_ticket(tid, "修正后仍有问题")
            svc_tr.assign_ticket(tid, "user2")
            svc_tr.add_ticket_note(tid, "user2开始排查", author="user2")
            svc_tr.resolve_ticket(tid, "最终修复完成")

            notes = svc_tr.get_ticket_notes(tid)
            assert len(notes) >= 7, f"应有至少7条备注, 实际{len(notes)}"

            expected_types = [
                db.TICKET_NOTE_ASSIGN,
                db.TICKET_NOTE_COMMENT,
                db.TICKET_NOTE_RESOLVE,
                db.TICKET_NOTE_REOPEN,
                db.TICKET_NOTE_ASSIGN,
                db.TICKET_NOTE_COMMENT,
                db.TICKET_NOTE_RESOLVE,
            ]
            actual_types = [n["note_type"] for n in notes]
            for et in expected_types:
                assert et in actual_types, f"缺少备注类型: {et}"

            for n in notes:
                assert n.get("created_at"), f"备注缺少创建时间: {n}"
                assert n.get("content"), f"备注缺少内容: {n}"

            from datetime import datetime as dt
            note_times = []
            for n in notes:
                try:
                    note_times.append(dt.fromisoformat(n["created_at"]))
                except (ValueError, TypeError):
                    pass
            if len(note_times) >= 2:
                for i in range(len(note_times) - 1):
                    assert note_times[i] <= note_times[i + 1], \
                        f"备注时间线不按顺序: {notes[i]['created_at']} > {notes[i+1]['created_at']}"

            audit_logs = svc_tr.get_ticket_audit_logs(action=db.AUDIT_ACTION_TICKET_CREATE)
            assert len(audit_logs) >= 1, "应有创建审计日志"

            assign_logs = svc_tr.get_ticket_audit_logs(action=db.AUDIT_ACTION_TICKET_ASSIGN)
            assert len(assign_logs) >= 2, f"应有2条分配审计日志, 实际{len(assign_logs)}"

            resolve_logs = svc_tr.get_ticket_audit_logs(action=db.AUDIT_ACTION_TICKET_RESOLVE)
            assert len(resolve_logs) >= 2, f"应有2条关闭审计日志, 实际{len(resolve_logs)}"

            reopen_logs = svc_tr.get_ticket_audit_logs(action=db.AUDIT_ACTION_TICKET_REOPEN)
            assert len(reopen_logs) >= 1, "应有1条重开审计日志"

            r88.ok()
            print(f"  [PASS] {r88.name}")
        except Exception as e:
            r88.fail(str(e))
            print(f"  [FAIL] {r88.name}: {e}")

        r89 = TestResult("测试89: CLI ticket 命令输出与文档对齐")
        results.append(r89)
        try:
            from click.testing import CliRunner
            from pipeline.cli import cli

            runner = CliRunner()
            ticket_db8 = os.path.join(tmpdir, "ticket_cli.db")
            svc_cli = PipelineService(ticket_db8)
            svc_cli.create_batch("cli_ticket_batch", SAMPLE_CSV)

            res_create = runner.invoke(cli, [
                "--db", ticket_db8, "ticket", "create",
                "CLI测试工单", "--batch-id", "1", "--trigger-rule", "baseline_block"
            ])
            assert res_create.exit_code == 0, f"create 失败: {res_create.output}"
            assert "[OK]" in res_create.output
            assert "工单已创建" in res_create.output
            assert "标题" in res_create.output
            assert "CLI测试工单" in res_create.output

            import re
            cli_tid = None
            for line in res_create.output.split("\n"):
                m = re.search(r"工单ID:\s*(\d+)", line)
                if m:
                    cli_tid = int(m.group(1))
                    break
            assert cli_tid is not None, "无法从create输出解析工单ID"

            res_list = runner.invoke(cli, ["--db", ticket_db8, "ticket", "list"])
            assert res_list.exit_code == 0, f"list 失败: {res_list.output}"
            assert "ID" in res_list.output or "标题" in res_list.output

            res_show = runner.invoke(cli, ["--db", ticket_db8, "ticket", "show", str(cli_tid)])
            assert res_show.exit_code == 0, f"show 失败: {res_show.output}"
            assert "工单 #" in res_show.output
            assert "状态" in res_show.output
            assert "备注时间线" in res_show.output

            res_assign = runner.invoke(cli, [
                "--db", ticket_db8, "ticket", "assign",
                str(cli_tid), "alice"
            ])
            assert res_assign.exit_code == 0, f"assign 失败: {res_assign.output}"
            assert "[OK]" in res_assign.output
            assert "已分配" in res_assign.output

            res_resolve = runner.invoke(cli, [
                "--db", ticket_db8, "ticket", "resolve",
                str(cli_tid), "已修复"
            ])
            assert res_resolve.exit_code == 0, f"resolve 失败: {res_resolve.output}"
            assert "[OK]" in res_resolve.output
            assert "已关闭" in res_resolve.output

            res_reopen = runner.invoke(cli, [
                "--db", ticket_db8, "ticket", "reopen",
                str(cli_tid), "问题复现"
            ])
            assert res_reopen.exit_code == 0, f"reopen 失败: {res_reopen.output}"
            assert "[OK]" in res_reopen.output
            assert "重新打开" in res_reopen.output

            export_file = os.path.join(tmpdir, "cli_ticket_export.json")
            res_export = runner.invoke(cli, [
                "--db", ticket_db8, "ticket", "export",
                str(cli_tid), "-o", export_file
            ])
            assert res_export.exit_code == 0, f"export 失败: {res_export.output}"
            assert "[OK]" in res_export.output
            assert "已导出" in res_export.output
            assert os.path.exists(export_file)

            with open(export_file, "r", encoding="utf-8") as ef:
                export_data = json.load(ef)
            assert "title" in export_data
            assert "notes" in export_data
            assert len(export_data["notes"]) >= 3, "导出应含处理历史"

            res_import = runner.invoke(cli, [
                "--db", ticket_db8, "ticket", "import",
                export_file, "--on-conflict", "rename"
            ])
            assert res_import.exit_code == 0, f"import 失败: {res_import.output}"
            assert "导入" in res_import.output or "重命名" in res_import.output

            res_list_assignee = runner.invoke(cli, [
                "--db", ticket_db8, "ticket", "list", "--assignee", "alice"
            ])
            assert res_list_assignee.exit_code == 0

            r89.ok()
            print(f"  [PASS] {r89.name}")
        except Exception as e:
            r89.fail(str(e))
            print(f"  [FAIL] {r89.name}: {e}")

        # ====== 汇总 ======
        print()
        total = len(results)
        passed = sum(1 for r in results if r.passed)
        failed = total - passed
        print(f"====== 回归测试结果: {passed}/{total} 通过 ======")
        for r in results:
            mark = "[OK]" if r.passed else "[FAIL]"
            detail = f" - {r.error}" if r.error else ""
            print(f"  {mark} {r.name}{detail}")

        if failed > 0:
            print(f"\n有 {failed} 项测试失败！")
            return 1
        else:
            print("\n所有回归测试通过！")
            return 0

    finally:
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except:
            pass


if __name__ == "__main__":
    sys.exit(run_tests())
