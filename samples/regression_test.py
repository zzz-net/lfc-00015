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
    SchemeError, SchemeConflictError, SchemeImportResult, SchemeCloneResult
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
