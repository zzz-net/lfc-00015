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
    SchemeError, SchemeConflictError, SchemeImportResult
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
