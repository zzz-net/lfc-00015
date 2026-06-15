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


class PipelineRegressionTest:
    def __init__(self):
        self.results = []
        self.tmpdir = None
        self.db_path = None
        self.out_dir = None

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="pipeline_regression_")
        self.db_path = os.path.join(self.tmpdir, "test.db")
        self.out_dir = os.path.join(self.tmpdir, "exports")
        os.makedirs(self.out_dir, exist_ok=True)

    def tearDown(self):
        try:
            if self.tmpdir:
                shutil.rmtree(self.tmpdir, ignore_errors=True)
        except:
            pass

    def _run_test(self, test_num, test_name, test_func):
        r = TestResult(f"测试{test_num}: {test_name}")
        self.results.append(r)
        try:
            self.setUp()
            test_func()
            r.ok()
            print(f"  [PASS] {r.name}")
        except Exception as e:
            r.fail(str(e))
            print(f"  [FAIL] {r.name}: {e}")
        finally:
            self.tearDown()
        return r

    def run_all(self):
        print("====== 开始回归测试 ======")

        test_methods = []
        for attr in dir(self):
            if attr.startswith('test_') and callable(getattr(self, attr)):
                try:
                    num = int(attr.split('_')[1])
                    test_methods.append((num, attr))
                except (IndexError, ValueError):
                    pass

        test_methods.sort(key=lambda x: x[0])

        for num, attr in test_methods:
            method = getattr(self, attr)
            doc = method.__doc__ or ''
            self._run_test(num, doc.strip() if doc else attr, method)

        print()
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        print(f"====== 回归测试结果: {passed}/{total} 通过 ======")
        for r in self.results:
            mark = "[OK]" if r.passed else "[FAIL]"
            detail = f" - {r.error}" if r.error else ""
            print(f"  {mark} {r.name}{detail}")

        if failed > 0:
            print(f"\n有 {failed} 项测试失败！")
            return 1
        else:
            print("\n所有回归测试通过！")
            return 0

    def test_01(self):
        """锁定后无条件拒绝 process，run 数量不变"""
        svc = PipelineService(self.db_path)
        batch_id = svc.create_batch("reg_001", SAMPLE_CSV)
        run_id1, run_n1 = svc.process_batch(batch_id)
        assert run_n1 == 1

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

    def test_02(self):
        """锁定后改源 CSV 再 process，默认导出不被污染"""
        svc = PipelineService(self.db_path)
        batch_id = svc.create_batch("reg_001", SAMPLE_CSV)
        run_id1, run_n1 = svc.process_batch(batch_id)
        assert run_n1 == 1
        metrics_v1 = svc.get_run_metrics(run_id1)

        modified_csv = os.path.join(self.tmpdir, "modified.csv")
        with open(SAMPLE_CSV, "r", encoding="utf-8") as f:
            content = f.read()
        modified_content = content.replace("23.5", "9999.0")
        with open(modified_csv, "w", encoding="utf-8") as f:
            f.write(modified_content)

        conn = db.get_connection(self.db_path)
        try:
            conn.execute("UPDATE batches SET source_file = ? WHERE id = ?", (modified_csv, batch_id))
            conn.commit()
        finally:
            conn.close()

        svc.lock_batch(batch_id)

        try:
            svc.process_batch(batch_id)
            assert False, "锁定后 process_batch 应抛出 BatchLockedError"
        except BatchLockedError:
            pass

        path1 = svc.export_metrics(batch_id, os.path.join(self.out_dir, "m1.csv"))
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

    def test_03(self):
        """跨重启后锁定状态、run 列表、导出指标一致"""
        svc = PipelineService(self.db_path)
        batch_id = svc.create_batch("reg_001", SAMPLE_CSV)
        run_id1, run_n1 = svc.process_batch(batch_id)
        assert run_n1 == 1
        svc.lock_batch(batch_id)

        path1 = svc.export_metrics(batch_id, os.path.join(self.out_dir, "m1.csv"))
        with open(path1, "r", encoding="utf-8-sig") as f:
            lines1 = f.readlines()

        del svc
        svc2 = PipelineService(self.db_path)

        batch2 = svc2.get_batch(batch_id)
        assert batch2["status"] == "locked", f"重启后状态应为 locked, 实际 {batch2['status']}"
        assert batch2["locked"] == 1

        runs2 = svc2.list_runs(batch_id)
        assert len(runs2) == 1, f"重启后 run 数应为 1, 实际 {len(runs2)}"

        path2 = svc2.export_metrics(batch_id, os.path.join(self.out_dir, "m2.csv"))
        with open(path2, "r", encoding="utf-8-sig") as f:
            lines2 = f.readlines()
        assert lines1 == lines2, "跨重启导出的 metrics CSV 应逐字节一致"

    def test_04(self):
        """未锁定批次重跑: run_number 递增，各 run 配置版本正确"""
        svc = PipelineService(self.db_path)
        batch_id = svc.create_batch("reg_001", SAMPLE_CSV)
        run_id1, run_n1 = svc.process_batch(batch_id)
        assert run_n1 == 1

        new_cfg = svc.set_threshold(batch_id, zscore_threshold=1.0)
        run_id2, run_n2 = svc.process_batch(batch_id)
        assert run_n2 == 2

        run2 = svc.get_run(run_id2)
        assert run2["config_version"] == new_cfg["version"], \
            f"run2 配置版本应为 v{new_cfg['version']}, 实际 v{run2['config_version']}"

        run1 = svc.get_run(run_id1)
        assert run1["config_version"] == 1, f"run1 配置版本应为 v1, 实际 v{run1['config_version']}"

        runs = svc.list_runs(batch_id)
        assert len(runs) == 2

    def test_05(self):
        """导出 metrics/errors/anomalies CSV 含 run_number 和 config_version 字段"""
        svc = PipelineService(self.db_path)
        batch_id = svc.create_batch("reg_001", SAMPLE_CSV)
        svc.process_batch(batch_id)
        new_cfg = svc.set_threshold(batch_id, zscore_threshold=1.0)
        run_id2, run_n2 = svc.process_batch(batch_id)

        mp = svc.export_metrics(batch_id, os.path.join(self.out_dir, "f_m.csv"), run_id2)
        ep = svc.export_errors(batch_id, os.path.join(self.out_dir, "f_e.csv"), run_id2)
        ap = svc.export_anomalies(batch_id, os.path.join(self.out_dir, "f_a.csv"), run_id2)

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

    def test_06(self):
        """UTF-8 BOM 的 CSV 可正常导入"""
        bom_path = os.path.join(self.tmpdir, "with_bom.csv")
        with open(bom_path, "wb") as f:
            f.write(b"\xef\xbb\xbf")
            with open(SAMPLE_CSV, "rb") as src:
                f.write(src.read())

        df_bom, errs_bom = import_csv(bom_path)
        assert "timestamp" in df_bom.columns, f"BOM CSV 首列表头被污染，实际列: {list(df_bom.columns)}"
        assert not df_bom.columns[0].startswith("\ufeff"), "首列不应包含 BOM 字符"
        assert len(df_bom) > 0, "BOM CSV 应有有效行"

        svc = PipelineService(self.db_path)
        batch_id_bom = svc.create_batch("bom_batch", bom_path)
        svc.process_batch(batch_id_bom)
        batch_bom = svc.get_batch(batch_id_bom)
        assert batch_bom["status"] == "processed", f"BOM 批次状态应为 processed, 实际 {batch_bom['status']}"

    def test_07(self):
        """CLI process 帮助文本不含 --force，行为与文本一致"""
        from click.testing import CliRunner
        from pipeline.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["process", "--help"])
        help_text = result.output
        assert "--force" not in help_text, "CLI process --help 不应出现 --force"
        assert result.exit_code == 0

    def test_08(self):
        """分析方案保存后跨重启一致"""
        svc3 = PipelineService(self.db_path)
        batch_id = svc3.create_batch("reg_001", SAMPLE_CSV)
        svc3.process_batch(batch_id)

        scheme_name = "scheme_for_restart_test"
        sid = svc3.save_scheme(scheme_name, batch_id=batch_id, description="重启测试方案")
        scheme_before = svc3.get_scheme(sid)
        assert scheme_before["name"] == scheme_name
        assert "cleaning" in scheme_before["config"]
        assert "missing_values" in scheme_before["config"]

        del svc3
        svc4 = PipelineService(self.db_path)
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

    def test_09(self):
        """方案导入冲突处理（同名/字段缺失/版本不兼容）"""
        svc5 = PipelineService(self.db_path)
        batch_id = svc5.create_batch("reg_001", SAMPLE_CSV)
        svc5.process_batch(batch_id)
        sid = svc5.save_scheme("scheme_for_restart_test", batch_id=batch_id, description="重启测试方案")

        schemes_tmp = os.path.join(self.tmpdir, "schemes")
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

    def test_10(self):
        """锁定批次参与对比但不被改写，方案可关联报告"""
        svc6 = PipelineService(self.db_path)
        batch_id = svc6.create_batch("reg_001", SAMPLE_CSV)
        svc6.process_batch(batch_id)
        sid = svc6.save_scheme("scheme_for_restart_test", batch_id=batch_id, description="重启测试方案")

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
        svc7 = PipelineService(self.db_path)
        reports_list = svc7.list_comparison_reports()
        assert any(r["id"] == report["report_id"] for r in reports_list), \
            "重启后对比报告应保留"
        report_reloaded = svc7.get_comparison_report(report["report_id"])
        assert report_reloaded is not None
        assert report_reloaded["scheme_name"] is not None
        assert batch_id in report_reloaded["batch_ids"]

    def test_11(self):
        """对比报告导出 JSON/CSV 字段稳定一致"""
        svc8 = PipelineService(self.db_path)
        batch_id = svc8.create_batch("reg_001", SAMPLE_CSV)
        svc8.process_batch(batch_id)
        sid = svc8.save_scheme("scheme_for_restart_test", batch_id=batch_id, description="重启测试方案")

        second_batch_id = svc8.create_batch("second_for_compare", SAMPLE_CSV)
        svc8.set_threshold(second_batch_id, zscore_threshold=1.0)
        svc8.process_batch(second_batch_id)

        report = svc8.generate_comparison_report(
            "compare_locked_batch", [batch_id, second_batch_id], scheme_id=sid)

        existing_reports = svc8.list_comparison_reports()
        assert len(existing_reports) > 0, "应有已生成的报告"
        rid = existing_reports[0]["id"]

        json_out = os.path.join(self.out_dir, f"report_{rid}.json")
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

        csv_dir = os.path.join(self.out_dir, f"report_csv_{rid}")
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

    def test_12(self):
        """CLI scheme/compare 命令帮助文本完整"""
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

    def test_13(self):
        """scheme save 输出 INFO 日志，含方案名和批次标识"""
        import logging

        svc_log1 = PipelineService(self.db_path)

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

    def test_14(self):
        """scheme import 各分支输出对应 INFO 日志，含文件名和方案名"""
        import logging

        svc_log2 = PipelineService(self.db_path)
        batch_id = svc_log2.create_batch("reg_001", SAMPLE_CSV)
        svc_log2.process_batch(batch_id)
        sid = svc_log2.save_scheme("scheme_for_restart_test", batch_id=batch_id, description="重启测试方案")

        schemes_dir = os.path.join(self.tmpdir, "logtest_schemes")
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

    def test_15(self):
        """compare run 输出 INFO 日志，含报告名、方案名、批次标识"""
        import logging

        svc_log3 = PipelineService(self.db_path)
        batch_id = svc_log3.create_batch("reg_001", SAMPLE_CSV)
        svc_log3.process_batch(batch_id)
        sid = svc_log3.save_scheme("scheme_for_restart_test", batch_id=batch_id, description="重启测试方案")

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

    def test_16(self):
        """方案 clone 成功克隆，同名时抛出 name_exists 冲突"""
        svc_clone1 = PipelineService(self.db_path)
        batch_id = svc_clone1.create_batch("reg_001", SAMPLE_CSV)
        svc_clone1.process_batch(batch_id)

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

    def test_17(self):
        """clone-and-apply 成功克隆应用、同名冲突、锁定批次拒绝"""
        svc_clone2 = PipelineService(self.db_path)
        batch_id = svc_clone2.create_batch("reg_001", SAMPLE_CSV)
        svc_clone2.process_batch(batch_id)

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

    def test_18(self):
        """重启后克隆方案仍可查询，应用后的配置版本保留"""
        svc_pre = PipelineService(self.db_path)
        batch_id = svc_pre.create_batch("reg_001", SAMPLE_CSV)
        svc_pre.process_batch(batch_id)
        sid = svc_pre.save_scheme("scheme_for_restart_test", batch_id=batch_id, description="重启测试方案")

        pre_sid = svc_pre.clone_scheme(sid, "restart_cloned_scheme", "重启前克隆")
        pre_batch_id = svc_pre.create_batch("restart_clone_batch", SAMPLE_CSV)
        svc_pre.process_batch(pre_batch_id)
        pre_result = svc_pre.clone_and_apply_scheme(
            pre_sid, "restart_clone_apply", pre_batch_id, "重启前克隆并应用")
        expected_cfg_v = pre_result.new_config_version

        del svc_pre

        svc_post = PipelineService(self.db_path)

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

    def test_19(self):
        """克隆方案可正常导出并再导入，配置一致"""
        svc_cmp = PipelineService(self.db_path)
        batch_id = svc_cmp.create_batch("reg_001", SAMPLE_CSV)
        svc_cmp.process_batch(batch_id)
        sid = svc_cmp.save_scheme("scheme_for_restart_test", batch_id=batch_id, description="重启测试方案")

        compat_sid = svc_cmp.clone_scheme(sid, "compat_clone_source", "兼容性测试源")

        export_dir = os.path.join(self.tmpdir, "compat_exports")
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

    def test_20(self):
        """clone 和 clone-and-apply 输出 INFO 日志，含源/目标方案和批次"""
        import logging

        svc_log4 = PipelineService(self.db_path)
        batch_id = svc_log4.create_batch("reg_001", SAMPLE_CSV)
        svc_log4.process_batch(batch_id)

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

    def test_21(self):
        """Same source (batch_id + run_id) different title import"""
        svc = PipelineService(self.db_path)
        batch_id = svc.create_batch("ticket_test_batch", SAMPLE_CSV)
        run_id, run_n = svc.process_batch(batch_id)

        ticket_id = svc.create_ticket(
            title="Original Ticket Title",
            source_batch_id=batch_id,
            source_run_id=run_id,
            assignee="test_user"
        )

        export_path = os.path.join(self.out_dir, f"ticket_{ticket_id}.json")
        svc.export_ticket(ticket_id, export_path)

        with open(export_path, "r", encoding="utf-8") as f:
            ticket_data = json.load(f)
        ticket_data["title"] = "Modified Ticket Title"
        with open(export_path, "w", encoding="utf-8") as f:
            json.dump(ticket_data, f, ensure_ascii=False)

        import_result = svc.import_ticket(
            export_path,
            on_conflict=TicketImportResult.ACTION_RENAME,
            new_title="Renamed Ticket Title"
        )

        assert import_result.success, "Import with rename should succeed"
        assert import_result.action == TicketImportResult.ACTION_RENAME
        assert import_result.conflict_type == TicketConflictError.CONFLICT_SOURCE

        new_ticket_id = import_result.ticket_id
        assert new_ticket_id != ticket_id, "Imported ticket should have new ID"

        new_ticket = svc.get_ticket(new_ticket_id)
        assert new_ticket["title"] == "Renamed Ticket Title"
        assert new_ticket["source_batch_id"] == batch_id
        assert new_ticket["source_run_id"] == run_id
        assert new_ticket["original_ticket_id"] == ticket_id
        assert new_ticket["imported_from"] is not None

        audit_logs = svc.get_ticket_audit_logs(
            action=db.AUDIT_ACTION_TICKET_IMPORT,
            limit=10
        )
        assert len(audit_logs) >= 1, "Should have audit logs for ticket import"

        conflict_logs = [
            log for log in audit_logs
            if log.get("config_diff", {}).get("conflict_type") == TicketConflictError.CONFLICT_SOURCE
        ]
        assert len(conflict_logs) >= 1, "Should have audit log with conflict_type=source_exists"

        rename_logs = [
            log for log in conflict_logs
            if log.get("config_diff", {}).get("conflict_action") == TicketImportResult.ACTION_RENAME
        ]
        assert len(rename_logs) >= 1, "Should have audit log with conflict_action=rename"

    def test_22(self):
        """Import ticket then continue processing"""
        svc = PipelineService(self.db_path)
        batch_id = svc.create_batch("ticket_proc_batch", SAMPLE_CSV)
        run_id, run_n = svc.process_batch(batch_id)

        original_ticket_id = svc.create_ticket(
            title="Ticket to Import",
            source_batch_id=batch_id,
            source_run_id=run_id
        )

        export_path = os.path.join(self.out_dir, f"ticket_{original_ticket_id}.json")
        svc.export_ticket(original_ticket_id, export_path)

        import_result = svc.import_ticket(
            export_path,
            on_conflict=TicketImportResult.ACTION_RENAME,
            new_title="Imported Ticket"
        )
        assert import_result.success
        imported_ticket_id = import_result.ticket_id

        imported_ticket = svc.get_ticket(imported_ticket_id)
        assert imported_ticket["status"] == db.TICKET_STATUS_OPEN
        assert imported_ticket["original_ticket_id"] == original_ticket_id
        assert imported_ticket["imported_from"] is not None

        svc.assign_ticket(imported_ticket_id, "alice")
        assigned_ticket = svc.get_ticket(imported_ticket_id)
        assert assigned_ticket["status"] == db.TICKET_STATUS_ASSIGNED
        assert assigned_ticket["assignee"] == "alice"
        assert assigned_ticket["original_ticket_id"] == original_ticket_id
        assert assigned_ticket["imported_from"] is not None

        svc.resolve_ticket(imported_ticket_id, "问题已修复", assignee="bob")
        resolved_ticket = svc.get_ticket(imported_ticket_id)
        assert resolved_ticket["status"] == db.TICKET_STATUS_RESOLVED
        assert resolved_ticket["resolution"] == "问题已修复"
        assert resolved_ticket["original_ticket_id"] == original_ticket_id
        assert resolved_ticket["imported_from"] is not None

        svc.reopen_ticket(imported_ticket_id, "问题复现，需要重新处理")
        reopened_ticket = svc.get_ticket(imported_ticket_id)
        assert reopened_ticket["status"] == db.TICKET_STATUS_REOPENED
        assert reopened_ticket["original_ticket_id"] == original_ticket_id
        assert reopened_ticket["imported_from"] is not None

        audit_logs = svc.get_ticket_audit_logs(limit=20)
        actions = [log["action"] for log in audit_logs
                   if log.get("config_diff", {}).get("ticket_id") == imported_ticket_id]
        assert "ticket_assign" in actions, "Should have ticket_assign audit log"
        assert "ticket_resolve" in actions, "Should have ticket_resolve audit log"
        assert "ticket_reopen" in actions, "Should have ticket_reopen audit log"

    def test_23(self):
        """重启后追溯：导入的工单及审计日志持久化，来源冲突信息可查"""
        svc1 = PipelineService(self.db_path)
        batch_id = svc1.create_batch("ticket_restart_batch", SAMPLE_CSV)
        run_id, run_n = svc1.process_batch(batch_id)

        original_ticket_id = svc1.create_ticket(
            title="Original Ticket for Restart",
            source_batch_id=batch_id,
            source_run_id=run_id
        )

        export_path = os.path.join(self.out_dir, f"ticket_{original_ticket_id}.json")
        svc1.export_ticket(original_ticket_id, export_path)

        with open(export_path, "r", encoding="utf-8") as f:
            ticket_data = json.load(f)
        ticket_data["title"] = "Modified Title for Restart Test"
        with open(export_path, "w", encoding="utf-8") as f:
            json.dump(ticket_data, f, ensure_ascii=False)

        import_result = svc1.import_ticket(
            export_path,
            on_conflict=TicketImportResult.ACTION_RENAME,
            new_title="Imported Ticket for Restart"
        )
        assert import_result.success
        imported_ticket_id = import_result.ticket_id
        assert import_result.conflict_type == TicketConflictError.CONFLICT_SOURCE

        del svc1

        svc2 = PipelineService(self.db_path)

        imported_ticket = svc2.get_ticket(imported_ticket_id)
        assert imported_ticket is not None, "Imported ticket should exist after restart"
        assert imported_ticket["original_ticket_id"] == original_ticket_id, \
            f"original_ticket_id should be {original_ticket_id}, actual {imported_ticket.get('original_ticket_id')}"
        assert imported_ticket["imported_from"] is not None, "imported_from should persist"
        assert imported_ticket["title"] == "Imported Ticket for Restart"
        assert imported_ticket["source_batch_id"] == batch_id
        assert imported_ticket["source_run_id"] == run_id

        original_ticket = svc2.get_ticket(original_ticket_id)
        assert original_ticket is not None, "Original ticket should exist after restart"

        audit_logs = svc2.get_ticket_audit_logs(
            action=db.AUDIT_ACTION_TICKET_IMPORT,
            limit=10
        )
        assert len(audit_logs) >= 1, "Audit logs should persist after restart"

        import_logs = [
            log for log in audit_logs
            if log.get("config_diff", {}).get("ticket_id") == imported_ticket_id
        ]
        assert len(import_logs) >= 1, "Should have import audit log for the imported ticket"

        conflict_logs = [
            log for log in import_logs
            if log.get("config_diff", {}).get("conflict_type") == TicketConflictError.CONFLICT_SOURCE
            and log.get("config_diff", {}).get("conflict_action") == TicketImportResult.ACTION_RENAME
        ]
        assert len(conflict_logs) >= 1, "Should have conflict entries in audit logs after restart"

    def test_24(self):
        """Filter tickets by assignee"""
        svc = PipelineService(self.db_path)

        ticket1_id = svc.create_ticket(title="Ticket 1 for Alice", assignee="alice")
        ticket2_id = svc.create_ticket(title="Ticket for Bob", assignee="bob")
        ticket3_id = svc.create_ticket(title="Ticket 2 for Alice", assignee="alice")
        ticket4_id = svc.create_ticket(title="Unassigned Ticket", assignee=None)

        alice_tickets = svc.list_tickets(assignee="alice")
        assert len(alice_tickets) == 2, f"Should have 2 tickets for alice, actual {len(alice_tickets)}"

        alice_ticket_ids = {t["id"] for t in alice_tickets}
        assert ticket1_id in alice_ticket_ids, "Ticket 1 should be in alice's tickets"
        assert ticket3_id in alice_ticket_ids, "Ticket 3 should be in alice's tickets"
        assert ticket2_id not in alice_ticket_ids, "Bob's ticket should not be in alice's tickets"
        assert ticket4_id not in alice_ticket_ids, "Unassigned ticket should not be in alice's tickets"

        for t in alice_tickets:
            assert t["assignee"] == "alice", f"Ticket assignee should be alice, actual {t.get('assignee')}"

        bob_tickets = svc.list_tickets(assignee="bob")
        assert len(bob_tickets) == 1, f"Should have 1 ticket for bob, actual {len(bob_tickets)}"
        assert bob_tickets[0]["id"] == ticket2_id
        assert bob_tickets[0]["assignee"] == "bob"

        all_tickets = svc.list_tickets()
        assert len(all_tickets) == 4, f"Should have 4 total tickets, actual {len(all_tickets)}"

        non_existent = svc.list_tickets(assignee="charlie")
        assert len(non_existent) == 0, "Should have no tickets for non-existent assignee"

    def test_25(self):
        """CLI scheme clone/clone-apply 帮助文本含规则和日志说明"""
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

    def test_26(self):
        """clone-apply 成功路径，终端输出含源/目标方案和批次，日志格式匹配文档"""
        from click.testing import CliRunner
        from pipeline.cli import cli
        import logging

        runner = CliRunner()
        svc_doc = PipelineService(self.db_path)

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
            cli_params = ["--db", self.db_path, "scheme", "clone-apply",
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

    def test_27(self):
        """clone-apply 到锁定批次拒绝，不创建新方案，终端和日志符合文档"""
        from click.testing import CliRunner
        from pipeline.cli import cli
        import logging

        runner = CliRunner()
        svc_rej = PipelineService(self.db_path)

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
            res = runner.invoke(cli, ["--db", self.db_path, "scheme", "clone-apply",
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

    def test_28(self):
        """derive 成功派生，source_scheme_id 记录正确，同名冲突抛 name_exists"""
        svc_d1 = PipelineService(self.db_path)

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

    def test_29(self):
        """derive-apply 成功派生应用、同名冲突、锁定批次拒绝、步骤级日志可见"""
        import logging

        svc_d2 = PipelineService(self.db_path)

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

    def test_30(self):
        """重启后派生方案来源可查，应用后配置版本保留"""
        svc_pre2 = PipelineService(self.db_path)
        batch_id = svc_pre2.create_batch("reg_001", SAMPLE_CSV)
        svc_pre2.process_batch(batch_id)

        restart_src = svc_pre2.save_scheme("restart_derive_src", batch_id=batch_id, description="重启派生源")
        restart_bid = svc_pre2.create_batch("restart_derive_batch", SAMPLE_CSV)
        svc_pre2.process_batch(restart_bid)
        restart_result = svc_pre2.derive_and_apply_scheme(
            restart_src, "restart_derive_target", restart_bid, "重启前派生应用")
        expected_v = restart_result.new_config_version
        expected_derived_id = restart_result.derived_scheme_id

        del svc_pre2

        svc_post2 = PipelineService(self.db_path)

        derived_after = svc_post2.get_scheme(expected_derived_id)
        assert derived_after is not None
        assert derived_after["source_scheme_id"] == restart_src, \
            f"重启后 source_scheme_id 应为 {restart_src}，实际 {derived_after.get('source_scheme_id')}"
        assert derived_after["name"] == "restart_derive_target"

        batch_after = svc_post2.get_batch(restart_bid)
        assert batch_after["config_version"] == expected_v, \
            f"重启后配置版本应为 v{expected_v}，实际 v{batch_after['config_version']}"

    def test_31(self):
        """派生方案导出再导入后来源可追溯，链路可用"""
        svc_d3 = PipelineService(self.db_path)
        batch_id = svc_d3.create_batch("reg_001", SAMPLE_CSV)
        svc_d3.process_batch(batch_id)

        ei_src = svc_d3.save_scheme("ei_source", batch_id=batch_id, description="导出导入源")
        ei_derived = svc_d3.derive_scheme(ei_src, "ei_derived", "导出导入派生")

        ei_export_dir = os.path.join(self.tmpdir, "ei_exports")
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

    def test_32(self):
        """CLI derive/derive-apply/scheme show 三边对齐"""
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

        svc_cli = PipelineService(self.db_path)
        cli_bid = svc_cli.create_batch("cli_derive_batch", SAMPLE_CSV)
        svc_cli.process_batch(cli_bid)
        cli_sid = svc_cli.save_scheme("cli_source", batch_id=cli_bid, description="CLI源")
        cli_target = svc_cli.create_batch("cli_derive_target", SAMPLE_CSV)
        svc_cli.process_batch(cli_target)

        res_derive = runner.invoke(cli, ["--db", self.db_path, "scheme", "derive",
                                          str(cli_sid), "cli_derived", "--description", "CLI派生"])
        assert res_derive.exit_code == 0
        assert "[OK]" in res_derive.output
        assert "派生方案" in res_derive.output
        assert "cli_derived" in res_derive.output
        assert "来源追溯" in res_derive.output

        res_da = runner.invoke(cli, ["--db", self.db_path, "scheme", "derive-apply",
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

        res_show = runner.invoke(cli, ["--db", self.db_path, "scheme", "show", str(cli_sid)])
        assert "派生来源" in res_show.output
        assert "原始方案" in res_show.output

    def test_33(self):
        """方案应用后批次 current_scheme 正确，历史记录生成，含来源方案"""
        svc_h1 = PipelineService(self.db_path)

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

    def test_34(self):
        """set-threshold 直接修改配置也记录历史，标记为 direct 操作"""
        svc_h2 = PipelineService(self.db_path)

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

    def test_35(self):
        """方案应用后回滚成功，配置版本递增，方案信息回退"""
        svc_rb1 = PipelineService(self.db_path)

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

    def test_36(self):
        """回滚到上一版本，来源方案信息正确"""
        svc_rb2 = PipelineService(self.db_path)

        rbt_bid = svc_rb2.create_batch("rb2_batch", SAMPLE_CSV)
        svc_rb2.process_batch(rbt_bid)
        rb2_src = svc_rb2.save_scheme("rb2_src", batch_id=rbt_bid, description="回滚测试源")
        rb2_derived = svc_rb2.derive_scheme(rb2_src, "rb2_derived", "回滚测试派生")

        svc_rb2.apply_scheme_to_batch(rb2_derived, rbt_bid)
        svc_rb2.set_threshold(rbt_bid, zscore_threshold=3.0)

        hist_before = svc_rb2.get_scheme_history(rbt_bid)
        before_count = len(hist_before)
        batch_before = svc_rb2.get_batch(rbt_bid)
        version_before = batch_before["config_version"]

        result = svc_rb2.rollback_scheme(rbt_bid)
        assert isinstance(result, SchemeRollbackResult)
        assert result.success is True
        assert result.previous_config_version == version_before
        assert result.new_config_version > version_before
        assert result.previous_scheme_id == rb2_derived
        assert result.previous_scheme_name == "rb2_derived"

        batch_after = svc_rb2.get_batch(rbt_bid)
        assert batch_after["current_scheme_id"] == rb2_derived
        assert batch_after["current_scheme_name"] == "rb2_derived"
        assert batch_after["config_version"] == result.new_config_version

        hist_after = svc_rb2.get_scheme_history(rbt_bid)
        assert len(hist_after) == before_count + 1
        assert hist_after[0]["action"] == "rollback"

    def test_37(self):
        """重启后回滚操作历史保留，当前方案正确"""
        svc_pre3 = PipelineService(self.db_path)
        pre3_bid = svc_pre3.create_batch("pre3_batch", SAMPLE_CSV)
        svc_pre3.process_batch(pre3_bid)
        pre3_src = svc_pre3.save_scheme("pre3_src", batch_id=pre3_bid, description="重启回滚源")

        svc_pre3.apply_scheme_to_batch(pre3_src, pre3_bid)
        svc_pre3.set_threshold(pre3_bid, zscore_threshold=2.0)
        rb_result = svc_pre3.rollback_scheme(pre3_bid)
        expected_new_v = rb_result.new_config_version
        expected_count = len(svc_pre3.get_scheme_history(pre3_bid))

        del svc_pre3

        svc_post3 = PipelineService(self.db_path)

        hist = svc_post3.get_scheme_history(pre3_bid)
        assert len(hist) == expected_count, f"重启后历史记录数应为 {expected_count}，实际 {len(hist)}"
        assert hist[0]["action"] == "rollback"

        batch = svc_post3.get_batch(pre3_bid)
        assert batch["config_version"] == expected_new_v
        assert batch["current_scheme_id"] == pre3_src
        assert batch["current_scheme_name"] == "pre3_src"

    def test_38(self):
        """dry-run 应用成功，未修改批次状态，含来源方案"""
        svc_dry1 = PipelineService(self.db_path)

        dry_base_bid = svc_dry1.create_batch("dry1_base", SAMPLE_CSV)
        svc_dry1.process_batch(dry_base_bid)
        dry_src = svc_dry1.save_scheme("dry1_src", batch_id=dry_base_bid, description="dry-run源")
        dry_derived = svc_dry1.derive_scheme(dry_src, "dry1_derived", "dry-run派生")

        dry_target_bid = svc_dry1.create_batch("dry1_target", SAMPLE_CSV)
        svc_dry1.process_batch(dry_target_bid)

        batch_before = svc_dry1.get_batch(dry_target_bid)
        config_before = json.loads(batch_before["config_json"])
        scheme_id_before = batch_before["current_scheme_id"]
        scheme_name_before = batch_before["current_scheme_name"]
        version_before = batch_before["config_version"]
        hist_before = len(svc_dry1.get_scheme_history(dry_target_bid))

        result = svc_dry1.dry_run_apply_scheme(dry_derived, dry_target_bid)
        assert isinstance(result, DryRunResult)
        assert result.can_proceed is True
        assert result.scheme_id == dry_derived
        assert result.scheme_name == "dry1_derived"
        assert result.source_scheme_id == dry_src
        assert result.source_scheme_name == "dry1_src"
        assert result.batch_id == dry_target_bid
        assert len(result.risks) >= 0

        batch_after = svc_dry1.get_batch(dry_target_bid)
        assert json.dumps(json.loads(batch_after["config_json"]), sort_keys=True) == \
               json.dumps(config_before, sort_keys=True), \
            "dry-run 不应修改批次配置"
        assert batch_after["current_scheme_id"] == scheme_id_before
        assert batch_after["current_scheme_name"] == scheme_name_before
        assert batch_after["config_version"] == version_before
        assert len(svc_dry1.get_scheme_history(dry_target_bid)) == hist_before, \
            "dry-run 不应新增历史记录"

    def test_39(self):
        """dry-run 风险类型覆盖：批次锁定、批次不存在、方案不存在"""
        svc_dry2 = PipelineService(self.db_path)

        r2_base_bid = svc_dry2.create_batch("risk_base", SAMPLE_CSV)
        svc_dry2.process_batch(r2_base_bid)
        r2_src = svc_dry2.save_scheme("risk_src", batch_id=r2_base_bid, description="风险测试源")

        r2_target_bid = svc_dry2.create_batch("risk_target", SAMPLE_CSV)
        svc_dry2.process_batch(r2_target_bid)

        r2_derived = svc_dry2.derive_scheme(r2_src, "risk_derived", "风险测试派生")

        svc_dry2.lock_batch(r2_target_bid)
        result2 = svc_dry2.dry_run_apply_scheme(r2_derived, r2_target_bid)
        assert result2.can_proceed is False
        assert any(r.risk_type == DryRunRisk.RISK_LOCKED for r in result2.risks), \
            "应检测到批次锁定风险"

        svc_dry2.unlock_batch(r2_target_bid)

        result3 = svc_dry2.dry_run_apply_scheme(r2_derived, 99999)
        assert result3.can_proceed is False
        assert any(r.risk_type == DryRunRisk.RISK_BATCH_NOT_FOUND for r in result3.risks), \
            "应检测到批次不存在风险"

        result4 = svc_dry2.dry_run_apply_scheme(99999, r2_target_bid)
        assert result4.can_proceed is False
        assert any(r.risk_type == DryRunRisk.RISK_SCHEME_NOT_FOUND for r in result4.risks), \
            "应检测到方案不存在风险"

    def test_40(self):
        """dry-run 到不存在的批次返回 can_proceed=False，含批次不存在风险"""
        svc_dry3 = PipelineService(self.db_path)

        d3_bid = svc_dry3.create_batch("dry3_base", SAMPLE_CSV)
        svc_dry3.process_batch(d3_bid)
        d3_sid = svc_dry3.save_scheme("dry3_scheme", batch_id=d3_bid)

        result = svc_dry3.dry_run_apply_scheme(d3_sid, 99999)
        assert result.can_proceed is False
        assert any(r.risk_type == DryRunRisk.RISK_BATCH_NOT_FOUND for r in result.risks)

    def test_41(self):
        """switch-scheme 切换成功，配置版本递增"""
        svc_sw1 = PipelineService(self.db_path)

        sw_base_bid = svc_sw1.create_batch("sw_base", SAMPLE_CSV)
        svc_sw1.process_batch(sw_base_bid)
        sw_src = svc_sw1.save_scheme("sw_src", batch_id=sw_base_bid, description="切换测试源")
        sw_derived = svc_sw1.derive_scheme(sw_src, "sw_derived", "切换测试派生")
        sw_other = svc_sw1.save_scheme("sw_other", batch_id=sw_base_bid, description="切换测试其他")

        sw_target_bid = svc_sw1.create_batch("sw_target", SAMPLE_CSV)
        svc_sw1.process_batch(sw_target_bid)
        svc_sw1.apply_scheme_to_batch(sw_derived, sw_target_bid)

        batch_before = svc_sw1.get_batch(sw_target_bid)
        version_before = batch_before["config_version"]
        scheme_before = batch_before["current_scheme_id"]
        hist_before = len(svc_sw1.get_scheme_history(sw_target_bid))

        result = svc_sw1.switch_scheme(
            SwitchSchemeResult.SWITCH_TYPE_APPLY,
            sw_target_bid,
            scheme_id=sw_other
        )
        assert isinstance(result, SwitchSchemeResult)
        assert result.success is True
        assert result.switch_type == SwitchSchemeResult.SWITCH_TYPE_APPLY
        assert result.new_scheme_id == sw_other
        assert result.new_scheme_name == "sw_other"
        assert result.new_config["version"] > version_before

        batch_after = svc_sw1.get_batch(sw_target_bid)
        assert batch_after["current_scheme_id"] == sw_other
        assert batch_after["current_scheme_name"] == "sw_other"
        assert batch_after["config_version"] == result.new_config["version"]

        hist_after = svc_sw1.get_scheme_history(sw_target_bid)
        assert len(hist_after) == hist_before + 1

    def test_42(self):
        """switch-scheme 到锁定批次、不存在方案的错误处理"""
        svc_sw2 = PipelineService(self.db_path)

        sw2_base_bid = svc_sw2.create_batch("sw2_base", SAMPLE_CSV)
        svc_sw2.process_batch(sw2_base_bid)
        sw2_sid1 = svc_sw2.save_scheme("sw2_s1", batch_id=sw2_base_bid)
        sw2_sid2 = svc_sw2.save_scheme("sw2_s2", batch_id=sw2_base_bid)

        sw2_target = svc_sw2.create_batch("sw2_target", SAMPLE_CSV)
        svc_sw2.process_batch(sw2_target)
        svc_sw2.apply_scheme_to_batch(sw2_sid1, sw2_target)

        result_nonexistent = svc_sw2.switch_scheme(
            SwitchSchemeResult.SWITCH_TYPE_APPLY,
            sw2_target,
            scheme_id=99999
        )
        assert result_nonexistent.success is False
        assert result_nonexistent.dry_run is not None
        assert any(r.risk_type == DryRunRisk.RISK_SCHEME_NOT_FOUND for r in result_nonexistent.dry_run.risks)

        svc_sw2.lock_batch(sw2_target)
        result_locked = svc_sw2.switch_scheme(
            SwitchSchemeResult.SWITCH_TYPE_APPLY,
            sw2_target,
            scheme_id=sw2_sid2
        )
        assert result_locked.success is False
        assert any(r.risk_type == DryRunRisk.RISK_LOCKED for r in result_locked.dry_run.risks)

        svc_sw2.unlock_batch(sw2_target)

    def test_43(self):
        """list-tickets 支持按 source_batch_id 筛选"""
        svc_tf = PipelineService(self.db_path)

        bid_t1 = svc_tf.create_batch("tf_batch1", SAMPLE_CSV)
        run_id1, _ = svc_tf.process_batch(bid_t1)
        bid_t2 = svc_tf.create_batch("tf_batch2", SAMPLE_CSV)
        run_id2, _ = svc_tf.process_batch(bid_t2)
        bid_t3 = svc_tf.create_batch("tf_batch3", SAMPLE_CSV)
        svc_tf.process_batch(bid_t3)

        run1 = svc_tf.get_latest_run(bid_t1)
        svc_tf.process_batch(bid_t1)
        run1b = svc_tf.get_latest_run(bid_t1)
        run2 = svc_tf.get_latest_run(bid_t2)

        t1 = svc_tf.create_ticket("T1", source_batch_id=bid_t1, source_run_id=run1["id"])
        t2 = svc_tf.create_ticket("T2", source_batch_id=bid_t1, source_run_id=run1b["id"])
        t3 = svc_tf.create_ticket("T3", source_batch_id=bid_t2, source_run_id=run2["id"])
        t4 = svc_tf.create_ticket("T4")

        filtered_b1 = svc_tf.list_tickets(source_batch_id=bid_t1)
        assert len(filtered_b1) == 2, f"按 batch {bid_t1} 筛选应返回 2 个，实际 {len(filtered_b1)}"
        ids_b1 = {t["id"] for t in filtered_b1}
        assert ids_b1 == {t1, t2}

        filtered_b2 = svc_tf.list_tickets(source_batch_id=bid_t2)
        assert len(filtered_b2) == 1
        assert filtered_b2[0]["id"] == t3

        filtered_b3 = svc_tf.list_tickets(source_batch_id=bid_t3)
        assert len(filtered_b3) == 0

        filtered_null = svc_tf.list_tickets(source_batch_id=None)
        assert len(filtered_null) == 4, f"不筛选 source_batch_id 应返回全部 4 个，实际 {len(filtered_null)}"

    def test_44(self):
        """get_ticket_audit_logs 支持按 action 和 result 筛选"""
        svc_al = PipelineService(self.db_path)

        al_bid = svc_al.create_batch("al_batch", SAMPLE_CSV)
        svc_al.process_batch(al_bid)

        t1 = svc_al.create_ticket("AL_T1")
        svc_al.assign_ticket(t1, "alice")
        svc_al.resolve_ticket(t1, "已修复")
        svc_al.reopen_ticket(t1, "问题复现")

        t2 = svc_al.create_ticket("AL_T2")
        svc_al.assign_ticket(t2, "bob")

        all_logs = svc_al.get_ticket_audit_logs()
        assert len(all_logs) >= 6, f"至少应有 6 条审计日志，实际 {len(all_logs)}"

        assign_logs = svc_al.get_ticket_audit_logs(action="ticket_assign")
        assert len(assign_logs) == 2, f"ticket_assign 操作应有 2 条，实际 {len(assign_logs)}"
        assert all(l["action"] == "ticket_assign" for l in assign_logs)

        resolve_logs = svc_al.get_ticket_audit_logs(action="ticket_resolve")
        assert len(resolve_logs) == 1
        assert resolve_logs[0]["config_diff"]["ticket_id"] == t1

        success_logs = svc_al.get_ticket_audit_logs(result="success")
        assert len(success_logs) >= 6
        assert all(l["result"] == "success" for l in success_logs)

        assign_success = svc_al.get_ticket_audit_logs(action="ticket_assign", result="success")
        assert len(assign_success) == 2

        limit_logs = svc_al.get_ticket_audit_logs(limit=3)
        assert len(limit_logs) == 3

    def test_45(self):
        """重启后 ticket 和 audit_log 数据保留"""
        svc_pre4 = PipelineService(self.db_path)

        pre4_bid = svc_pre4.create_batch("pre4_batch", SAMPLE_CSV)
        svc_pre4.process_batch(pre4_bid)
        pre4_run = svc_pre4.get_latest_run(pre4_bid)

        t_pre = svc_pre4.create_ticket("PRE_T1", source_batch_id=pre4_bid,
                                         source_run_id=pre4_run["id"])
        svc_pre4.assign_ticket(t_pre, "eve")
        svc_pre4.resolve_ticket(t_pre, "重启前解决")

        expected_status = svc_pre4.get_ticket(t_pre)["status"]

        del svc_pre4

        svc_post4 = PipelineService(self.db_path)

        ticket = svc_post4.get_ticket(t_pre)
        assert ticket is not None
        assert ticket["title"] == "PRE_T1"
        assert ticket["status"] == expected_status
        assert ticket["source_batch_id"] == pre4_bid
        assert ticket["source_run_id"] == pre4_run["id"]
        assert ticket["assignee"] == "eve"

        logs = svc_post4.get_ticket_audit_logs()
        assert len(logs) >= 3
        assert any(l.get("config_diff", {}).get("ticket_id") == t_pre
                   and l["action"] == "ticket_resolve" for l in logs)

    def test_46(self):
        """import_ticket on_conflict=reject 成功拒绝，不创建重复"""
        svc_rej = PipelineService(self.db_path)

        rej_bid = svc_rej.create_batch("rej_batch", SAMPLE_CSV)
        svc_rej.process_batch(rej_bid)
        rej_run = svc_rej.get_latest_run(rej_bid)

        t_rej = svc_rej.create_ticket("RejectTest", source_batch_id=rej_bid,
                                        source_run_id=rej_run["id"])

        rej_path = os.path.join(self.tmpdir, "rej_ticket.json")
        svc_rej.export_ticket(t_rej, rej_path)

        with open(rej_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["title"] = "RejectTest Modified"
        with open(rej_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        tickets_before = svc_rej.list_tickets()
        assert len(tickets_before) == 1

        result = svc_rej.import_ticket(rej_path, on_conflict=TicketImportResult.ACTION_REJECT)
        assert isinstance(result, TicketImportResult)
        assert result.success is False
        assert result.action == TicketImportResult.ACTION_REJECT
        assert result.conflict_type == TicketConflictError.CONFLICT_SOURCE
        assert result.original_ticket_id == t_rej

        tickets_after = svc_rej.list_tickets()
        assert len(tickets_after) == 1, "reject 不应创建新工单"

    def test_47(self):
        """import_ticket 同源冲突正确返回失败结果，错误类型一致"""
        svc_err = PipelineService(self.db_path)

        err_bid = svc_err.create_batch("err_batch", SAMPLE_CSV)
        svc_err.process_batch(err_bid)
        err_run = svc_err.get_latest_run(err_bid)

        t_err = svc_err.create_ticket("ErrorTest", source_batch_id=err_bid,
                                       source_run_id=err_run["id"])

        err_path = os.path.join(self.tmpdir, "err_ticket.json")
        svc_err.export_ticket(t_err, err_path)

        with open(err_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["title"] = "Modified Title"
        with open(err_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        result = svc_err.import_ticket(err_path, on_conflict=TicketImportResult.ACTION_REJECT)
        assert isinstance(result, TicketImportResult)
        assert result.success is False
        assert result.action == TicketImportResult.ACTION_REJECT
        assert result.conflict_type == TicketConflictError.CONFLICT_SOURCE
        assert result.original_ticket_id == t_err

    def test_48(self):
        """import_ticket 完整导入所有字段，包括可选字段"""
        svc_full = PipelineService(self.db_path)

        full_bid = svc_full.create_batch("full_batch", SAMPLE_CSV)
        svc_full.process_batch(full_bid)
        full_run = svc_full.get_latest_run(full_bid)

        t_full = svc_full.create_ticket(
            "FullImport",
            source_batch_id=full_bid,
            source_run_id=full_run["id"],
            trigger_rule="zscore_high_temp"
        )
        svc_full.assign_ticket(t_full, "grace")
        svc_full.resolve_ticket(t_full, "已完整解决")

        full_path = os.path.join(self.tmpdir, "full_ticket.json")
        svc_full.export_ticket(t_full, full_path)

        with open(full_path, "r", encoding="utf-8") as f:
            exported = json.load(f)
        assert exported["title"] == "FullImport"
        assert exported["source_batch_id"] == full_bid
        assert exported["source_run_id"] == full_run["id"]
        assert exported["trigger_rule"] == "zscore_high_temp"
        assert exported["status"] == "resolved"
        assert exported["assignee"] == "grace"
        assert exported["resolution"] == "已完整解决"

        new_svc = PipelineService(self.db_path)
        result = new_svc.import_ticket(full_path, on_conflict=TicketImportResult.ACTION_RENAME,
                                        new_title="FullImport_New")
        assert result.success

        imported = new_svc.get_ticket(result.ticket_id)
        assert imported["title"] == "FullImport_New"
        assert imported["source_batch_id"] == full_bid
        assert imported["source_run_id"] == full_run["id"]
        assert imported["trigger_rule"] == "zscore_high_temp"
        assert imported["assignee"] == "grace"
        assert imported["status"] == "resolved"
        assert imported["imported_from"] is not None
        assert imported["original_ticket_id"] == t_full

    def test_49(self):
        """get_ticket 支持查询不存在的工单返回 None"""
        svc_get = PipelineService(self.db_path)

        t_none = svc_get.get_ticket(99999)
        assert t_none is None, "不存在的工单应返回 None"

        g_bid = svc_get.create_batch("g_batch", SAMPLE_CSV)
        svc_get.process_batch(g_bid)
        t_exist = svc_get.create_ticket("ExistTest")
        t_found = svc_get.get_ticket(t_exist)
        assert t_found is not None
        assert t_found["id"] == t_exist
        assert t_found["title"] == "ExistTest"

    def test_50(self):
        """assign/resolve/reopen 验证状态转换链和审计日志"""
        svc_st = PipelineService(self.db_path)

        t_st = svc_st.create_ticket("StatusTest")

        from pipeline.database import (
            TICKET_STATUS_OPEN, TICKET_STATUS_ASSIGNED,
            TICKET_STATUS_RESOLVED, TICKET_STATUS_REOPENED
        )

        t1 = svc_st.get_ticket(t_st)
        assert t1["status"] == TICKET_STATUS_OPEN

        svc_st.assign_ticket(t_st, "henry")
        t2 = svc_st.get_ticket(t_st)
        assert t2["status"] == TICKET_STATUS_ASSIGNED
        assert t2["assignee"] == "henry"

        svc_st.resolve_ticket(t_st, "状态测试解决")
        t3 = svc_st.get_ticket(t_st)
        assert t3["status"] == TICKET_STATUS_RESOLVED
        assert t3["assignee"] == "henry"

        svc_st.reopen_ticket(t_st, "状态测试重开")
        t4 = svc_st.get_ticket(t_st)
        assert t4["status"] == TICKET_STATUS_REOPENED

        logs = svc_st.get_ticket_audit_logs()
        ticket_logs = [l for l in logs if l.get("config_diff", {}).get("ticket_id") == t_st]
        actions = [l["action"] for l in ticket_logs]
        assert "ticket_assign" in actions
        assert "ticket_resolve" in actions
        assert "ticket_reopen" in actions
        assert all(l["result"] == "success" for l in ticket_logs)

    def test_51(self):
        """非法状态转换抛出 TicketError"""
        svc_ill = PipelineService(self.db_path)

        t_ill = svc_ill.create_ticket("IllegalTest")
        svc_ill.assign_ticket(t_ill, "ivy")
        svc_ill.resolve_ticket(t_ill, "已解决")

        try:
            svc_ill.assign_ticket(t_ill, "jack")
            assert False, "resolved 状态直接 assign 应抛 TicketError"
        except TicketError:
            pass

        try:
            svc_ill.resolve_ticket(t_ill, "重复解决")
            assert False, "resolved 状态重复 resolve 应抛 TicketError"
        except TicketError:
            pass

        t_open = svc_ill.create_ticket("OpenTest")
        try:
            svc_ill.reopen_ticket(t_open, "未解决直接重开")
            assert False, "open 状态直接 reopen 应抛 TicketError"
        except TicketError:
            pass

    def test_52(self):
        """list_tickets 组合筛选和多条件筛选"""
        svc_mf = PipelineService(self.db_path)

        mf_bid = svc_mf.create_batch("mf_batch", SAMPLE_CSV)
        run_id1, _ = svc_mf.process_batch(mf_bid)
        mf_run1 = svc_mf.get_latest_run(mf_bid)
        svc_mf.process_batch(mf_bid)
        mf_run2 = svc_mf.get_latest_run(mf_bid)

        t1 = svc_mf.create_ticket("MF1")
        t2 = svc_mf.create_ticket("MF2")
        t3 = svc_mf.create_ticket("MF3",
                                  source_batch_id=mf_bid, source_run_id=mf_run1["id"])
        t4 = svc_mf.create_ticket("MF4", source_batch_id=mf_bid,
                                  source_run_id=mf_run2["id"])

        svc_mf.assign_ticket(t1, "kate")
        svc_mf.assign_ticket(t2, "kate")
        svc_mf.assign_ticket(t3, "leo")
        svc_mf.resolve_ticket(t1, "已完成")

        all_open = svc_mf.list_tickets(status="open")
        assert len(all_open) == 1, f"open 状态应有 1 个，实际 {len(all_open)}"
        assert all_open[0]["id"] == t4
        assert all(t["status"] == "open" for t in all_open)

        all_assigned = svc_mf.list_tickets(status="assigned")
        assert len(all_assigned) == 2, f"assigned 状态应有 2 个，实际 {len(all_assigned)}"

        all_resolved = svc_mf.list_tickets(status="resolved")
        assert len(all_resolved) == 1
        assert all_resolved[0]["id"] == t1

        kate_assigned = svc_mf.list_tickets(status="assigned", assignee="kate")
        assert len(kate_assigned) == 1
        assert kate_assigned[0]["id"] == t2

        mf_batch_assigned = svc_mf.list_tickets(status="assigned", source_batch_id=mf_bid)
        assert len(mf_batch_assigned) == 1
        assert mf_batch_assigned[0]["id"] == t3

        combo = svc_mf.list_tickets(status="assigned", assignee="leo", source_batch_id=mf_bid)
        assert len(combo) == 1
        assert combo[0]["id"] == t3

        combo_empty = svc_mf.list_tickets(status="resolved", assignee="leo")
        assert len(combo_empty) == 0

    def test_53(self):
        """import_ticket on_conflict=rename 成功创建新工单，old_title字段正确"""
        svc_ren = PipelineService(self.db_path)

        ren_bid = svc_ren.create_batch("ren_batch", SAMPLE_CSV)
        svc_ren.process_batch(ren_bid)
        ren_run = svc_ren.get_latest_run(ren_bid)

        t_ren = svc_ren.create_ticket("RenameOrig",
                                       source_batch_id=ren_bid,
                                       source_run_id=ren_run["id"],
                                       assignee="mike")
        svc_ren.assign_ticket(t_ren, "nancy")

        ren_path = os.path.join(self.tmpdir, "ren_ticket.json")
        svc_ren.export_ticket(t_ren, ren_path)

        with open(ren_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["title"] = "RenameNew"
        with open(ren_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        tickets_before = svc_ren.list_tickets()
        assert len(tickets_before) == 1

        result = svc_ren.import_ticket(ren_path, on_conflict="rename", new_title="RenameImported")
        assert isinstance(result, TicketImportResult)
        assert result.success is True
        assert result.action == TicketImportResult.ACTION_RENAME
        assert result.conflict_type == TicketConflictError.CONFLICT_SOURCE
        assert result.original_ticket_id == t_ren
        assert result.ticket_id != t_ren

        tickets_after = svc_ren.list_tickets()
        assert len(tickets_after) == 2, "rename 应创建新工单"

        new_ticket = svc_ren.get_ticket(result.ticket_id)
        assert new_ticket["title"] == "RenameImported"
        assert new_ticket["assignee"] == "nancy"
        assert new_ticket["imported_from"] is not None
        assert new_ticket["original_ticket_id"] == t_ren

    def test_54(self):
        """导出导入 cycle 测试：多次导出导入保持一致性"""
        svc_cyc = PipelineService(self.db_path)

        cyc_bid = svc_cyc.create_batch("cyc_batch", SAMPLE_CSV)
        svc_cyc.process_batch(cyc_bid)
        cyc_run = svc_cyc.get_latest_run(cyc_bid)

        t_orig = svc_cyc.create_ticket("CycleTest",
                                        source_batch_id=cyc_bid,
                                        source_run_id=cyc_run["id"],
                                        trigger_rule="cycle_rule")

        path1 = os.path.join(self.tmpdir, "cycle1.json")
        svc_cyc.export_ticket(t_orig, path1)

        r1 = svc_cyc.import_ticket(path1, on_conflict="rename", new_title="CycleTest_v1")
        assert r1.success
        t1 = r1.ticket_id
        assert t1 != t_orig

        path2 = os.path.join(self.tmpdir, "cycle2.json")
        svc_cyc.export_ticket(t1, path2)

        r2 = svc_cyc.import_ticket(path2, on_conflict="rename", new_title="CycleTest_v2")
        assert r2.success
        t2 = r2.ticket_id
        assert t2 != t1 and t2 != t_orig

        ticket_orig = svc_cyc.get_ticket(t_orig)
        ticket1 = svc_cyc.get_ticket(t1)
        ticket2 = svc_cyc.get_ticket(t2)

        assert ticket_orig["source_batch_id"] == ticket1["source_batch_id"] == ticket2["source_batch_id"]
        assert ticket_orig["source_run_id"] == ticket1["source_run_id"] == ticket2["source_run_id"]
        assert ticket1["original_ticket_id"] == t_orig
        assert ticket2["original_ticket_id"] == t_orig
        assert ticket1["imported_from"] is not None
        assert ticket2["imported_from"] is not None

    def test_55(self):
        """工单相关 CLI 命令帮助文本齐全"""
        from click.testing import CliRunner
        from pipeline.cli import cli

        runner = CliRunner()

        res_ticket_help = runner.invoke(cli, ["ticket", "--help"])
        assert res_ticket_help.exit_code == 0
        assert "create" in res_ticket_help.output
        assert "export" in res_ticket_help.output
        assert "import" in res_ticket_help.output
        assert "assign" in res_ticket_help.output
        assert "resolve" in res_ticket_help.output
        assert "reopen" in res_ticket_help.output
        assert "list" in res_ticket_help.output
        assert "show" in res_ticket_help.output

        res_import_help = runner.invoke(cli, ["ticket", "import", "--help"])
        assert res_import_help.exit_code == 0
        assert "on-conflict" in res_import_help.output
        assert "reject" in res_import_help.output
        assert "rename" in res_import_help.output
        assert "new-title" in res_import_help.output

        res_list_help = runner.invoke(cli, ["ticket", "list", "--help"])
        assert res_list_help.exit_code == 0
        assert "status" in res_list_help.output
        assert "assignee" in res_list_help.output
        assert "batch-id" in res_list_help.output or "source-batch-id" in res_list_help.output

    def test_56(self):
        """综合场景：创建-处理-存方案-派生应用-回滚-重启验证完整链路"""
        svc_e2e = PipelineService(self.db_path)

        e2e_bid1 = svc_e2e.create_batch("e2e_batch1", SAMPLE_CSV)
        svc_e2e.process_batch(e2e_bid1)
        run1 = svc_e2e.get_latest_run(e2e_bid1)
        metrics1 = svc_e2e.get_run_metrics(run1["id"])

        e2e_sid = svc_e2e.save_scheme("e2e_scheme", batch_id=e2e_bid1, description="端到端方案")
        e2e_derived = svc_e2e.derive_scheme(e2e_sid, "e2e_derived", "端到端派生")

        e2e_bid2 = svc_e2e.create_batch("e2e_batch2", SAMPLE_CSV)
        svc_e2e.process_batch(e2e_bid2)
        svc_e2e.apply_scheme_to_batch(e2e_derived, e2e_bid2)
        svc_e2e.lock_batch(e2e_bid2)

        t_e2e = svc_e2e.create_ticket("E2E Ticket",
                                       source_batch_id=e2e_bid2,
                                       source_run_id=svc_e2e.get_latest_run(e2e_bid2)["id"])
        svc_e2e.assign_ticket(t_e2e, "robert")
        svc_e2e.resolve_ticket(t_e2e, "端到端解决")

        export_path = os.path.join(self.tmpdir, "e2e_export.json")
        svc_e2e.export_ticket(t_e2e, export_path)

        import_result = svc_e2e.import_ticket(export_path, on_conflict="rename",
                                               new_title="E2E Ticket Imported")
        assert import_result.success
        assert import_result.action == "rename"

        svc_e2e.unlock_batch(e2e_bid2)
        svc_e2e.rollback_scheme(e2e_bid2)

        expected_version = svc_e2e.get_batch(e2e_bid2)["config_version"]
        expected_history_count = len(svc_e2e.get_scheme_history(e2e_bid2))

        del svc_e2e

        svc_recover = PipelineService(self.db_path)

        batch = svc_recover.get_batch(e2e_bid2)
        assert batch["config_version"] == expected_version
        assert batch["current_scheme_id"] == e2e_derived

        history = svc_recover.get_scheme_history(e2e_bid2)
        assert len(history) == expected_history_count
        assert history[0]["action"] == "rollback"

        scheme = svc_recover.get_scheme(e2e_derived)
        assert scheme["source_scheme_id"] == e2e_sid

        ticket = svc_recover.get_ticket(import_result.ticket_id)
        assert ticket["title"] == "E2E Ticket Imported"
        assert ticket["original_ticket_id"] == t_e2e
        assert ticket["status"] == "resolved"

        logs = svc_recover.get_ticket_audit_logs()
        assert any(l.get("config_diff", {}).get("ticket_id") == t_e2e
                   and l["action"] == "ticket_resolve" for l in logs)
        assert any(l.get("config_diff", {}).get("ticket_id") == import_result.ticket_id
                   and l.get("config_diff", {}).get("conflict_type") == "title_exists"
                   for l in logs)


if __name__ == "__main__":
    print("=" * 70)
    print("Pipeline Regression Test Suite (Class-based)")
    print("=" * 70)
    test_count = sum(1 for m in dir(PipelineRegressionTest) if m.startswith('test_'))
    print(f"Total test methods: {test_count}")
    print()

    test_runner = PipelineRegressionTest()
    exit_code = test_runner.run_all()

    exit(exit_code)
