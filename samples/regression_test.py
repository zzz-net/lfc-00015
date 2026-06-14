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

from pipeline.service import PipelineService, BatchLockedError, BatchServiceError
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
