"""
验收测试脚本 - 验证实验数据处理流水线的完整验收链路

覆盖的验收要求:
1. 用样例数据完成一次处理并导出指标
2. 时间戳缺失或传感器值不是数字的行要给出行级错误且保留旧结果
3. 未锁定批次重跑要生成新的运行记录，锁定批次不能被覆盖
4. 重启后，批次状态、配置版本、锁定标记和导出指标要一致
"""
import os
import sys
import json
import shutil
import tempfile
import sqlite3

# 将项目根目录加入 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.service import PipelineService, BatchLockedError, BatchServiceError
from pipeline import database as db
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
    tmpdir = tempfile.mkdtemp(prefix="pipeline_test_")
    db_path = os.path.join(tmpdir, "test.db")

    try:
        # ====== 测试 1: 用样例数据完成一次处理并导出指标 ======
        r1 = TestResult("测试1: 创建批次并完成首次处理、导出指标")
        results.append(r1)
        try:
            svc = PipelineService(db_path)
            batch_id = svc.create_batch("exp_001", SAMPLE_CSV)
            assert batch_id > 0

            run_id, run_number = svc.process_batch(batch_id)
            assert run_id > 0
            assert run_number == 1

            batch = svc.get_batch(batch_id)
            assert batch["status"] == "processed", f"状态应为 processed, 实际 {batch['status']}"
            assert batch["locked"] == 0

            metrics = svc.get_run_metrics(run_id)
            assert len(metrics) > 0, "应有指标数据"

            out_dir = os.path.join(tmpdir, "exports")
            metrics_path = svc.export_metrics(batch_id, os.path.join(out_dir, "metrics.csv"))
            assert os.path.exists(metrics_path)
            with open(metrics_path, "r", encoding="utf-8-sig") as f:
                lines = f.readlines()
                assert len(lines) > 1, "导出的 CSV 应包含表头和数据行"

            r1.ok()
            print(f"  [PASS] {r1.name}  (batch_id={batch_id}, run_id={run_id}, metrics={len(metrics)})")
        except Exception as e:
            r1.fail(str(e))
            print(f"  [FAIL] {r1.name}: {e}")

        # ====== 测试 2: 行级错误（时间戳缺失、非数字值） ======
        r2 = TestResult("测试2: 行级错误识别（时间戳缺失+非数字值）")
        results.append(r2)
        try:
            svc = PipelineService(db_path)
            latest = svc.get_latest_run(batch_id)
            errors = svc.get_run_errors(latest["id"])

            has_missing_ts = any(e["error_type"] == "missing_timestamp" for e in errors)
            has_non_numeric = any(e["error_type"] == "non_numeric_value" for e in errors)
            assert has_missing_ts, "应检测到缺失时间戳的错误"
            assert has_non_numeric, "应检测到非数字传感器值的错误"

            errors_path = svc.export_errors(batch_id, os.path.join(out_dir, "errors.csv"))
            assert os.path.exists(errors_path)

            r2.ok()
            print(f"  [PASS] {r2.name}  (errors={len(errors)}, missing_ts={has_missing_ts}, non_numeric={has_non_numeric})")
        except Exception as e:
            r2.fail(str(e))
            print(f"  [FAIL] {r2.name}: {e}")

        # ====== 测试 3: 修改阈值后重跑，生成新运行记录 ======
        r3 = TestResult("测试3: 修改阈值后重跑，生成新运行记录")
        results.append(r3)
        try:
            svc = PipelineService(db_path)
            old_cfg = json.loads(svc.get_batch(batch_id)["config_json"])
            old_version = old_cfg["version"]

            new_cfg = svc.set_threshold(batch_id, zscore_threshold=1.5)
            assert new_cfg["version"] == old_version + 1, f"配置版本应从 {old_version} 递增到 {old_version + 1}"

            before_runs = svc.list_runs(batch_id)
            run_id2, run_number2 = svc.process_batch(batch_id)
            after_runs = svc.list_runs(batch_id)

            assert run_number2 == 2, f"运行次数应为 2, 实际 {run_number2}"
            assert len(after_runs) == len(before_runs) + 1, "应新增一条运行记录"

            batch = svc.get_batch(batch_id)
            assert batch["status"] == "processed"
            assert batch["config_version"] == new_cfg["version"]

            anomalies_v1 = svc.get_run_anomalies(latest["id"])
            anomalies_v2 = svc.get_run_anomalies(run_id2)
            assert len(anomalies_v2) >= len(anomalies_v1), \
                f"降低阈值后异常点应更多或相等: v1={len(anomalies_v1)}, v2={len(anomalies_v2)}"

            r3.ok()
            print(f"  [PASS] {r3.name}  (run_number={run_number2}, config_v={new_cfg['version']}, anomalies_v1={len(anomalies_v1)}, anomalies_v2={len(anomalies_v2)})")
        except Exception as e:
            r3.fail(str(e))
            print(f"  [FAIL] {r3.name}: {e}")

        # ====== 测试 4: 锁定批次不能被覆盖 ======
        r4 = TestResult("测试4: 锁定批次保护，禁止重跑")
        results.append(r4)
        try:
            svc = PipelineService(db_path)
            svc.lock_batch(batch_id)
            batch = svc.get_batch(batch_id)
            assert batch["locked"] == 1
            assert batch["status"] == "locked"

            try:
                svc.process_batch(batch_id)
                assert False, "锁定批次处理应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            try:
                svc.set_threshold(batch_id, zscore_threshold=2.0)
                assert False, "锁定批次修改阈值应抛出 BatchLockedError"
            except BatchLockedError:
                pass

            runs_after_lock = svc.list_runs(batch_id)
            assert len(runs_after_lock) == 2, "锁定后不应增加运行记录"

            r4.ok()
            print(f"  [PASS] {r4.name}  (status={batch['status']}, runs={len(runs_after_lock)})")
        except Exception as e:
            r4.fail(str(e))
            print(f"  [FAIL] {r4.name}: {e}")

        # ====== 测试 5: 重启一致性（关闭再打开数据库检查） ======
        r5 = TestResult("测试5: 重启后批次状态、配置版本、锁定标记、导出指标一致")
        results.append(r5)
        try:
            del svc

            svc2 = PipelineService(db_path)
            batch2 = svc2.get_batch(batch_id)
            assert batch2["status"] == "locked", f"重启后状态应为 locked, 实际 {batch2['status']}"
            assert batch2["locked"] == 1, "重启后锁定标记应保留"
            assert batch2["config_version"] == new_cfg["version"], f"重启后配置版本应为 {new_cfg['version']}, 实际 {batch2['config_version']}"

            runs2 = svc2.list_runs(batch_id)
            assert len(runs2) == 2, f"重启后运行记录数应为 2, 实际 {len(runs2)}"

            metrics2 = svc2.get_run_metrics(run_id2)
            assert len(metrics2) == len(metrics), "重启后指标数量应一致"
            for m1, m2 in zip(sorted(metrics, key=lambda x: (x["sensor_name"], x["metric_name"])),
                              sorted(metrics2, key=lambda x: (x["sensor_name"], x["metric_name"]))):
                assert m1["sensor_name"] == m2["sensor_name"]
                assert m1["metric_name"] == m2["metric_name"]
                assert abs(m1["metric_value"] - m2["metric_value"]) < 1e-9, \
                    f"指标值不一致: {m1} vs {m2}"

            exports2 = svc2.list_exports(batch_id)
            assert len(exports2) >= 2, "重启后导出记录应保留"

            r5.ok()
            print(f"  [PASS] {r5.name}  (status={batch2['status']}, locked={batch2['locked']}, cfg_v={batch2['config_version']}, runs={len(runs2)}, metrics={len(metrics2)}, exports={len(exports2)})")
        except Exception as e:
            r5.fail(str(e))
            print(f"  [FAIL] {r5.name}: {e}")

        # ====== 测试 6: 解锁后可以再次重跑 ======
        r6 = TestResult("测试6: 解锁后可以再次重跑")
        results.append(r6)
        try:
            svc = PipelineService(db_path)
            svc.unlock_batch(batch_id)
            batch = svc.get_batch(batch_id)
            assert batch["locked"] == 0
            assert batch["status"] == "processed"

            run_id3, run_number3 = svc.process_batch(batch_id)
            assert run_number3 == 3

            r6.ok()
            print(f"  [PASS] {r6.name}  (run_number={run_number3})")
        except Exception as e:
            r6.fail(str(e))
            print(f"  [FAIL] {r6.name}: {e}")

        # ====== 汇总 ======
        print()
        total = len(results)
        passed = sum(1 for r in results if r.passed)
        failed = total - passed
        print(f"====== 验收测试结果: {passed}/{total} 通过 ======")
        for r in results:
            mark = "[OK]" if r.passed else "[FAIL]"
            detail = f" - {r.error}" if r.error else ""
            print(f"  {mark} {r.name}{detail}")

        if failed > 0:
            print(f"\n有 {failed} 项测试失败！")
            return 1
        else:
            print("\n所有验收测试通过！")
            return 0

    finally:
        # 清理
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except:
            pass


if __name__ == "__main__":
    sys.exit(run_tests())
