import json
import sqlite3
import unittest
from unittest.mock import patch

from heimdallr.shared import store
from heimdallr.resource_monitor import worker


class ResourceMonitorStoreTests(unittest.TestCase):
    def test_list_resource_monitor_active_case_ids(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO study_handoff_state (
                    study_uid, manifest_digest, case_id, status, first_seen_at, last_seen_at
                ) VALUES ('1.2.3', 'abc', 'CasePrepare', 'preparing', '2026-04-17 10:00:00', '2026-04-17 10:00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at, claimed_at)
                VALUES ('CaseSeg', '/tmp/seg', 'claimed', '2026-04-17 10:00:00', '2026-04-17 10:00:10')
                """
            )
            conn.execute(
                """
                INSERT INTO metrics_queue (case_id, input_path, status, created_at, claimed_at)
                VALUES ('CaseMetrics', '/tmp/met', 'claimed', '2026-04-17 10:00:00', '2026-04-17 10:00:10')
                """
            )
            conn.execute(
                """
                INSERT INTO dicom_egress_queue (
                    case_id, study_uid, artifact_path, artifact_type, destination_name,
                    destination_host, destination_port, destination_called_aet, status, created_at, claimed_at
                ) VALUES (
                    'CaseEgress', '1.2.4', 'artifact.dcm', 'secondary_capture', 'PACS',
                    '127.0.0.1', 104, 'PACS', 'claimed', '2026-04-17 10:00:00', '2026-04-17 10:00:10'
                )
                """
            )
            conn.commit()

            self.assertEqual(store.list_resource_monitor_active_case_ids(conn, stage="prepare"), ["CasePrepare"])
            self.assertEqual(store.list_resource_monitor_active_case_ids(conn, stage="segmentation"), ["CaseSeg"])
            self.assertEqual(store.list_resource_monitor_active_case_ids(conn, stage="metrics"), ["CaseMetrics"])
            self.assertEqual(store.list_resource_monitor_active_case_ids(conn, stage="egress"), ["CaseEgress"])
        finally:
            conn.close()

    def test_insert_resource_monitor_samples_persists_rows(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            store.ensure_schema(conn)
            store.insert_resource_monitor_samples(
                conn,
                [
                    {
                        "sampled_at": "2026-04-17 10:00:00",
                        "service_slug": "metrics",
                        "service_unit": "heimdallr-metrics.service",
                        "stage": "metrics",
                        "main_pid": 123,
                        "subtree_pids_json": json.dumps([123, 456]),
                        "active_case_ids_json": json.dumps(["CaseA"]),
                        "rss_mb": 120.5,
                        "peak_rss_mb": 130.0,
                        "subtree_rss_mb": 240.0,
                        "subtree_peak_rss_mb": 260.0,
                        "subtree_pss_mb": 180.0,
                        "major_faults": 7,
                        "cgroup_memory_current_mb": 250.0,
                        "cgroup_memory_peak_mb": 275.0,
                        "host_mem_total_mb": 32000.0,
                        "host_mem_available_mb": 12000.0,
                        "host_swap_used_mb": 0.0,
                        "host_mem_used_percent": 62.5,
                        "notes_json": json.dumps({"active_state": "active"}),
                    }
                ],
            )
            row = conn.execute(
                "SELECT service_slug, main_pid, rss_mb, subtree_pss_mb, active_case_ids_json FROM resource_monitor_samples"
            ).fetchone()
            self.assertEqual(row["service_slug"], "metrics")
            self.assertEqual(row["main_pid"], 123)
            self.assertEqual(row["rss_mb"], 120.5)
            self.assertEqual(row["subtree_pss_mb"], 180.0)
            self.assertEqual(json.loads(row["active_case_ids_json"]), ["CaseA"])

            peak_row = conn.execute(
                """
                SELECT case_id, stage, sample_count, max_main_rss_mb, max_subtree_pss_mb
                FROM resource_monitor_case_peaks
                """
            ).fetchone()
            self.assertEqual(peak_row["case_id"], "CaseA")
            self.assertEqual(peak_row["stage"], "metrics")
            self.assertEqual(peak_row["sample_count"], 1)
            self.assertEqual(peak_row["max_main_rss_mb"], 120.5)
            self.assertEqual(peak_row["max_subtree_pss_mb"], 180.0)
        finally:
            conn.close()

    def test_insert_resource_monitor_samples_skips_shared_case_attribution(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            store.ensure_schema(conn)
            store.insert_resource_monitor_samples(
                conn,
                [
                    {
                        "sampled_at": "2026-04-17 10:00:00",
                        "service_slug": "segmentation",
                        "service_unit": "heimdallr-segmentation.service",
                        "stage": "segmentation",
                        "main_pid": 456,
                        "subtree_pids_json": json.dumps([456, 789]),
                        "active_case_ids_json": json.dumps(["CaseA", "CaseB"]),
                        "rss_mb": 80.0,
                        "peak_rss_mb": 81.0,
                        "subtree_rss_mb": 200.0,
                        "subtree_peak_rss_mb": 220.0,
                        "subtree_pss_mb": 150.0,
                        "major_faults": 3,
                        "cgroup_memory_current_mb": 500.0,
                        "cgroup_memory_peak_mb": 700.0,
                        "host_mem_total_mb": 32000.0,
                        "host_mem_available_mb": 15000.0,
                        "host_swap_used_mb": 0.0,
                        "host_mem_used_percent": 53.0,
                        "notes_json": json.dumps({"active_state": "active"}),
                    }
                ],
            )
            count = conn.execute("SELECT COUNT(*) FROM resource_monitor_case_peaks").fetchone()[0]
            self.assertEqual(count, 0)
        finally:
            conn.close()


class ResourceMonitorWorkerTests(unittest.TestCase):
    def test_parse_smaps_rollup_pss_returns_zero_when_missing(self):
        self.assertEqual(worker._parse_smaps_rollup_pss(worker.Path("/definitely/missing")), 0)

    def test_parse_proc_stat_major_faults_reads_field(self):
        raw = "123 (python) S 1 2 3 4 5 6 7 8 9 10 11 12"
        self.assertEqual(worker._parse_proc_stat_major_faults(raw), 9)

    def test_systemd_properties_reads_system_unit_by_default(self):
        with patch.object(worker.subprocess, "check_output", return_value="ActiveState=active\nExecMainPID=123\n") as mocked:
            props = worker._systemd_properties("heimdallr-prepare.service")

        self.assertEqual(props["ActiveState"], "active")
        self.assertEqual(props["ExecMainPID"], "123")
        mocked.assert_called_once_with(
            [
                "systemctl",
                "show",
                "heimdallr-prepare.service",
                "-p",
                "ActiveState",
                "-p",
                "ExecMainPID",
                "-p",
                "ControlGroup",
            ],
            text=True,
            stderr=worker.subprocess.DEVNULL,
            timeout=5,
        )

    def test_systemd_properties_reads_user_unit_when_prefixed(self):
        with patch.object(worker.subprocess, "check_output", return_value="ActiveState=active\nExecMainPID=456\n") as mocked:
            props = worker._systemd_properties("user:heimdallr-prepare.service")

        self.assertEqual(props["ActiveState"], "active")
        self.assertEqual(props["ExecMainPID"], "456")
        mocked.assert_called_once_with(
            [
                "systemctl",
                "--user",
                "show",
                "heimdallr-prepare.service",
                "-p",
                "ActiveState",
                "-p",
                "ExecMainPID",
                "-p",
                "ControlGroup",
            ],
            text=True,
            stderr=worker.subprocess.DEVNULL,
            timeout=5,
        )


if __name__ == "__main__":
    unittest.main()
