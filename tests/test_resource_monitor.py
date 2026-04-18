import json
import sqlite3
import unittest

from heimdallr.shared import store


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
                "SELECT service_slug, main_pid, rss_mb, active_case_ids_json FROM resource_monitor_samples"
            ).fetchone()
            self.assertEqual(row["service_slug"], "metrics")
            self.assertEqual(row["main_pid"], 123)
            self.assertEqual(row["rss_mb"], 120.5)
            self.assertEqual(json.loads(row["active_case_ids_json"]), ["CaseA"])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
