import json
import gzip
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import nibabel as nib
import numpy as np

from heimdallr.segmentation.worker import (
    PipelineLogger,
    _extra_args_use_gpu,
    _record_segmentation_pipeline_state,
    WorkerShutdownRequestedError,
    run_segmentation_pipeline,
    resolve_segmentation_plan,
    segment_case,
    should_reuse_existing_segmentation,
)
from heimdallr.segmentation import worker as segmentation_worker
from heimdallr.shared import store
from heimdallr.shared.segmentation_inventory import build_segmentation_inventory


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestSegmentationReuse(unittest.TestCase):
    def test_gpu_device_argument_detection_covers_supported_cli_forms(self):
        self.assertTrue(_extra_args_use_gpu(["--device", "gpu"]))
        self.assertTrue(_extra_args_use_gpu(["-d", "gpu:0"]))
        self.assertTrue(_extra_args_use_gpu(["--device=gpu:1"]))
        self.assertFalse(_extra_args_use_gpu(["--device", "cpu"]))
        self.assertFalse(_extra_args_use_gpu(["--device", "cpu", "--fast"]))
        with patch.object(segmentation_worker.sys, "platform", "linux"):
            self.assertTrue(_extra_args_use_gpu([]))

    def test_build_segmentation_inventory_uses_brain_l3_and_organ_presence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            nifti_path = root / "image.nii.gz"
            total_dir = root / "artifacts" / "total"
            write_nifti(nifti_path, np.zeros((10, 10, 10), dtype=np.float32))

            brain = np.zeros((10, 10, 10), dtype=np.float32)
            brain[3:7, 3:7, 3:7] = 1.0
            l3 = np.zeros((10, 10, 10), dtype=np.float32)
            l3[3:7, 3:7, 3:7] = 1.0
            liver = np.zeros((10, 10, 10), dtype=np.float32)
            liver[1:8, 1:8, 0:4] = 1.0
            lung = np.zeros((10, 10, 10), dtype=np.float32)
            lung[2:8, 2:8, 0:3] = 1.0
            write_nifti(total_dir / "brain.nii.gz", brain)
            write_nifti(total_dir / "vertebrae_L3.nii.gz", l3)
            write_nifti(total_dir / "liver.nii.gz", liver)
            write_nifti(total_dir / "lung_lower_lobe_right.nii.gz", lung)

            inventory = build_segmentation_inventory(total_dir, nifti_path)

        self.assertTrue(inventory["brain"]["complete"])
        self.assertTrue(inventory["vertebrae_L3"]["complete"])
        self.assertEqual(inventory["parenchymal_organs"]["present"], ["liver"])
        self.assertTrue(inventory["parenchymal_organs"]["any_present"])
        self.assertTrue(inventory["liver"]["present"])
        self.assertEqual(inventory["lungs"]["present_lobes"], ["lung_lower_lobe_right"])
        self.assertEqual(inventory["lungs"]["complete_lobes"], [])
        self.assertTrue(inventory["lungs"]["any_present"])

    def test_segment_case_propagates_worker_shutdown_for_queue_retry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            case_id = "ShutdownCase_20260420_1"
            case_dir = base / case_id
            metadata_dir = case_dir / "metadata"
            logs_dir = case_dir / "logs"
            artifacts_dir = case_dir / "artifacts"
            derived_dir = case_dir / "derived"
            metadata_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            derived_dir.mkdir(parents=True, exist_ok=True)

            id_json_path = metadata_dir / "id.json"
            id_json_path.write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "StudyInstanceUID": "1.2.3",
                        "PatientName": "Alice Example",
                        "AccessionNumber": "1",
                        "StudyDate": "20260420",
                        "Modality": "CT",
                        "Pipeline": {},
                    }
                ),
                encoding="utf-8",
            )
            selected_nifti = base / "selected.nii.gz"
            selected_nifti.write_bytes(gzip.compress(b"1"))
            shutdown_error = WorkerShutdownRequestedError(
                "[total] Worker shutdown requested while task was still running"
            )

            with (
                patch("heimdallr.segmentation.worker.study_dir", return_value=case_dir),
                patch("heimdallr.segmentation.worker.study_artifacts_dir", return_value=artifacts_dir),
                patch("heimdallr.segmentation.worker.study_derived_dir", return_value=derived_dir),
                patch("heimdallr.segmentation.worker.study_logs_dir", return_value=logs_dir),
                patch("heimdallr.segmentation.worker.study_metadata_dir", return_value=metadata_dir),
                patch("heimdallr.segmentation.worker.study_id_json", return_value=id_json_path),
                patch(
                    "heimdallr.segmentation.worker.select_prepared_series",
                    return_value=(
                        selected_nifti,
                        {
                            "SelectedSeriesNumber": "2",
                            "SelectedPhase": "native",
                            "SliceCount": 100,
                            "SelectedSeriesInstanceUID": "1.2.3.4.5",
                        },
                    ),
                ),
                patch(
                    "heimdallr.segmentation.worker.resolve_segmentation_plan",
                    return_value=("ct_native_segmentation_only", [{"name": "total"}]),
                ),
                patch(
                    "heimdallr.segmentation.worker.should_reuse_existing_segmentation",
                    return_value=(False, None),
                ),
                patch(
                    "heimdallr.segmentation.worker.run_segmentation_pipeline",
                    side_effect=shutdown_error,
                ),
                patch("heimdallr.segmentation.worker.db_connect", return_value=MagicMock()),
                patch("heimdallr.segmentation.worker.store.update_id_json"),
            ):
                with self.assertRaises(WorkerShutdownRequestedError):
                    segment_case(case_dir)

            payload = json.loads(id_json_path.read_text(encoding="utf-8"))
            pipeline = payload["Pipeline"]
            self.assertEqual(pipeline["segmentation_status"], "error")
            self.assertEqual(
                pipeline["segmentation_error"],
                "[total] Worker shutdown requested while task was still running",
            )
            self.assertEqual(
                (logs_dir / "error.log").read_text(encoding="utf-8"),
                "[total] Worker shutdown requested while task was still running",
            )

    def test_resolve_segmentation_plan_accepts_portal_venous_fallback(self):
        with patch(
            "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
            return_value=(
                "ct_native_segmentation_only",
                {
                    "required": {"modality": "CT", "selected_phase": ["native"]},
                    "tasks": [{"name": "total", "enabled": True}],
                },
            ),
        ):
            profile_name, tasks = resolve_segmentation_plan("CT", "portal_venous")

        self.assertEqual(profile_name, "ct_native_segmentation_only")
        self.assertEqual(tasks, [{"name": "total", "enabled": True}])

    def test_resolve_segmentation_plan_accepts_any_contrast_fallback(self):
        with patch(
            "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
            return_value=(
                "ct_native_segmentation_only",
                {
                    "required": {"modality": "CT", "selected_phase": ["native"]},
                    "tasks": [{"name": "total", "enabled": True}],
                },
            ),
        ):
            profile_name, tasks = resolve_segmentation_plan("CT", "arterial")

        self.assertEqual(profile_name, "ct_native_segmentation_only")
        self.assertEqual(tasks, [{"name": "total", "enabled": True}])

    def test_resolve_segmentation_plan_filters_tasks_for_requested_metrics(self):
        segmentation_profile = {
            "required": {"modality": "CT", "selected_phase": ["native"]},
            "tasks": [
                {"name": "total", "enabled": True, "output_dir": "artifacts/total"},
                {"name": "tissue_types", "enabled": True, "output_dir": "artifacts/tissue_types"},
            ],
        }
        metrics_profile = {
            "jobs": [
                {
                    "name": "bone_health_l1_hu",
                    "enabled": True,
                    "requires_segmentation_tasks": ["total"],
                },
                {
                    "name": "l3_muscle_area",
                    "enabled": True,
                    "requires_segmentation_tasks": ["total", "tissue_types"],
                },
            ],
        }
        with (
            patch(
                "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
                return_value=("ct_native_segmentation_only", segmentation_profile),
            ),
            patch(
                "heimdallr.segmentation.worker.load_metrics_pipeline_profile_for_segmentation",
                return_value=("ct_native_basic_metrics", metrics_profile),
            ),
        ):
            _profile_name, tasks = resolve_segmentation_plan(
                "CT",
                "native",
                requested_metrics_modules=["bone_health_l1_hu"],
            )

        self.assertEqual([task["name"] for task in tasks], ["total"])

    def test_resolve_segmentation_plan_keeps_tissue_types_when_requested_metric_needs_it(self):
        segmentation_profile = {
            "required": {"modality": "CT", "selected_phase": ["native"]},
            "tasks": [
                {"name": "total", "enabled": True, "output_dir": "artifacts/total"},
                {"name": "tissue_types", "enabled": True, "output_dir": "artifacts/tissue_types"},
            ],
        }
        metrics_profile = {
            "jobs": [
                {
                    "name": "l3_muscle_area",
                    "enabled": True,
                    "requires_segmentation_tasks": ["total", "tissue_types"],
                },
            ],
        }
        with (
            patch(
                "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
                return_value=("ct_native_segmentation_only", segmentation_profile),
            ),
            patch(
                "heimdallr.segmentation.worker.load_metrics_pipeline_profile_for_segmentation",
                return_value=("ct_native_basic_metrics", metrics_profile),
            ),
        ):
            _profile_name, tasks = resolve_segmentation_plan(
                "CT",
                "native",
                requested_metrics_modules=["l3_muscle_area"],
            )

        self.assertEqual([task["name"] for task in tasks], ["total", "tissue_types"])

    def test_resolve_segmentation_plan_keeps_lung_nodules_when_requested_metric_needs_it(self):
        segmentation_profile = {
            "required": {"modality": "CT", "selected_phase": ["native"]},
            "tasks": [
                {"name": "total", "enabled": True, "output_dir": "artifacts/total"},
                {"name": "lung_nodules", "enabled": True, "output_dir": "artifacts/lung_nodules"},
                {"name": "tissue_types", "enabled": True, "output_dir": "artifacts/tissue_types"},
            ],
        }
        metrics_profile = {
            "jobs": [
                {
                    "name": "lung_nodules",
                    "enabled": True,
                    "requires_segmentation_tasks": ["total", "lung_nodules"],
                },
            ],
        }
        with (
            patch(
                "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
                return_value=("ct_native_segmentation_only", segmentation_profile),
            ),
            patch(
                "heimdallr.segmentation.worker.load_metrics_pipeline_profile_for_segmentation",
                return_value=("ct_native_basic_metrics", metrics_profile),
            ),
        ):
            _profile_name, tasks = resolve_segmentation_plan(
                "CT",
                "native",
                requested_metrics_modules=["lung_nodules"],
            )

        self.assertEqual([task["name"] for task in tasks], ["total", "lung_nodules"])

    def test_resolve_segmentation_plan_keeps_effusion_task_when_requested_metric_needs_it(self):
        segmentation_profile = {
            "required": {"modality": "CT", "selected_phase": ["native"]},
            "tasks": [
                {"name": "total", "enabled": True, "output_dir": "artifacts/total"},
                {
                    "name": "pleural_pericard_effusion",
                    "enabled": True,
                    "output_dir": "artifacts/pleural_pericard_effusion",
                },
            ],
        }
        metrics_profile = {
            "jobs": [
                {
                    "name": "pleural_pericard_effusion",
                    "enabled": True,
                    "requires_segmentation_tasks": ["total", "pleural_pericard_effusion"],
                },
            ],
        }
        with (
            patch(
                "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
                return_value=("ct_native_segmentation_only", segmentation_profile),
            ),
            patch(
                "heimdallr.segmentation.worker.load_metrics_pipeline_profile_for_segmentation",
                return_value=("ct_native_basic_metrics", metrics_profile),
            ),
        ):
            _profile_name, tasks = resolve_segmentation_plan(
                "CT",
                "native",
                requested_metrics_modules=["pleural_pericard_effusion"],
            )

        self.assertEqual(
            [task["name"] for task in tasks],
            ["total", "pleural_pericard_effusion"],
        )

    def test_resolve_segmentation_plan_includes_automatic_head_tasks_with_requested_metric(self):
        segmentation_profile = {
            "required": {"modality": "CT", "selected_phase": ["native"]},
            "tasks": [
                {"name": "total", "enabled": True, "output_dir": "artifacts/total"},
                {"name": "tissue_types", "enabled": True, "output_dir": "artifacts/tissue_types"},
                {"name": "cerebral_bleed", "enabled": True, "output_dir": "artifacts/cerebral_bleed"},
                {"name": "brain_structures", "enabled": True, "output_dir": "artifacts/brain_structures"},
            ],
        }
        metrics_profile = {
            "jobs": [
                {
                    "name": "bone_health_l1_hu",
                    "enabled": True,
                    "requires_segmentation_tasks": ["total"],
                },
                {
                    "name": "head_complete_qc",
                    "enabled": True,
                    "automatic": True,
                    "requires_segmentation_tasks": [
                        "total",
                        "cerebral_bleed",
                        "brain_structures",
                    ],
                },
            ],
        }
        with (
            patch(
                "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
                return_value=("ct_native_segmentation_only", segmentation_profile),
            ),
            patch(
                "heimdallr.segmentation.worker.load_metrics_pipeline_profile_for_segmentation",
                return_value=("ct_native_basic_metrics", metrics_profile),
            ),
        ):
            _profile_name, tasks = resolve_segmentation_plan(
                "CT",
                "native",
                requested_metrics_modules=["bone_health_l1_hu"],
            )

        self.assertEqual(
            [task["name"] for task in tasks],
            ["total", "cerebral_bleed", "brain_structures"],
        )

    def test_resolve_segmentation_plan_rejects_missing_required_task(self):
        segmentation_profile = {
            "required": {"modality": "CT", "selected_phase": ["native"]},
            "tasks": [
                {"name": "total", "enabled": True, "output_dir": "artifacts/total"},
            ],
        }
        metrics_profile = {
            "jobs": [
                {
                    "name": "head_complete_qc",
                    "enabled": True,
                    "requires_segmentation_tasks": [
                        "total",
                        "cerebral_bleed",
                        "brain_structures",
                    ],
                },
            ],
        }
        with (
            patch(
                "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
                return_value=("ct_native_segmentation_only", segmentation_profile),
            ),
            patch(
                "heimdallr.segmentation.worker.load_metrics_pipeline_profile_for_segmentation",
                return_value=("ct_head_complete_metrics", metrics_profile),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "cerebral_bleed"):
                resolve_segmentation_plan(
                    "CT",
                    "native",
                    requested_metrics_modules=["head_complete_qc"],
                )

    def test_run_segmentation_pipeline_preserves_total_args_and_skips_tissue_without_complete_l3(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "CaseGate"
            artifacts_dir = case_output / "artifacts"
            log_dir = case_output / "logs"
            nifti_path = case_output / "derived" / "CaseGate.nii.gz"
            write_nifti(nifti_path, np.zeros((10, 10, 10), dtype=np.float32))
            calls: list[tuple[str, list[str]]] = []

            def fake_run_task(task_name, _input_file, output_folder, extra_args=None, log_file=None):
                calls.append((task_name, list(extra_args or [])))
                if task_name == "total":
                    l3 = np.zeros((10, 10, 10), dtype=np.float32)
                    l3[3:7, 3:7, 0:4] = 1.0
                    write_nifti(Path(output_folder) / "vertebrae_L3.nii.gz", l3)
                else:
                    Path(output_folder, "mask.nii.gz").write_bytes(gzip.compress(b"ok"))

            with (
                patch(
                    "heimdallr.segmentation.worker.resolve_segmentation_plan",
                    return_value=(
                        "ct_native_segmentation_only",
                        [
                            {
                                "name": "total",
                                "output_dir": "artifacts/total",
                                "extra_args": ["--fast", "--device", "gpu"],
                            },
                            {
                                "name": "tissue_types",
                                "output_dir": "artifacts/tissue_types",
                                "extra_args": ["--device", "gpu"],
                            },
                        ],
                    ),
                ),
                patch("heimdallr.segmentation.worker.run_task", side_effect=fake_run_task),
            ):
                info = run_segmentation_pipeline(
                    case_id="CaseGate",
                    modality="CT",
                    selected_phase="native",
                    nifti_path=nifti_path,
                    case_output=case_output,
                    artifacts_dir=artifacts_dir,
                    log_dir=log_dir,
                    logger=PipelineLogger(None),
                )

        self.assertEqual(calls, [("total", ["--fast", "--device", "gpu"])])
        self.assertEqual([task["name"] for task in info["tasks"]], ["total"])
        self.assertEqual(info["skipped_tasks"][0]["name"], "tissue_types")
        self.assertEqual(info["skipped_tasks"][0]["gatekeeper"], "l3_complete")

    def test_run_segmentation_pipeline_runs_head_tasks_after_complete_brain_gate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "CaseHeadGate"
            artifacts_dir = case_output / "artifacts"
            log_dir = case_output / "logs"
            nifti_path = case_output / "derived" / "CaseHeadGate.nii.gz"
            write_nifti(nifti_path, np.zeros((10, 10, 10), dtype=np.float32))
            calls: list[str] = []

            def fake_run_task(task_name, _input_file, output_folder, extra_args=None, log_file=None):
                calls.append(task_name)
                if task_name == "total":
                    skull = np.zeros((10, 10, 10), dtype=np.float32)
                    brain = np.zeros((10, 10, 10), dtype=np.float32)
                    skull[0:8, 2:8, 2:8] = 1.0
                    brain[3:7, 3:7, 3:7] = 1.0
                    write_nifti(Path(output_folder) / "skull.nii.gz", skull)
                    write_nifti(Path(output_folder) / "brain.nii.gz", brain)
                else:
                    Path(output_folder, "mask.nii.gz").write_bytes(gzip.compress(b"ok"))

            with (
                patch(
                    "heimdallr.segmentation.worker.resolve_segmentation_plan",
                    return_value=(
                        "ct_native_segmentation_only",
                        [
                            {"name": "total", "output_dir": "artifacts/total"},
                            {"name": "cerebral_bleed", "output_dir": "artifacts/cerebral_bleed"},
                            {"name": "brain_structures", "output_dir": "artifacts/brain_structures"},
                        ],
                    ),
                ),
                patch("heimdallr.segmentation.worker.run_task", side_effect=fake_run_task),
            ):
                info = run_segmentation_pipeline(
                    case_id="CaseHeadGate",
                    modality="CT",
                    selected_phase="native",
                    nifti_path=nifti_path,
                    case_output=case_output,
                    artifacts_dir=artifacts_dir,
                    log_dir=log_dir,
                    logger=PipelineLogger(None),
                )

        self.assertEqual(calls, ["total", "cerebral_bleed", "brain_structures"])
        self.assertTrue(info["gatekeepers"]["head_complete"]["complete"])
        self.assertEqual(info["gatekeepers"]["head_complete"]["reason"], "complete_brain")
        self.assertEqual(
            [task["name"] for task in info["tasks"]],
            ["total", "cerebral_bleed", "brain_structures"],
        )

    def test_run_automatic_ct_pipeline_uses_inventory_to_select_tasks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "CaseAutomatic"
            artifacts_dir = case_output / "artifacts"
            log_dir = case_output / "logs"
            nifti_path = case_output / "derived" / "CaseAutomatic.nii.gz"
            write_nifti(nifti_path, np.zeros((10, 10, 10), dtype=np.float32))
            calls: list[str] = []

            def fake_run_task(task_name, _input_file, output_folder, extra_args=None, log_file=None):
                calls.append(task_name)
                if task_name == "total":
                    l3 = np.zeros((10, 10, 10), dtype=np.float32)
                    liver = np.zeros((10, 10, 10), dtype=np.float32)
                    lung = np.zeros((10, 10, 10), dtype=np.float32)
                    l3[3:7, 3:7, 3:7] = 1.0
                    liver[1:8, 1:8, 0:4] = 1.0
                    lung[1:8, 1:8, 1:9] = 1.0
                    write_nifti(Path(output_folder) / "vertebrae_L3.nii.gz", l3)
                    write_nifti(Path(output_folder) / "liver.nii.gz", liver)
                    write_nifti(Path(output_folder) / "lung_lower_lobe_right.nii.gz", lung)
                else:
                    Path(output_folder, "mask.nii.gz").write_bytes(gzip.compress(b"ok"))

            metrics_profile = {
                "jobs": [
                    {
                        "name": "l3_muscle_area",
                        "requires_segmentation_tasks": ["total", "tissue_types"],
                        "requires_inventory": ["vertebrae_L3.complete"],
                    },
                    {
                        "name": "head_complete_qc",
                        "automatic": True,
                        "requires_segmentation_tasks": [
                            "total",
                            "cerebral_bleed",
                            "brain_structures",
                        ],
                        "requires_inventory": ["brain.complete"],
                    },
                    {
                        "name": "parenchymal_organ_volumetry",
                        "requires_segmentation_tasks": ["total"],
                        "requires_inventory": ["parenchymal_organs.any_present"],
                    },
                    {
                        "name": "pleural_pericard_effusion",
                        "automatic": True,
                        "requires_segmentation_tasks": ["total", "pleural_pericard_effusion"],
                        "requires_inventory": ["lungs.any_present"],
                    },
                    {
                        "name": "liver_lesions",
                        "automatic": True,
                        "requires_segmentation_tasks": ["total", "liver_lesions"],
                        "requires_inventory": ["liver.present"],
                    },
                ],
            }

            with (
                patch(
                    "heimdallr.segmentation.worker.resolve_segmentation_plan",
                    return_value=(
                        "ct_automatic_segmentation",
                        [
                            {"name": "total", "output_dir": "artifacts/total"},
                            {"name": "tissue_types", "output_dir": "artifacts/tissue_types"},
                            {
                                "name": "pleural_pericard_effusion",
                                "output_dir": "artifacts/pleural_pericard_effusion",
                            },
                            {"name": "liver_lesions", "output_dir": "artifacts/liver_lesions"},
                            {"name": "cerebral_bleed", "output_dir": "artifacts/cerebral_bleed"},
                            {"name": "brain_structures", "output_dir": "artifacts/brain_structures"},
                        ],
                    ),
                ),
                patch(
                    "heimdallr.segmentation.worker.load_metrics_pipeline_profile_for_segmentation",
                    return_value=("ct_automatic_metrics", metrics_profile),
                ),
                patch("heimdallr.segmentation.worker.run_task", side_effect=fake_run_task),
            ):
                info = run_segmentation_pipeline(
                    case_id="CaseAutomatic",
                    modality="CT",
                    selected_phase="native",
                    nifti_path=nifti_path,
                    case_output=case_output,
                    artifacts_dir=artifacts_dir,
                    log_dir=log_dir,
                    logger=PipelineLogger(None),
                )

            self.assertEqual(
                calls,
                ["total", "tissue_types", "pleural_pericard_effusion", "liver_lesions"],
            )
            self.assertEqual(
                info["automatic_ct_plan"]["selected_jobs"],
                [
                    "l3_muscle_area",
                    "parenchymal_organ_volumetry",
                    "pleural_pericard_effusion",
                    "liver_lesions",
                ],
            )
            self.assertEqual(
                [task["name"] for task in info["skipped_tasks"]],
                ["cerebral_bleed", "brain_structures"],
            )
            self.assertTrue((artifacts_dir / "segmentation_inventory.json").exists())

    def test_record_segmentation_pipeline_state_closes_failed_stage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            id_json_path = Path(tmpdir) / "id.json"
            id_json_path.write_text(
                json.dumps(
                    {
                        "CaseID": "case-1",
                        "StudyInstanceUID": "1.2.3",
                        "Pipeline": {
                            "prepare_elapsed_time": "0:00:40",
                            "prepare_start_time": "2026-04-10T16:59:00-03:00",
                            "start_time": "2026-04-10T17:00:00-03:00",
                            "segmentation_start_time": "2026-04-10T17:00:00-03:00",
                        },
                    }
                ),
                encoding="utf-8",
            )

            conn = MagicMock()
            with (
                patch("heimdallr.segmentation.worker.study_id_json", return_value=id_json_path),
                patch("heimdallr.segmentation.worker.db_connect", return_value=conn),
                patch("heimdallr.segmentation.worker.store.update_id_json"),
            ):
                _record_segmentation_pipeline_state(
                    "case-1",
                    status="error",
                    end_dt=datetime.fromisoformat("2026-04-10T17:01:15-03:00"),
                    error="segmentation failed",
                )

            payload = json.loads(id_json_path.read_text(encoding="utf-8"))
            pipeline = payload["Pipeline"]
            self.assertEqual(pipeline["segmentation_status"], "error")
            self.assertEqual(pipeline["segmentation_error"], "segmentation failed")
            self.assertEqual(pipeline["segmentation_end_time"], "2026-04-10T17:01:15-03:00")
            self.assertEqual(pipeline["segmentation_elapsed_time"], "0:01:15")
            self.assertEqual(pipeline["pipeline_active_elapsed_time"], "0:01:55")

    def test_reuses_when_sqlite_signature_matches_and_outputs_exist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "case"
            total_dir = case_output / "artifacts" / "total"
            tissue_dir = case_output / "artifacts" / "tissue_types"
            total_dir.mkdir(parents=True, exist_ok=True)
            tissue_dir.mkdir(parents=True, exist_ok=True)
            (total_dir / "mask.nii.gz").write_bytes(gzip.compress(b"1"))
            (tissue_dir / "mask.nii.gz").write_bytes(gzip.compress(b"1"))

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                store.upsert_study_metadata(
                    conn,
                    {
                        "StudyInstanceUID": "1.2.3",
                        "PatientName": "Alice Example",
                        "ClinicalName": "AliceE_20260407_1",
                        "AccessionNumber": "1",
                        "StudyDate": "20260407",
                        "Modality": "CT",
                    },
                )
                store.update_segmentation_signature(
                    conn,
                    "1.2.3",
                    series_instance_uid="1.2.3.4.5",
                    slice_count=476,
                    profile_name="ct_native_segmentation_only",
                    task_names=["total", "tissue_types"],
                    elapsed_time="0:03:21",
                )

                with patch("heimdallr.segmentation.worker.db_connect", return_value=conn):
                    reused, elapsed = should_reuse_existing_segmentation(
                        "1.2.3",
                        case_output,
                        {
                            "SelectedSeriesInstanceUID": "1.2.3.4.5",
                            "SliceCount": 476,
                        },
                        "ct_native_segmentation_only",
                        [
                            {"name": "total", "output_dir": "artifacts/total"},
                            {"name": "tissue_types", "output_dir": "artifacts/tissue_types"},
                        ],
                    )
            finally:
                conn.close()

        self.assertTrue(reused)
        self.assertEqual(elapsed, "0:03:21")

    def test_does_not_reuse_when_slice_count_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "case"
            total_dir = case_output / "artifacts" / "total"
            tissue_dir = case_output / "artifacts" / "tissue_types"
            total_dir.mkdir(parents=True, exist_ok=True)
            tissue_dir.mkdir(parents=True, exist_ok=True)
            (total_dir / "mask.nii.gz").write_bytes(gzip.compress(b"1"))
            (tissue_dir / "mask.nii.gz").write_bytes(gzip.compress(b"1"))

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                store.upsert_study_metadata(
                    conn,
                    {
                        "StudyInstanceUID": "1.2.3",
                        "PatientName": "Alice Example",
                        "ClinicalName": "AliceE_20260407_1",
                        "AccessionNumber": "1",
                        "StudyDate": "20260407",
                        "Modality": "CT",
                    },
                )
                store.update_segmentation_signature(
                    conn,
                    "1.2.3",
                    series_instance_uid="1.2.3.4.5",
                    slice_count=476,
                    profile_name="ct_native_segmentation_only",
                    task_names=["total", "tissue_types"],
                    elapsed_time="0:03:21",
                )

                with patch("heimdallr.segmentation.worker.db_connect", return_value=conn):
                    reused, elapsed = should_reuse_existing_segmentation(
                        "1.2.3",
                        case_output,
                        {
                            "SelectedSeriesInstanceUID": "1.2.3.4.5",
                            "SliceCount": 477,
                        },
                        "ct_native_segmentation_only",
                        [
                            {"name": "total", "output_dir": "artifacts/total"},
                            {"name": "tissue_types", "output_dir": "artifacts/tissue_types"},
                        ],
                    )
            finally:
                conn.close()

        self.assertFalse(reused)
        self.assertIsNone(elapsed)

    def test_does_not_reuse_when_existing_nifti_is_truncated(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "case"
            total_dir = case_output / "artifacts" / "total"
            tissue_dir = case_output / "artifacts" / "tissue_types"
            total_dir.mkdir(parents=True, exist_ok=True)
            tissue_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(total_dir / "mask.nii.gz", "wb") as handle:
                handle.write(b"ok")
            (tissue_dir / "mask.nii.gz").write_bytes(gzip.compress(b"partial")[:-4])

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                store.upsert_study_metadata(
                    conn,
                    {
                        "StudyInstanceUID": "1.2.3",
                        "PatientName": "Alice Example",
                        "ClinicalName": "AliceE_20260407_1",
                        "AccessionNumber": "1",
                        "StudyDate": "20260407",
                        "Modality": "CT",
                    },
                )
                store.update_segmentation_signature(
                    conn,
                    "1.2.3",
                    series_instance_uid="1.2.3.4.5",
                    slice_count=476,
                    profile_name="ct_native_segmentation_only",
                    task_names=["total", "tissue_types"],
                    elapsed_time="0:03:21",
                )

                with patch("heimdallr.segmentation.worker.db_connect", return_value=conn):
                    reused, elapsed = should_reuse_existing_segmentation(
                        "1.2.3",
                        case_output,
                        {
                            "SelectedSeriesInstanceUID": "1.2.3.4.5",
                            "SliceCount": 476,
                        },
                        "ct_native_segmentation_only",
                        [
                            {"name": "total", "output_dir": "artifacts/total"},
                            {"name": "tissue_types", "output_dir": "artifacts/tissue_types"},
                        ],
                    )
            finally:
                conn.close()

        self.assertFalse(reused)
        self.assertIsNone(elapsed)


if __name__ == "__main__":
    unittest.main()
