import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nibabel as nib
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heimdallr.metrics.jobs import body_fat_abdominal_volumes, body_fat_l3_slice  # noqa: E402
from heimdallr.metrics.jobs._body_fat_job_common import (  # noqa: E402
    build_abdominal_aggregate,
    compute_level_measurements,
)
from heimdallr.shared import settings  # noqa: E402


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestMetricsBodyFatJobs(unittest.TestCase):
    def test_build_abdominal_aggregate_handles_reversed_z_order(self):
        shape = (8, 8, 20)
        sat = np.zeros(shape, dtype=bool)
        torso = np.zeros(shape, dtype=bool)
        sat[:, :, 2:17] = True
        torso[1:7, 1:7, 2:17] = True
        level_masks = {}
        positions = {
            "T12": (15, 16),
            "L1": (13, 14),
            "L2": (10, 12),
            "L3": (7, 9),
            "L4": (4, 6),
            "L5": (2, 3),
        }
        for level, (z0, z1) in positions.items():
            mask = np.zeros(shape, dtype=bool)
            mask[2:6, 2:6, z0 : z1 + 1] = True
            level_masks[level] = mask

        level_measurements, complete_levels, measurable_levels = compute_level_measurements(
            level_masks=level_masks,
            sat_mask=sat,
            torso_mask=torso,
            spacing_xyz=(1.0, 1.0, 2.0),
        )
        aggregate = build_abdominal_aggregate(
            level_measurements=level_measurements,
            complete_levels=complete_levels,
            measurable_levels=measurable_levels,
            sat_mask=sat,
            torso_mask=torso,
            spacing_xyz=(1.0, 1.0, 2.0),
        )

        self.assertEqual(aggregate["measured_region"], "T12-L5")
        self.assertEqual(aggregate["slice_range"], [2, 16])
        self.assertTrue(aggregate["coverage_complete"])
        self.assertGreater(aggregate["torso_fat_volume_cm3"], 0)

    def test_body_fat_jobs_write_metric_payloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseFat_20260403_789"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "artifacts" / "tissue_types").mkdir(parents=True)

            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "Modality": "CT",
                        "StudyInstanceUID": "1.2.3.4.5",
                        "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
                    }
                ),
                encoding="utf-8",
            )
            (case_dir / "metadata" / "metadata.json").write_text("{}", encoding="utf-8")
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            ct = np.zeros((12, 12, 20), dtype=np.float32)
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 2.0))

            for level, (z0, z1) in {
                "T12": (15, 16),
                "L1": (13, 14),
                "L2": (10, 12),
                "L3": (7, 9),
                "L4": (4, 6),
                "L5": (2, 3),
            }.items():
                vertebra = np.zeros_like(ct, dtype=np.float32)
                vertebra[2:10, 2:10, z0 : z1 + 1] = 1.0
                write_nifti(case_dir / "artifacts" / "total" / f"vertebrae_{level}.nii.gz", vertebra)

            sat = np.zeros_like(ct, dtype=np.float32)
            torso = np.zeros_like(ct, dtype=np.float32)
            sat[6:11, 6:11, 2:17] = 1.0
            torso[1:6, 1:6, 2:17] = 1.0
            write_nifti(case_dir / "artifacts" / "tissue_types" / "subcutaneous_fat.nii.gz", sat)
            write_nifti(case_dir / "artifacts" / "tissue_types" / "torso_fat.nii.gz", torso)

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(sys, "argv", ["body_fat_abdominal_volumes", "--case-id", case_id, "--job-config-json", '{"generate_overlay": true}']):
                    self.assertEqual(body_fat_abdominal_volumes.main(), 0)
                with patch.object(sys, "argv", ["body_fat_l3_slice", "--case-id", case_id, "--job-config-json", '{"generate_overlay": true}']):
                    self.assertEqual(body_fat_l3_slice.main(), 0)

            abdominal_result = json.loads(
                (case_dir / "artifacts" / "metrics" / "body_fat_abdominal_volumes" / "result.json").read_text(encoding="utf-8")
            )
            l3_result = json.loads(
                (case_dir / "artifacts" / "metrics" / "body_fat_l3_slice" / "result.json").read_text(encoding="utf-8")
            )

            self.assertEqual(abdominal_result["status"], "done")
            self.assertTrue(abdominal_result["measurement"]["aggregate"]["coverage_complete"])
            self.assertGreater(abdominal_result["measurement"]["aggregate"]["visceral_proxy_volume_cm3"], 0)
            self.assertIn("overlay_png", abdominal_result["artifacts"])

            self.assertEqual(l3_result["status"], "done")
            self.assertGreater(l3_result["measurement"]["visceral_proxy_area_cm2"], 0)
            self.assertIn("overlay_png", l3_result["artifacts"])


if __name__ == "__main__":
    unittest.main()
