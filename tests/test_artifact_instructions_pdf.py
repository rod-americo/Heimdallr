import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from heimdallr.metrics.artifact_instructions_pdf import (
    _format_parenchymal_organs_summary,
    _parenchymal_steatosis_summary,
    build_artifact_instructions_pdf,
    build_artifact_instructions_secondary_capture,
)


class TestArtifactInstructionsPdf(unittest.TestCase):
    def test_parenchymal_summary_includes_native_kidney_and_suspected_allograft(self):
        summary = _format_parenchymal_organs_summary(
            {
                "kidney_right": {
                    "analysis_status": "complete",
                    "volume_cm3": 30.15,
                    "hu_mean": 18.0,
                }
            },
            renal_anatomy_qc={
                "suspected_renal_allografts": [
                    {"source_mask": "kidney_right", "volume_cm3": 150.3}
                ]
            },
            locale="en_US",
        )

        self.assertIn("right kidney: 30 cm³", summary)
        self.assertIn("suspected right renal allograft: 150 cm³", summary)

    def test_parenchymal_steatosis_summary_uses_current_pdff_relation(self):
        summary = _parenchymal_steatosis_summary(
            {
                "density_suppressed_due_to_contrast": False,
                "organs": {
                    "liver": {"hu_mean": 47.92},
                    "spleen": {"hu_mean": 48.0},
                },
            },
            locale="en_US",
        )

        self.assertIn("Estimated PDFF", summary)
        self.assertRegex(summary, r"10[\\.,]4 ?%")

    def test_build_artifact_instructions_pdf_creates_pdf_for_completed_modules(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "CasePDF_20260415_1"
            case_dir = root / "studies" / case_id
            (case_dir / "metadata").mkdir(parents=True, exist_ok=True)
            (case_dir / "artifacts" / "metrics" / "l3_muscle_area").mkdir(parents=True, exist_ok=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "PatientName": "Alice Example",
                        "AccessionNumber": "123",
                        "StudyDate": "20260415",
                    }
                ),
                encoding="utf-8",
            )
            (case_dir / "artifacts" / "metrics" / "l3_muscle_area" / "result.json").write_text(
                json.dumps(
                    {
                        "metric_key": "l3_muscle_area",
                        "status": "done",
                        "measurement": {
                            "skeletal_muscle_area_cm2": 42.5,
                        },
                    }
                ),
                encoding="utf-8",
            )

            output_path = case_dir / "artifacts" / "metrics" / "instructions" / "artifact_instructions.pdf"

            with (
                patch("heimdallr.metrics.artifact_instructions_pdf.study_dir", return_value=case_dir),
                patch("heimdallr.metrics.artifact_instructions_pdf.study_id_json", return_value=case_dir / "metadata" / "id.json"),
                patch("heimdallr.metrics.artifact_instructions_pdf.study_artifacts_dir", return_value=case_dir / "artifacts"),
            ):
                built = build_artifact_instructions_pdf(case_id)

                self.assertEqual(built, output_path)
                self.assertTrue(output_path.exists())
                self.assertGreater(output_path.stat().st_size, 0)

    def test_build_artifact_instructions_secondary_capture_creates_series(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "CasePDF_20260415_1"
            case_dir = root / "studies" / case_id
            (case_dir / "metadata").mkdir(parents=True, exist_ok=True)
            (case_dir / "artifacts" / "metrics" / "l3_muscle_area").mkdir(parents=True, exist_ok=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "StudyInstanceUID": "1.2.3",
                        "PatientName": "Alice Example",
                        "AccessionNumber": "123",
                        "StudyDate": "20260415",
                    }
                ),
                encoding="utf-8",
            )
            (case_dir / "artifacts" / "metrics" / "l3_muscle_area" / "result.json").write_text(
                json.dumps(
                    {
                        "metric_key": "l3_muscle_area",
                        "status": "done",
                        "measurement": {
                            "skeletal_muscle_area_cm2": 42.5,
                        },
                    }
                ),
                encoding="utf-8",
            )

            output_dir = case_dir / "artifacts" / "metrics" / "instructions" / "dicom_sc"

            with (
                patch("heimdallr.metrics.artifact_instructions_pdf.study_dir", return_value=case_dir),
                patch("heimdallr.metrics.artifact_instructions_pdf.study_id_json", return_value=case_dir / "metadata" / "id.json"),
                patch("heimdallr.metrics.artifact_instructions_pdf.study_artifacts_dir", return_value=case_dir / "artifacts"),
            ):
                built = build_artifact_instructions_secondary_capture(case_id)

            self.assertTrue(built["series_instance_uid"])
            self.assertGreater(len(built["paths"]), 0)
            self.assertEqual(output_dir, built["paths"][0].parent)
            for path in built["paths"]:
                self.assertTrue(path.exists())
                self.assertGreater(path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
