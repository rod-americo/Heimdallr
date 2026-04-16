import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from heimdallr.metrics.artifact_instructions_pdf import build_artifact_instructions_pdf


class TestArtifactInstructionsPdf(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
