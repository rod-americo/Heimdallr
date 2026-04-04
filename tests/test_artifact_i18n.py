import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heimdallr.metrics.jobs._l3_overlay_text import build_overlay_text, resolve_artifact_locale  # noqa: E402


class TestArtifactI18n(unittest.TestCase):
    def test_l3_overlay_locale_defaults_from_presentation_settings(self):
        with patch("heimdallr.metrics.jobs._l3_overlay_text.settings.ARTIFACTS_LOCALE", "pt_BR"):
            self.assertEqual(resolve_artifact_locale({}), "pt_BR")

    def test_l3_overlay_locale_allows_job_override(self):
        with patch("heimdallr.metrics.jobs._l3_overlay_text.settings.ARTIFACTS_LOCALE", "pt_BR"):
            self.assertEqual(resolve_artifact_locale({"locale": "en_US"}), "en_US")

    def test_l3_overlay_text_in_pt_br_uses_decimal_comma(self):
        title, lines = build_overlay_text(
            slice_idx=87,
            probable_viewer_slice_index_one_based=164,
            muscle_area_cm2=42.15,
            height_m=1.75,
            smi_cm2_m2=13.76,
            locale="pt_BR",
        )

        self.assertEqual(title, "Corte no centro L3")
        self.assertEqual(
            lines,
            [
                "SMA (Área músculo-esquelética): 42,1 cm²",
                "Corte NIfTI: 87",
                "Provável corte no visualizador: 164",
                "Altura do paciente: 1,75 m",
                "SMI (Índice de Massa Muscular Esquelética): 13,8 cm²/m²",
            ],
        )

    def test_l3_overlay_text_in_en_us_uses_decimal_point(self):
        title, lines = build_overlay_text(
            slice_idx=12,
            probable_viewer_slice_index_one_based=42,
            muscle_area_cm2=38.04,
            height_m=1.82,
            smi_cm2_m2=11.48,
            locale="en_US",
        )

        self.assertEqual(title, "L3 Center Slice")
        self.assertEqual(
            lines,
            [
                "SMA (Skeletal muscle area): 38.0 cm²",
                "NIfTI slice: 12",
                "Probable viewer slice: 42",
                "Patient height: 1.82 m",
                "SMI (Skeletal Muscle Index): 11.5 cm²/m²",
            ],
        )


if __name__ == "__main__":
    unittest.main()
