import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heimdallr.metrics.jobs._l3_overlay_text import build_overlay_text, resolve_artifact_locale  # noqa: E402
from heimdallr.metrics.jobs._parenchymal_overlay_text import (  # noqa: E402
    build_overlay_text as build_parenchymal_overlay_text,
    derivation_description as parenchymal_derivation_description,
    resolve_artifact_locale as resolve_parenchymal_locale,
    series_description as parenchymal_series_description,
)
from heimdallr.metrics.jobs._brain_volumetry_overlay_text import (  # noqa: E402
    build_overlay_text as build_brain_overlay_text,
    derivation_description as brain_derivation_description,
    resolve_artifact_locale as resolve_brain_locale,
    series_description as brain_series_description,
)


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
            muscle_density_hu_mean=31.6,
            height_m=1.75,
            smi_cm2_m2=13.76,
            locale="pt_BR",
        )

        self.assertEqual(title, "Corte no centro L3")
        self.assertEqual(
            lines,
            [
                "SMA (Área músculo-esquelética): 42,1 cm²",
                "Densidade média muscular: 32 UH",
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
            muscle_density_hu_mean=28.4,
            height_m=1.82,
            smi_cm2_m2=11.48,
            locale="en_US",
        )

        self.assertEqual(title, "L3 Center Slice")
        self.assertEqual(
            lines,
            [
                "SMA (Skeletal muscle area): 38.0 cm²",
                "Mean muscle density: 28 HU",
                "NIfTI slice: 12",
                "Probable viewer slice: 42",
                "Patient height: 1.82 m",
                "SMI (Skeletal Muscle Index): 11.5 cm²/m²",
            ],
        )

    def test_l3_overlay_text_omits_density_when_unavailable(self):
        _title, lines = build_overlay_text(
            slice_idx=12,
            probable_viewer_slice_index_one_based=42,
            muscle_area_cm2=38.04,
            muscle_density_hu_mean=None,
            height_m=1.82,
            smi_cm2_m2=11.48,
            locale="pt_BR",
        )

        self.assertEqual(
            lines,
            [
                "SMA (Área músculo-esquelética): 38,0 cm²",
                "Corte NIfTI: 12",
                "Provável corte no visualizador: 42",
                "Altura do paciente: 1,82 m",
                "SMI (Índice de Massa Muscular Esquelética): 11,5 cm²/m²",
            ],
        )

    def test_parenchymal_overlay_text_in_pt_br_uses_dot_grouping_and_uh(self):
        with patch("heimdallr.metrics.jobs._parenchymal_overlay_text.settings.ARTIFACTS_LOCALE", "pt_BR"):
            self.assertEqual(resolve_parenchymal_locale({}), "pt_BR")

        lines = build_parenchymal_overlay_text(
            organ_measurements={
                "liver": {"analysis_status": "complete", "observed_volume_cm3": 1355.0, "hu_mean": 56.0},
                "spleen": {"analysis_status": "complete", "observed_volume_cm3": 384.0, "hu_mean": 48.0},
                "pancreas": {"analysis_status": "complete", "observed_volume_cm3": 98.0, "hu_mean": 28.0},
                "kidney_right": {"analysis_status": "complete", "observed_volume_cm3": 154.0, "hu_mean": 27.0},
                "kidney_left": {"analysis_status": "complete", "observed_volume_cm3": 152.0, "hu_mean": 27.0},
            },
            locale="pt_BR",
        )

        self.assertEqual(
            lines,
            [
                "Órgãos parenquimatosos:",
                "Fígado: 1.355 cm³ | 56 UH",
                "Baço: 384 cm³ | 48 UH",
                "Pâncreas: 98 cm³ | 28 UH",
                "Rim direito: 154 cm³ | 27 UH",
                "Rim esquerdo: 152 cm³ | 27 UH",
            ],
        )
        self.assertEqual(
            parenchymal_series_description("pt_BR"),
            "Heimdallr Overlay de Órgãos Parenquimatosos 5 mm",
        )
        self.assertIn(
            "Reconstrução axial de 5 mm",
            parenchymal_derivation_description("pt_BR"),
        )

    def test_parenchymal_overlay_text_in_en_us_uses_comma_grouping_and_hu(self):
        with patch("heimdallr.metrics.jobs._parenchymal_overlay_text.settings.ARTIFACTS_LOCALE", "pt_BR"):
            self.assertEqual(resolve_parenchymal_locale({"locale": "en_US"}), "en_US")

        lines = build_parenchymal_overlay_text(
            organ_measurements={
                "liver": {"analysis_status": "complete", "observed_volume_cm3": 1355.0, "hu_mean": 56.0},
                "spleen": {"analysis_status": "complete", "observed_volume_cm3": 384.0, "hu_mean": 48.0},
                "pancreas": {"analysis_status": "complete", "observed_volume_cm3": 98.0, "hu_mean": 28.0},
                "kidney_right": {"analysis_status": "complete", "observed_volume_cm3": 154.0, "hu_mean": 27.0},
                "kidney_left": {"analysis_status": "complete", "observed_volume_cm3": 152.0, "hu_mean": 27.0},
            },
            locale="en_US",
        )

        self.assertEqual(
            lines,
            [
                "Parenchymal organs:",
                "Liver: 1,355 cm³ | 56 HU",
                "Spleen: 384 cm³ | 48 HU",
                "Pancreas: 98 cm³ | 28 HU",
                "Right kidney: 154 cm³ | 27 HU",
                "Left kidney: 152 cm³ | 27 HU",
            ],
        )
        self.assertEqual(
            parenchymal_series_description("en_US"),
            "Heimdallr Parenchymal Organ Overlay 5 mm",
        )

    def test_parenchymal_overlay_text_supports_volume_only_lines(self):
        lines = build_parenchymal_overlay_text(
            organ_measurements={
                "liver": {"analysis_status": "complete", "observed_volume_cm3": 1355.0, "hu_mean": None},
            },
            locale="pt_BR",
        )

        self.assertEqual(
            lines,
            [
                "Órgãos parenquimatosos:",
                "Fígado: 1.355 cm³",
            ],
        )

    def test_brain_volumetry_overlay_text_in_en_us(self):
        with patch("heimdallr.metrics.jobs._brain_volumetry_overlay_text.settings.ARTIFACTS_LOCALE", "pt_BR"):
            self.assertEqual(resolve_brain_locale({"locale": "en_US"}), "en_US")

        lines = build_brain_overlay_text(
            measurement={"analysis_status": "complete", "observed_volume_cm3": 1325.0},
            locale="en_US",
        )

        self.assertEqual(lines, ["Brain volumetry:", "Brain: 1,325 cm³"])
        self.assertEqual(brain_series_description("en_US"), "Heimdallr Brain Volumetry Overlay 5 mm")
        self.assertIn("5 mm axial reconstruction", brain_derivation_description("en_US"))

    def test_brain_volumetry_overlay_text_in_pt_br(self):
        with patch("heimdallr.metrics.jobs._brain_volumetry_overlay_text.settings.ARTIFACTS_LOCALE", "pt_BR"):
            self.assertEqual(resolve_brain_locale({}), "pt_BR")

        lines = build_brain_overlay_text(
            measurement={"analysis_status": "complete", "observed_volume_cm3": 1325.0},
            locale="pt_BR",
        )

        self.assertEqual(lines, ["Volumetria cerebral:", "Encéfalo: 1.325 cm³"])


if __name__ == "__main__":
    unittest.main()
