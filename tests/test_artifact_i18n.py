import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heimdallr.metrics.jobs._l3_overlay_text import (  # noqa: E402
    build_overlay_text,
    derivation_description as l3_derivation_description,
    resolve_artifact_locale,
    series_description as l3_series_description,
)
from heimdallr.metrics.jobs._vat_sat_overlay_text import (  # noqa: E402
    build_overlay_text as build_vat_sat_overlay_text,
    derivation_description as vat_sat_derivation_description,
    resolve_artifact_locale as resolve_vat_sat_locale,
    series_description as vat_sat_series_description,
)
from heimdallr.metrics.jobs._parenchymal_overlay_text import (  # noqa: E402
    build_overlay_lines as build_parenchymal_overlay_lines,
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
from heimdallr.metrics.jobs._lung_nodules_overlay_text import (  # noqa: E402
    build_component_overlay_text as build_lung_nodule_overlay_text,
    series_description as lung_nodule_series_description,
)
from heimdallr.shared.i18n import translate  # noqa: E402


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
        self.assertEqual(l3_series_description("pt_BR"), "Heimdallr Overlay de Área Muscular em L3")
        self.assertIn(
            "densidade=31,60 UH",
            l3_derivation_description(
                "pt_BR",
                muscle_area_cm2=42.15,
                smi_cm2_m2=13.76,
                muscle_density_hu_mean=31.6,
            ),
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
        self.assertEqual(l3_series_description("en_US"), "Heimdallr L3 Muscle Area Overlay")

    def test_vat_sat_overlay_text_in_pt_br_uses_locale(self):
        with patch("heimdallr.metrics.jobs._vat_sat_overlay_text.settings.ARTIFACTS_LOCALE", "pt_BR"):
            self.assertEqual(resolve_vat_sat_locale({}), "pt_BR")

        title, panel_titles, lines, legend, sagittal_level = build_vat_sat_overlay_text(
            slice_idx=8,
            probable_viewer_slice_index_one_based=12,
            sat_area_cm2=184.5,
            vat_area_cm2=42.25,
            ratio=0.229,
            locale="pt_BR",
        )

        self.assertEqual(title, "Razão VAT/SAT - Corte central em L3")
        self.assertEqual(panel_titles, ("Medida axial", "Referência sagital"))
        self.assertEqual(
            lines,
            [
                "Nível: L3",
                "Corte NIfTI: 8",
                "Provável corte no visualizador: 12",
                "SAT: 184,5 cm²",
                "VAT: 42,2 cm²",
                "VAT/SAT: 0,2290",
            ],
        )
        self.assertIn("gordura subcutânea", legend)
        self.assertEqual(sagittal_level, "Nível axial z=8 | espessura 3 mm")
        self.assertEqual(vat_sat_series_description("pt_BR"), "Heimdallr Overlay de Razão VAT/SAT")
        self.assertIn(
            "razão=0,2290",
            vat_sat_derivation_description(
                "pt_BR",
                vat_area_cm2=42.25,
                sat_area_cm2=184.5,
                ratio=0.229,
            ),
        )

    def test_lung_nodule_component_overlay_text_in_pt_br_records_viewer_slice(self):
        title, lines = build_lung_nodule_overlay_text(
            component_id=7,
            component_index=2,
            component_count=4,
            slice_idx=33,
            probable_viewer_slice_index_one_based=120,
            voxel_count=42,
            volume_cm3=0.126,
            locale="pt_BR",
        )

        self.assertEqual(title, "Nódulo pulmonar 2/4")
        self.assertEqual(
            lines,
            [
                "ID do componente: 7",
                "Corte NIfTI: 33",
                "Provável corte no visualizador: 120",
                "Voxels: 42",
                "Volume: 0,126 cm³",
            ],
        )
        self.assertEqual(lung_nodule_series_description("pt_BR"), "Heimdallr Overlay de Nódulos Pulmonares")

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

    def test_parenchymal_overlay_marks_only_out_of_range_volume_values(self):
        lines = build_parenchymal_overlay_lines(
            organ_measurements={
                "liver": {
                    "analysis_status": "complete",
                    "observed_volume_cm3": 1801.0,
                    "hu_mean": 49.0,
                },
                "spleen": {
                    "analysis_status": "complete",
                    "observed_volume_cm3": 400.0,
                    "hu_mean": 50.0,
                },
                "kidney_right": {
                    "analysis_status": "complete",
                    "observed_volume_cm3": 99.0,
                    "hu_mean": 30.0,
                },
                "kidney_left": {
                    "analysis_status": "complete",
                    "observed_volume_cm3": 100.0,
                    "hu_mean": 30.0,
                },
            },
            locale="pt_BR",
        )

        alert_text = {
            line.text[line.alert_span[0] : line.alert_span[1]]
            for line in lines
            if line.alert_span is not None
        }
        self.assertEqual(alert_text, {"1.801", "99"})

    def test_parenchymal_overlay_separates_suspected_allograft_without_alert(self):
        lines = build_parenchymal_overlay_lines(
            organ_measurements={
                "kidney_right": {
                    "analysis_status": "complete",
                    "volume_cm3": 30.15,
                    "hu_mean": 18.0,
                },
                "kidney_left": {
                    "analysis_status": "complete",
                    "volume_cm3": 33.33,
                    "hu_mean": 17.0,
                },
            },
            locale="pt_BR",
            renal_anatomy_qc={
                "suspected_renal_allografts": [
                    {
                        "source_mask": "kidney_right",
                        "volume_cm3": 150.3,
                    }
                ]
            },
        )

        self.assertEqual(
            [line.text for line in lines],
            [
                "Órgãos parenquimatosos:",
                "Rim direito: 30 cm³ | 18 UH",
                "Rim esquerdo: 33 cm³ | 17 UH",
                "Provável enxerto renal direito: 150 cm³",
            ],
        )
        self.assertEqual(
            {
                line.text[line.alert_span[0] : line.alert_span[1]]
                for line in lines
                if line.alert_span is not None
            },
            {"30", "33"},
        )

    def test_parenchymal_overlay_adds_localized_steatosis_line_below_liver(self):
        lines = build_parenchymal_overlay_text(
            organ_measurements={
                "liver": {
                    "analysis_status": "complete",
                    "observed_volume_cm3": 1355.0,
                    "hu_mean": 45.0,
                },
                "spleen": {
                    "analysis_status": "complete",
                    "observed_volume_cm3": 384.0,
                    "hu_mean": 50.0,
                },
            },
            locale="pt_BR",
            hepatic_steatosis={"status": "estimated", "estimated_percent": 12},
        )

        self.assertEqual(
            lines[1:4],
            ["Fígado: 1.355 cm³ | 45 UH", "Esteatose: 12%", "Baço: 384 cm³ | 50 UH"],
        )

    def test_parenchymal_overlay_localizes_partial_and_insufficient_samples(self):
        organ_measurements = {
            "liver": {
                "analysis_status": "incomplete",
                "observed_volume_cm3": None,
                "hu_mean": 45.0,
            }
        }

        estimated = build_parenchymal_overlay_text(
            organ_measurements=organ_measurements,
            locale="pt_BR",
            hepatic_steatosis={
                "status": "estimated",
                "estimated_percent": 12,
                "partial_coverage": True,
            },
        )
        indeterminate = build_parenchymal_overlay_text(
            organ_measurements=organ_measurements,
            locale="pt_BR",
            hepatic_steatosis={"status": "spleen_sample_insufficient"},
        )
        insufficient = build_parenchymal_overlay_text(
            organ_measurements=organ_measurements,
            locale="pt_BR",
            hepatic_steatosis={"status": "liver_sample_insufficient"},
        )

        self.assertEqual(estimated[-1], "Esteatose: 12% (cobertura parcial)")
        self.assertEqual(
            indeterminate[-1],
            "Esteatose: indeterminada — amostra esplênica insuficiente",
        )
        self.assertEqual(insufficient[-1], "Esteatose: amostra hepática insuficiente")

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

    def test_head_complete_artifact_strings_are_translated(self):
        self.assertEqual(
            translate("head.volume_table.title", locale="en_US"),
            "Brain Structure Volumes",
        )
        self.assertEqual(
            translate("head.volume_table.title", locale="pt_BR"),
            "Volumes de Estruturas Cerebrais",
        )
        self.assertEqual(
            translate("head.structures.brainstem", locale="en_US"),
            "Brainstem",
        )
        self.assertEqual(
            translate("head.structures.brainstem", locale="pt_BR"),
            "Tronco encefálico",
        )


if __name__ == "__main__":
    unittest.main()
