import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heimdallr.metrics.jobs.tests._liver_segments_overlay_text import build_overlay_text  # noqa: E402


class TestLiverSegmentsOverlayText(unittest.TestCase):
    def test_build_overlay_text_formats_segment_volumes_without_decimals(self):
        lines = build_overlay_text(
            segment_measurements={
                "liver_segment_1": {"volume_cm3": 89.0},
                "liver_segment_2": {"volume_cm3": 1355.0},
                "liver_segment_3": {"volume_cm3": None},
            },
            locale="pt_BR",
        )

        self.assertEqual(
            lines,
            [
                "Segmentos hepáticos:",
                "I: 89 cm³",
                "II: 1.355 cm³",
            ],
        )


if __name__ == "__main__":
    unittest.main()
