import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heimdallr.tui.i18n import format_refresh_seconds, no_data, queue_status_label, stage_label, stage_state_label  # noqa: E402


class TestTuiI18n(unittest.TestCase):
    def test_pt_br_labels_and_numeric_formatting(self):
        with patch("heimdallr.tui.i18n.settings.TUI_LOCALE", "pt_BR"):
            self.assertEqual(stage_label("processed"), "Processado")
            self.assertEqual(queue_status_label("claimed"), "em execução")
            self.assertEqual(stage_state_label("warning"), "ALERTA")
            self.assertEqual(format_refresh_seconds(2.5), "atualização 2,5s")
            self.assertEqual(no_data(), "n/d")

    def test_en_us_labels_and_numeric_formatting(self):
        with patch("heimdallr.tui.i18n.settings.TUI_LOCALE", "en_US"):
            self.assertEqual(stage_label("processed"), "Processed")
            self.assertEqual(queue_status_label("claimed"), "claimed")
            self.assertEqual(stage_state_label("warning"), "WARNING")
            self.assertEqual(format_refresh_seconds(2.5), "refresh 2.5s")
            self.assertEqual(no_data(), "n/a")


if __name__ == "__main__":
    unittest.main()
