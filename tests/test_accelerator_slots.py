import os
import select
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from heimdallr.shared import accelerator_slots


class TestAcceleratorSlots(unittest.TestCase):
    def test_disabled_admission_is_a_noop(self):
        with patch.object(accelerator_slots.settings, "ACCELERATOR_TASK_SLOTS", 0):
            with accelerator_slots.accelerator_slot(enabled=True) as slot:
                self.assertIsNone(slot)

    def test_slots_are_shared_between_concurrent_callers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            acquired_third = threading.Event()
            release_third = threading.Event()

            def claim_third():
                with accelerator_slots.accelerator_slot(enabled=True, poll_seconds=0.01):
                    acquired_third.set()
                    release_third.wait(1)

            with (
                patch.object(accelerator_slots.settings, "ACCELERATOR_TASK_SLOTS", 2),
                patch.object(accelerator_slots.settings, "RUNTIME_DIR", Path(tmpdir)),
                accelerator_slots.accelerator_slot(enabled=True) as first,
                accelerator_slots.accelerator_slot(enabled=True) as second,
            ):
                self.assertNotEqual(first, second)
                thread = threading.Thread(target=claim_third)
                thread.start()
                self.assertFalse(acquired_third.wait(0.05))

            self.assertTrue(acquired_third.wait(1))
            release_third.set()
            thread.join(1)
            self.assertFalse(thread.is_alive())

    def test_slots_are_shared_between_processes(self):
        child_code = """
import time
from heimdallr.shared import accelerator_slots
from heimdallr.shared import settings
from unittest.mock import patch

print('ready', flush=True)
with patch.object(settings, 'ACCELERATOR_TASK_SLOTS', 1), patch.object(settings, 'RUNTIME_DIR', __import__('pathlib').Path(__import__('os').environ['TEST_SLOT_ROOT'])):
    with accelerator_slots.accelerator_slot(enabled=True, poll_seconds=0.01):
        print('acquired', flush=True)
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with (
                patch.object(accelerator_slots.settings, "ACCELERATOR_TASK_SLOTS", 1),
                patch.object(accelerator_slots.settings, "RUNTIME_DIR", root),
                accelerator_slots.accelerator_slot(enabled=True),
            ):
                env = os.environ.copy()
                env["TEST_SLOT_ROOT"] = str(root)
                process = subprocess.Popen(
                    [sys.executable, "-c", child_code],
                    cwd=Path(__file__).resolve().parents[1],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                assert process.stdout is not None
                self.assertEqual(process.stdout.readline().strip(), "ready")
                readable, _, _ = select.select([process.stdout], [], [], 0.1)
                self.assertFalse(readable)
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertIn("acquired", stdout)


if __name__ == "__main__":
    unittest.main()
