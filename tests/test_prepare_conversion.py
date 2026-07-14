import subprocess
import tempfile
import threading
import time
import unittest
import concurrent.futures
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pydicom.uid import ExplicitVRLittleEndian, JPEGLosslessSV1

from heimdallr.prepare import worker


class TestPrepareConversion(unittest.TestCase):
    def test_convert_series_isolates_duplicate_numbers_and_basenames(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first_source = root / "source_a" / "image.dcm"
            second_source = root / "source_b" / "image.dcm"
            first_source.parent.mkdir()
            second_source.parent.mkdir()
            first_source.write_bytes(b"first")
            second_source.write_bytes(b"second")
            observed_inputs = []

            def fake_dcm2niix(input_dir, output_dir):
                observed_inputs.append(Path(input_dir))
                (Path(output_dir) / "converted.nii.gz").write_bytes(b"nifti")
                return subprocess.CompletedProcess(["dcm2niix"], 0, None, "")

            with (
                patch.object(worker, "_detect_series_transfer_syntax_uid", return_value=str(ExplicitVRLittleEndian)),
                patch.object(worker, "_run_dcm2niix_conversion", side_effect=fake_dcm2niix),
                concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor,
            ):
                futures = [
                    executor.submit(
                        worker.convert_series,
                        "4",
                        [first_source, second_source],
                        root / f"output_{index}.nii.gz",
                        root,
                        modality="CT",
                    )
                    for index in range(2)
                ]
                results = [future.result() for future in futures]

            self.assertEqual(len(observed_inputs), 2)
            self.assertNotEqual(observed_inputs[0], observed_inputs[1])
            self.assertTrue(all(len(list(path.iterdir())) == 2 for path in observed_inputs))
            self.assertTrue(all(result["method"] == "dcm2niix" for result in results))

    def test_conversion_pool_keeps_running_while_phase_capacity_is_busy(self):
        converted_third = threading.Event()
        phase_started = threading.Event()
        release_phase = threading.Event()
        phase_calls = 0
        phase_lock = threading.Lock()

        def fake_convert(uid, series_data, case_output_dir, temp_dir):
            if uid == "3":
                converted_third.set()
            return {"uid": uid, "series_number": uid}

        def fake_phase(candidate, *, submitted_at=None):
            nonlocal phase_calls
            with phase_lock:
                phase_calls += 1
                call_number = phase_calls
            if call_number == 1:
                phase_started.set()
                self.assertTrue(release_phase.wait(1))
            return candidate

        result_holder = []

        def run_batch():
            result_holder.extend(
                worker.process_ct_series_batch(
                    {str(index): {} for index in range(1, 4)},
                    Path("unused"),
                    Path("unused"),
                )
            )

        with (
            patch.object(worker.settings, "PREPARE_SERIES_CONVERSION_WORKERS", 2),
            patch.object(worker.settings, "TOTALSEG_GET_PHASE_MAX_PARALLEL", 1),
            patch.object(worker, "_convert_ct_series", side_effect=fake_convert),
            patch.object(worker, "_detect_ct_series_phase", side_effect=fake_phase),
        ):
            thread = threading.Thread(target=run_batch)
            thread.start()
            self.assertTrue(phase_started.wait(1))
            self.assertTrue(converted_third.wait(1))
            release_phase.set()
            thread.join(2)

        self.assertFalse(thread.is_alive())
        self.assertEqual([item["uid"] for item in result_holder], ["1", "2", "3"])

    def test_mixed_batch_does_not_run_phase_detection_for_mr(self):
        candidates = [
            {
                "uid": "ct",
                "modality": "CT",
                "path": Path("ct.nii.gz"),
                "phase_json_path": Path("ct.phase.json"),
                "_series_started": time.perf_counter(),
            },
            {"uid": "mr", "modality": "MR", "_series_started": time.perf_counter()},
        ]
        phase_calls = []

        def fake_phase(_input, _output, *, timing=None):
            phase_calls.append(True)
            return {"phase": "portal_venous"}

        with patch.object(worker, "run_totalseg_phase", side_effect=fake_phase):
            worker._detect_ct_series_phase(candidates[0], submitted_at=time.perf_counter())
            worker._detect_ct_series_phase(candidates[1], submitted_at=time.perf_counter())

        self.assertEqual(len(phase_calls), 1)

    def test_qc_conversion_skips_nonprimary_derived_series(self):
        derived = {"files": ["a", "b"], "ImageType": ["DERIVED", "PRIMARY", "MPR"]}
        original = {"files": ["a", "b"], "ImageType": ["ORIGINAL", "PRIMARY", "AXIAL"]}
        self.assertFalse(
            worker.should_convert_qc_series(
                "derived",
                derived,
                primary_series_uids={"primary"},
                exam_modality="CT",
            )
        )
        self.assertTrue(
            worker.should_convert_qc_series(
                "primary",
                derived,
                primary_series_uids={"primary"},
                exam_modality="CT",
            )
        )
        self.assertTrue(
            worker.should_convert_qc_series(
                "original",
                original,
                primary_series_uids={"primary"},
                exam_modality="CT",
            )
        )

    def test_convert_series_logs_dcm2niix_failure_details(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)
            source = temp_dir / "input.dcm"
            source.write_bytes(b"not-a-real-dicom")
            output = temp_dir / "output.nii.gz"

            with patch.object(worker, "_detect_series_transfer_syntax_uid", return_value=str(ExplicitVRLittleEndian)):
                with patch.object(
                    worker,
                    "_run_dcm2niix_conversion",
                    return_value=subprocess.CompletedProcess(
                        args=["dcm2niix"],
                        returncode=1,
                        stdout=None,
                        stderr="decoder missing\nbad offset",
                    ),
                ):
                    with patch("builtins.print") as print_mock:
                        result = worker.convert_series("4", [source], output, temp_dir, modality="CT")

            self.assertIsNone(result)
            rendered = " ".join(
                " ".join(str(arg) for arg in call.args)
                for call in print_mock.call_args_list
            )
            self.assertIn("dcm2niix failed for series 4", rendered)
            self.assertIn("decoder missing", rendered)
            self.assertFalse(output.exists())

    def test_convert_series_falls_back_to_dicom2nifti_for_ct_jpeg_lossless(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)
            source = temp_dir / "input.dcm"
            source.write_bytes(b"not-a-real-dicom")
            output = temp_dir / "output.nii.gz"

            def fake_convert(input_dir, output_path, reorient_nifti=False):
                Path(output_path).write_bytes(b"nifti")

            fake_module = SimpleNamespace(dicom_series_to_nifti=fake_convert)

            with patch.object(worker, "_detect_series_transfer_syntax_uid", return_value=str(JPEGLosslessSV1)):
                with patch.object(
                    worker,
                    "_run_dcm2niix_conversion",
                    return_value=subprocess.CompletedProcess(
                        args=["dcm2niix"],
                        returncode=1,
                        stdout=None,
                        stderr="Error: JPEG signature 0xFFD8FF not found",
                    ),
                ):
                    with patch.object(worker.importlib, "import_module", return_value=fake_module) as import_mock:
                        result = worker.convert_series("4", [source], output, temp_dir, modality="CT")

            self.assertIsNotNone(result)
            self.assertEqual(result["method"], "dicom2nifti")
            self.assertEqual(result["transfer_syntax_uid"], str(JPEGLosslessSV1))
            self.assertTrue(output.exists())
            import_mock.assert_called_once_with("dicom2nifti")

    def test_totalseg_phase_uses_configured_device_and_timeout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)
            source = temp_dir / "series.nii.gz"
            output = temp_dir / "series.phase.json"
            source.write_bytes(b"nifti")
            output.write_text('{"phase": "native"}')
            fake_process = SimpleNamespace(
                pid=12345,
                returncode=0,
                communicate=lambda timeout: (None, ""),
            )

            with patch.object(worker.settings, "TOTALSEG_GET_PHASE_DEVICE", "mps"):
                with patch.object(worker.settings, "TOTALSEG_GET_PHASE_TIMEOUT_SECONDS", 17):
                    with patch.object(worker.settings, "TOTALSEG_GET_PHASE_THREAD_LIMIT", 1):
                        with patch.object(
                            worker.subprocess,
                            "Popen",
                            return_value=fake_process,
                        ) as popen_mock:
                            with patch.object(worker, "_terminate_phase_process_group") as terminate_mock:
                                result = worker.run_totalseg_phase(source, output)

            self.assertEqual(result["phase"], "native")
            cmd = popen_mock.call_args.args[0]
            _, kwargs = popen_mock.call_args
            self.assertIn("--device", cmd)
            self.assertIn("mps", cmd)
            self.assertTrue(kwargs["start_new_session"])
            self.assertEqual(kwargs["env"]["OMP_NUM_THREADS"], "1")
            self.assertEqual(kwargs["env"]["VECLIB_MAXIMUM_THREADS"], "1")
            terminate_mock.assert_called_with(12345)

    def test_totalseg_phase_limits_threads_for_cpu_device(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)
            source = temp_dir / "series.nii.gz"
            output = temp_dir / "series.phase.json"
            source.write_bytes(b"nifti")
            output.write_text('{"phase": "native"}')
            fake_process = SimpleNamespace(
                pid=12345,
                returncode=0,
                communicate=lambda timeout: (None, ""),
            )

            with patch.object(worker.settings, "TOTALSEG_GET_PHASE_DEVICE", "cpu"):
                with patch.object(worker.settings, "TOTALSEG_GET_PHASE_TIMEOUT_SECONDS", 17):
                    with patch.object(worker.settings, "TOTALSEG_GET_PHASE_THREAD_LIMIT", 1):
                        with patch.object(
                            worker.subprocess,
                            "Popen",
                            return_value=fake_process,
                        ) as popen_mock:
                            with patch.object(worker, "_terminate_phase_process_group"):
                                result = worker.run_totalseg_phase(source, output)

            self.assertEqual(result["phase"], "native")
            cmd = popen_mock.call_args.args[0]
            _, kwargs = popen_mock.call_args
            self.assertIn("--device", cmd)
            self.assertIn("cpu", cmd)
            self.assertEqual(kwargs["env"]["OMP_NUM_THREADS"], "1")
            self.assertEqual(kwargs["env"]["MKL_NUM_THREADS"], "1")
            self.assertEqual(kwargs["env"]["OPENBLAS_NUM_THREADS"], "1")
            self.assertEqual(kwargs["env"]["VECLIB_MAXIMUM_THREADS"], "1")
            self.assertEqual(kwargs["env"]["NUMEXPR_NUM_THREADS"], "1")

    def test_totalseg_phase_timeout_returns_unknown_without_blocking_prepare(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)
            source = temp_dir / "series.nii.gz"
            output = temp_dir / "series.phase.json"
            source.write_bytes(b"nifti")
            fake_process = SimpleNamespace(
                pid=12345,
                communicate=lambda timeout: (_ for _ in ()).throw(
                    subprocess.TimeoutExpired(["totalseg_get_phase"], timeout)
                ),
                wait=lambda timeout: None,
            )

            with patch.object(worker.settings, "TOTALSEG_GET_PHASE_TIMEOUT_SECONDS", 1):
                with patch.object(
                    worker.subprocess,
                    "Popen",
                    return_value=fake_process,
                ):
                    with patch.object(worker, "_terminate_phase_process_group") as terminate_mock:
                        with patch("builtins.print") as print_mock:
                            result = worker.run_totalseg_phase(source, output)

            self.assertIsNone(result)
            terminate_mock.assert_called_with(12345)
            rendered = " ".join(
                " ".join(str(arg) for arg in call.args)
                for call in print_mock.call_args_list
            )
            self.assertIn("Phase detection timed out", rendered)

    def test_totalseg_phase_reports_capacity_wait_separately(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "series.nii.gz"
            output = root / "series.phase.json"
            source.write_bytes(b"nifti")
            output.write_text('{"phase": "native"}')
            fake_process = SimpleNamespace(
                pid=12345,
                returncode=0,
                communicate=lambda timeout: (None, ""),
            )
            semaphore_entered = threading.Event()
            release_semaphore = threading.Event()
            test_case = self

            class BlockingSemaphore:
                def __enter__(self):
                    semaphore_entered.set()
                    test_case.assertTrue(release_semaphore.wait(1))
                    return self

                def __exit__(self, *_args):
                    return False

            semaphore = BlockingSemaphore()
            timing = {}
            result_holder = []

            def run_phase():
                result_holder.append(worker.run_totalseg_phase(source, output, timing=timing))

            with (
                patch.object(worker, "_TOTALSEG_PHASE_SEMAPHORE", semaphore),
                patch.object(worker.subprocess, "Popen", return_value=fake_process),
                patch.object(worker, "_terminate_phase_process_group"),
            ):
                thread = threading.Thread(target=run_phase)
                thread.start()
                self.assertTrue(semaphore_entered.wait(1))
                time.sleep(0.03)
                release_semaphore.set()
                thread.join(1)

            self.assertFalse(thread.is_alive())
            self.assertEqual(result_holder[0]["phase"], "native")
            self.assertGreaterEqual(timing["capacity_wait_seconds"], 0.02)
            self.assertGreaterEqual(timing["inference_seconds"], 0.0)


if __name__ == "__main__":
    unittest.main()
