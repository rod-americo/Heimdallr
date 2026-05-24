#!/usr/bin/env python3
"""Run OCR over DICOM pixel data to screen for burned-in text.

This is a local validation helper for smoke datasets. It does not anonymize
pixels and it does not prove that a study is publishable. By default OCR text is
not written to stdout or the JSON report; use ``--include-text`` only for local
investigation in ignored runtime paths.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pydicom
from PIL import Image
from pydicom.errors import InvalidDicomError


TEXT_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")


@dataclass(frozen=True)
class DicomPayload:
    member: str
    raw: bytes


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def _safe_zip_members(zip_handle: zipfile.ZipFile) -> Iterable[zipfile.ZipInfo]:
    for info in zip_handle.infolist():
        if info.is_dir():
            continue
        name = info.filename
        if name.startswith("/") or ".." in Path(name).parts:
            raise ValueError(f"Unsafe ZIP member path: {name}")
        yield info


def iter_dicom_payloads(source: Path) -> Iterable[DicomPayload]:
    if source.is_dir():
        for path in sorted(item for item in source.rglob("*") if item.is_file()):
            yield DicomPayload(str(path.relative_to(source)), path.read_bytes())
        return
    if zipfile.is_zipfile(source):
        with zipfile.ZipFile(source, "r") as zip_handle:
            for info in _safe_zip_members(zip_handle):
                yield DicomPayload(info.filename, zip_handle.read(info))
        return
    yield DicomPayload(source.name, source.read_bytes())


def normalize_text(text: str) -> str:
    lines = []
    for line in text.splitlines():
        stripped = " ".join(line.strip().split())
        if stripped:
            lines.append(stripped)
    return "\n".join(lines)


def has_ocr_text(text: str, *, min_text_chars: int) -> bool:
    return sum(1 for char in text if char in TEXT_CHARS) >= int(min_text_chars)


def _first_numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _windowed_uint8(frame: np.ndarray, ds: pydicom.Dataset) -> np.ndarray:
    arr = np.asarray(frame, dtype=np.float32)
    slope = _first_numeric(getattr(ds, "RescaleSlope", None))
    intercept = _first_numeric(getattr(ds, "RescaleIntercept", None))
    if slope is not None:
        arr = arr * slope
    if intercept is not None:
        arr = arr + intercept

    center = _first_numeric(getattr(ds, "WindowCenter", None))
    width = _first_numeric(getattr(ds, "WindowWidth", None))
    if center is not None and width is not None and width > 0:
        low = center - width / 2.0
        high = center + width / 2.0
    else:
        low, high = np.percentile(arr, [0.5, 99.5])
        if float(high) <= float(low):
            low = float(np.min(arr))
            high = float(np.max(arr))
    if float(high) <= float(low):
        return np.zeros(arr.shape, dtype=np.uint8)
    arr = np.clip((arr - low) / (high - low), 0.0, 1.0)
    if str(getattr(ds, "PhotometricInterpretation", "")).upper() == "MONOCHROME1":
        arr = 1.0 - arr
    return np.asarray(arr * 255.0, dtype=np.uint8)


def frames_for_ocr(ds: pydicom.Dataset, *, max_frames_per_instance: int) -> list[np.ndarray]:
    pixels = ds.pixel_array
    if pixels.ndim == 2:
        return [_windowed_uint8(pixels, ds)]
    if pixels.ndim == 3:
        frame_count = pixels.shape[0]
        max_frames = max(1, int(max_frames_per_instance))
        if frame_count <= max_frames:
            indices = list(range(frame_count))
        else:
            indices = sorted(
                set(np.linspace(0, frame_count - 1, num=max_frames, dtype=int).tolist())
            )
        return [_windowed_uint8(pixels[index], ds) for index in indices]
    return []


def run_tesseract(
    image: np.ndarray,
    *,
    tesseract_bin: str,
    psm: int,
    timeout_seconds: float,
) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        image_path = Path(tmpdir) / "frame.png"
        Image.fromarray(image).save(image_path)
        result = subprocess.run(
            [
                tesseract_bin,
                str(image_path),
                "stdout",
                "--psm",
                str(int(psm)),
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=float(timeout_seconds),
        )
    if result.returncode not in {0, 1}:
        raise RuntimeError(result.stderr.strip() or f"tesseract exited {result.returncode}")
    return normalize_text(result.stdout)


def verify_source(
    source: Path,
    *,
    tesseract_bin: str,
    max_instances: int | None,
    max_frames_per_instance: int,
    min_text_chars: int,
    psm: int,
    timeout_seconds: float,
    include_text: bool,
) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    dicom_instances = 0
    skipped_non_dicom = 0
    skipped_no_pixels = 0
    ocr_frames = 0
    decode_errors: list[dict[str, str]] = []

    for payload in iter_dicom_payloads(source):
        if max_instances is not None and dicom_instances >= max_instances:
            break
        try:
            ds = pydicom.dcmread(io.BytesIO(payload.raw), force=True)
        except InvalidDicomError:
            skipped_non_dicom += 1
            continue
        except Exception as exc:
            skipped_non_dicom += 1
            decode_errors.append({"member": payload.member, "error": str(exc)})
            continue
        if "PixelData" not in ds:
            skipped_no_pixels += 1
            continue
        dicom_instances += 1
        try:
            frames = frames_for_ocr(ds, max_frames_per_instance=max_frames_per_instance)
        except Exception as exc:
            decode_errors.append({"member": payload.member, "error": str(exc)})
            continue
        for frame_index, frame in enumerate(frames):
            ocr_frames += 1
            text = run_tesseract(
                frame,
                tesseract_bin=tesseract_bin,
                psm=psm,
                timeout_seconds=timeout_seconds,
            )
            if not has_ocr_text(text, min_text_chars=min_text_chars):
                continue
            finding: dict[str, Any] = {
                "member": payload.member,
                "frame_index": frame_index,
                "text_length": len(text),
                "text_sha256": _hash_text(text),
            }
            if include_text:
                finding["text"] = text
            findings.append(finding)

    return {
        "source": str(source),
        "tesseract_bin": tesseract_bin,
        "dicom_instances_scanned": dicom_instances,
        "ocr_frames_scanned": ocr_frames,
        "skipped_non_dicom_members": skipped_non_dicom,
        "skipped_instances_without_pixels": skipped_no_pixels,
        "decode_errors": decode_errors,
        "finding_count": len(findings),
        "burned_in_text_suspected": bool(findings),
        "findings": findings,
        "report_text_included": include_text,
        "limitations": [
            "OCR screening can miss small, rotated, low-contrast, or stylized burned-in text.",
            "A clean OCR report does not replace manual pixel review for publishable datasets.",
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="DICOM file, directory, or ZIP to screen.")
    parser.add_argument(
        "--report",
        type=Path,
        help="Optional JSON report path. Use ignored runtime paths for local datasets.",
    )
    parser.add_argument(
        "--tesseract-bin",
        default=shutil.which("tesseract") or "tesseract",
        help="Path to tesseract binary.",
    )
    parser.add_argument("--max-instances", type=int, help="Limit number of DICOM instances.")
    parser.add_argument(
        "--max-frames-per-instance",
        type=int,
        default=1,
        help="Maximum frames sampled from each multi-frame instance.",
    )
    parser.add_argument(
        "--min-text-chars",
        type=int,
        default=4,
        help="Minimum OCR alphanumeric character count to flag a frame.",
    )
    parser.add_argument("--psm", type=int, default=6, help="Tesseract page segmentation mode.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument(
        "--include-text",
        action="store_true",
        help="Include OCR text in the JSON report. Use only in ignored local paths.",
    )
    parser.add_argument(
        "--allow-findings",
        action="store_true",
        help="Exit 0 even when OCR text is detected.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tesseract_path = Path(args.tesseract_bin)
    resolved_tesseract = shutil.which(args.tesseract_bin) if not tesseract_path.exists() else str(tesseract_path)
    if not resolved_tesseract:
        print(f"ERROR: tesseract binary not found: {args.tesseract_bin}")
        print("Install tesseract or pass --tesseract-bin /path/to/tesseract.")
        return 2

    report = verify_source(
        args.source,
        tesseract_bin=resolved_tesseract,
        max_instances=args.max_instances,
        max_frames_per_instance=args.max_frames_per_instance,
        min_text_chars=args.min_text_chars,
        psm=args.psm,
        timeout_seconds=args.timeout_seconds,
        include_text=args.include_text,
    )
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    summary = {
        "source": report["source"],
        "dicom_instances_scanned": report["dicom_instances_scanned"],
        "ocr_frames_scanned": report["ocr_frames_scanned"],
        "finding_count": report["finding_count"],
        "burned_in_text_suspected": report["burned_in_text_suspected"],
        "report": str(args.report) if args.report else None,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    if report["burned_in_text_suspected"] and not args.allow_findings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
