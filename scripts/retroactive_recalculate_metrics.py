#!/usr/bin/env python3
# Copyright (c) 2026 Rodrigo Americo
import argparse
import concurrent.futures
import json
import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from heimdallr.shared import settings as config
from core.metrics import calculate_all_metrics


def iter_case_folders(output_dir, selected_cases=None):
    case_dirs = sorted(d for d in output_dir.iterdir() if d.is_dir())
    if not selected_cases:
        return case_dirs
    selected = set(selected_cases)
    return [case_dir for case_dir in case_dirs if case_dir.name in selected]


def find_nifti_for_case(case_folder, nii_archive):
    case_id = case_folder.name

    nifti_path = nii_archive / f"{case_id}.nii.gz"
    if nifti_path.exists():
        return nifti_path

    id_json = case_folder / "id.json"
    if id_json.exists():
        try:
            with open(id_json, "r") as f:
                meta = json.load(f)

            clinical_name = meta.get("ClinicalName")
            if clinical_name and clinical_name != "Unknown":
                candidate = nii_archive / f"{clinical_name}.nii.gz"
                if candidate.exists():
                    return candidate
        except Exception:
            pass

    candidates = list(nii_archive.glob(f"*{case_id}*.nii.gz"))
    if candidates:
        return candidates[0]

    return None


def load_study_uid(case_folder):
    id_json = case_folder / "id.json"
    if not id_json.exists():
        return None

    try:
        with open(id_json, "r") as f:
            meta = json.load(f)
        return meta.get("StudyInstanceUID")
    except Exception:
        return None


def process_case(case_folder, nii_archive, prune_incomplete_bleed=False, generate_overlays=True):
    case_id = case_folder.name
    nifti_path = find_nifti_for_case(case_folder, nii_archive)
    if nifti_path is None:
        return {
            "ok": False,
            "case_id": case_id,
            "error": f"NIfTI not found for {case_id}",
            "study_uid": load_study_uid(case_folder),
            "results": None,
        }

    results = calculate_all_metrics(
        case_id,
        nifti_path,
        case_folder,
        generate_overlays=generate_overlays
    )

    with open(case_folder / "resultados.json", "w") as f:
        json.dump(results, f, indent=2)

    return {
        "ok": True,
        "case_id": case_id,
        "error": None,
        "study_uid": load_study_uid(case_folder),
        "results": results,
    }


def build_parser():
    parser = argparse.ArgumentParser(
        description="Retroactively recalculate Heimdallr metrics from existing segmentations."
    )
    parser.add_argument(
        "--case",
        action="append",
        dest="cases",
        help="Specific case_id to process. Repeat for multiple cases."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N matching cases."
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip updating CalculationResults in the database."
    )
    parser.add_argument(
        "--prune-incomplete-bleed",
        action="store_true",
        help="Deprecated. Legacy bleed outputs are now preserved."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker threads to use."
    )
    parser.add_argument(
        "--skip-overlays",
        action="store_true",
        help="Do not regenerate PNG overlays during retroactive recalculation."
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    output_dir = Path(config.OUTPUT_DIR)
    nii_archive = Path(config.BASE_DIR / "nii")
    db_path = Path(config.DB_PATH)

    cases = iter_case_folders(output_dir, selected_cases=args.cases)
    if args.limit is not None:
        cases = cases[:args.limit]

    total_cases = len(cases)
    print(f"Starting retroactive metrics recalculation for {total_cases} case(s)...")

    db_conn = None
    if not args.no_db and db_path.exists():
        db_conn = sqlite3.connect(str(db_path))

    processed = 0
    failed = 0

    stone_cases = []

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            future_map = {
                executor.submit(
                    process_case,
                    case_folder,
                    nii_archive,
                    args.prune_incomplete_bleed,
                    not args.skip_overlays
                ): case_folder
                for case_folder in cases
            }

            for idx, future in enumerate(concurrent.futures.as_completed(future_map), start=1):
                case_folder = future_map[future]
                print(f"[{idx}/{total_cases}] Finished {case_folder.name}...", end="\r")
                payload = future.result()

                if payload["ok"]:
                    processed += 1
                    results = payload["results"] or {}
                    if (results.get("renal_stone_count") or 0) > 0:
                        stone_cases.append(payload["case_id"])

                    if db_conn is not None and payload["study_uid"]:
                        cursor = db_conn.cursor()
                        cursor.execute(
                            "UPDATE dicom_metadata SET CalculationResults = ? WHERE StudyInstanceUID = ?",
                            (json.dumps(results), payload["study_uid"])
                        )
                        db_conn.commit()
                else:
                    failed += 1
                    print(f"\n[Error] {payload['error']}")
    finally:
        if db_conn is not None:
            db_conn.close()

    print(f"\nFinished. Processed: {processed}. Failed: {failed}.")
    if stone_cases:
        print("Stone cases:")
        for case_id in sorted(stone_cases):
            print(case_id)


if __name__ == "__main__":
    main()
