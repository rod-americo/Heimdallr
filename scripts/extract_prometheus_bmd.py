#!/usr/bin/env python3
import sys
import json
import logging
import argparse
from pathlib import Path
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

# Import the validated BMD module
from metrics.metric_l1_bmd import compute_metric

def find_prometheus_bmd_cases(base_dir):
    """
    Find all cases in Prometheus dataset that have segmentations.
    Matches the structure: base_dir/case_id/series_*.nii.gz
    and base_dir/case_id/segmentations/total/series_id/vertebrae_L1.nii.gz
    """
    base_path = Path(base_dir)
    cases = []
    
    # Iterate through patient folders
    for case_folder in base_path.iterdir():
        if not case_folder.is_dir():
            continue
            
        # Find all NIfTI series in the case folder
        niftis = list(case_folder.glob("series_*.nii.gz"))
        
        for nifti_path in niftis:
            series_id = nifti_path.name.replace(".nii.gz", "")
            
            # Prometheus nested structure: segmentations/total/<series_id>/
            total_dir = case_folder / "segmentations" / "total" / series_id
            l1_mask = total_dir / "vertebrae_L1.nii.gz"
            
            if l1_mask.exists():
                cases.append({
                    "case_id": case_folder.name,
                    "series_id": series_id,
                    "nifti_path": nifti_path,
                    "case_output_folder": case_folder / "segmentations"
                })
                
    return cases

def process_single_bmd_case(case_info):
    try:
        results = compute_metric(
            case_output_folder=case_info["case_output_folder"],
            nifti_path=case_info["nifti_path"],
            case_id=case_info["case_id"],
            generate_overlays=False # No overlays for mass extraction to save time/space
        )
        results["series_id"] = case_info["series_id"]
        return results
    except Exception as e:
        return {
            "case_id": case_info["case_id"],
            "series_id": case_info["series_id"],
            "L1_bmd_analysis_status": "Error",
            "error_message": str(e)
        }

def main():
    parser = argparse.ArgumentParser(description="Focused Mass Extraction of L1 BMD for Prometheus Dataset")
    parser.add_argument("--input-dir", default="/storage/dataset/prometheus/images", help="Prometheus dataset base dir")
    parser.add_argument("--output-csv", default="prometheus_bmd_results.csv", help="Output path for consolidated results")
    parser.add_argument("--workers", type=int, default=8, help="Number of parallel workers")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of cases for testing")
    args = parser.parse_args()

    print(f"Scanning {args.input_dir} for L1 BMD cases...")
    all_cases = find_prometheus_bmd_cases(args.input_dir)
    
    if args.limit:
        all_cases = all_cases[:args.limit]
        
    print(f"Found {len(all_cases)} series with L1 masks. Starting extraction...")

    results_list = []
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_single_bmd_case, c): c for c in all_cases}
        
        for future in tqdm(as_completed(futures), total=len(all_cases), desc="Processing"):
            res = future.result()
            results_list.append(res)

    df = pd.DataFrame(results_list)
    # Ensure ID columns are first
    cols = ["case_id", "series_id", "L1_bmd_analysis_status"]
    other_cols = [c for c in df.columns if c not in cols]
    df = df[cols + other_cols]
    
    df.to_csv(args.output_csv, index=False)
    print(f"\nSuccess! Saved {len(results_list)} results to {args.output_csv}")

if __name__ == "__main__":
    main()
