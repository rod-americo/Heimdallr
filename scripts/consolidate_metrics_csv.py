#!/usr/bin/env python3
import json
import csv
from pathlib import Path
import argparse

def main():
    parser = argparse.ArgumentParser(description="Consolidate metrics.json files into a single CSV.")
    parser.add_argument("--images-dir", default="/storage/dataset/prometheus/images", type=str)
    parser.add_argument("--output", default="output/bmd_comparison/pre_test_results.csv", type=str)
    args = parser.parse_args()

    images_dir = Path(args.images_dir)
    output_file = Path(args.output)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    all_data = []
    
    # Percorrer as pastas de casos
    for case_dir in sorted(images_dir.iterdir()):
        if not case_dir.is_dir():
            continue
        
        # Procurar metrics.json em cada série (pasta segmentations/series_*)
        seg_dir = case_dir / "segmentations"
        if not seg_dir.exists():
            continue
            
        for series_dir in seg_dir.glob("series_*"):
            metrics_file = series_dir / "metrics.json"
            if metrics_file.exists():
                try:
                    with open(metrics_file, "r") as f:
                        data = json.load(f)
                        data["case_id"] = case_dir.name
                        data["series_id"] = series_dir.name.replace("series_", "")
                        all_data.append(data)
                except Exception as e:
                    print(f"Error reading {metrics_file}: {e}")

    if not all_data:
        print("No metrics found.")
        return

    # Pegar todos os campos possíveis para o header do CSV
    fieldnames = set()
    for row in all_data:
        fieldnames.update(row.keys())
    
    # Ordenar campos com os mais importantes primeiro
    priority_fields = ["case_id", "series_id", "modality", "body_regions"]
    sorted_fields = priority_fields + sorted(list(fieldnames - set(priority_fields)))

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sorted_fields)
        writer.writeheader()
        for row in all_data:
            writer.writerow(row)

    print(f"Consolidated {len(all_data)} rows into {output_file}")

if __name__ == "__main__":
    main()
