#!/usr/bin/env python3
"""
Database Backfill Script

Updates the database with calculation results from already processed cases.
Scans output/ directory for cases with resultados.json and updates the database.
"""

import json
import sqlite3
from pathlib import Path

# Import config for DB path
import config

def backfill_database():
    """
    Backfill database with calculation results from existing processed cases.
    """
    output_dir = config.OUTPUT_DIR
    db_path = config.DB_PATH
    
    print("=== Database Backfill Script ===\n")
    print(f"Output directory: {output_dir}")
    print(f"Database: {db_path}\n")
    
    # Get all case directories
    case_dirs = [d for d in output_dir.iterdir() if d.is_dir()]
    print(f"Found {len(case_dirs)} case directories\n")
    
    updated_count = 0
    skipped_count = 0
    error_count = 0
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    for case_dir in case_dirs:
        case_id = case_dir.name
        id_json = case_dir / "id.json"
        results_json = case_dir / "resultados.json"
        
        # Check if we have both required files
        if not id_json.exists():
            print(f"⚠️  {case_id}: Missing id.json, skipping")
            skipped_count += 1
            continue
            
        if not results_json.exists():
            print(f"⚠️  {case_id}: Missing resultados.json, skipping")
            skipped_count += 1
            continue
        
        try:
            # Read StudyInstanceUID from id.json
            with open(id_json, 'r') as f:
                id_data = json.load(f)
                study_uid = id_data.get("StudyInstanceUID")
            
            if not study_uid:
                print(f"⚠️  {case_id}: No StudyInstanceUID in id.json, skipping")
                skipped_count += 1
                continue
            
            # Read calculation results
            with open(results_json, 'r') as f:
                results_data = json.load(f)
            
            # Update database
            c.execute(
                "UPDATE dicom_metadata SET CalculationResults = ? WHERE StudyInstanceUID = ?",
                (json.dumps(results_data), study_uid)
            )
            
            if c.rowcount > 0:
                print(f"✓ {case_id}: Updated")
                updated_count += 1
            else:
                print(f"⚠️  {case_id}: StudyInstanceUID not found in database")
                skipped_count += 1
                
        except Exception as e:
            print(f"✗ {case_id}: Error - {e}")
            error_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n=== Summary ===")
    print(f"Total cases: {len(case_dirs)}")
    print(f"Updated: {updated_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")
    
    if updated_count > 0:
        print(f"\n✓ Successfully backfilled {updated_count} cases!")
    else:
        print("\n⚠️  No cases were updated")

if __name__ == "__main__":
    backfill_database()
