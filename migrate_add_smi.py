#!/usr/bin/env python3
# Copyright (c) 2026 Rodrigo Americo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Database Migration: Add SMI Field

Adds SMI (Skeletal Muscle Index) column to the dicom_metadata table.
Safe to run multiple times - checks for existing column before adding.
"""

import sqlite3
import sys
from pathlib import Path

# Import configuration
import config

def migrate():
    """Add SMI column to dicom_metadata table."""
    db_path = config.DB_PATH
    
    if not db_path.exists():
        print(f"❌ Database not found at {db_path}")
        print("   Please ensure the database exists before running migration.")
        sys.exit(1)
    
    print(f"🔧 Migrating database: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get current table schema
        cursor.execute("PRAGMA table_info(dicom_metadata)")
        columns = [row[1] for row in cursor.fetchall()]
        
        print(f"   Current columns: {', '.join(columns)}")
        
        # Check if SMI column exists
        if 'SMI' not in columns:
            print("   Adding column: SMI (REAL)")
            cursor.execute("ALTER TABLE dicom_metadata ADD COLUMN SMI REAL")
            print("   ✓ SMI column added")
        else:
            print("   ℹ SMI column already exists")
        
        conn.commit()
        
        # Verify final schema
        cursor.execute("PRAGMA table_info(dicom_metadata)")
        final_columns = [row[1] for row in cursor.fetchall()]
        
        print(f"\n✅ Migration complete!")
        print(f"   Final columns: {', '.join(final_columns)}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate()
