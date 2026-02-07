import os
import shutil
import json
import glob

SOURCE_DIR = "data/dataset"
DEST_DIR = "_ap_rxays_png"

def main():
    if not os.path.exists(DEST_DIR):
        os.makedirs(DEST_DIR)
        print(f"Created directory: {DEST_DIR}")

    count = 0
    skipped_no_metadata = 0
    skipped_no_age = 0
    
    for root, dirs, files in os.walk(SOURCE_DIR):
        if "thorax_image.png" in files:
            # Find metadata file
            json_files = glob.glob(os.path.join(root, "metadado_*.json"))
            
            if not json_files:
                # specific logic: sometimes metadata might be in a different format or missing
                # checking for any json file just in case if glob failed (unlikely if pattern matches)
                print(f"No metadata file found in {root}, skipping.")
                skipped_no_metadata += 1
                continue
            
            # Use the first found metadata file (assuming one per folder or consistent data)
            metadata_path = json_files[0]
            
            try:
                with open(metadata_path, 'r') as f:
                    data = json.load(f)
                    
                age = data.get("Patient's Age")
                
                if not age:
                    print(f"Age not found in {metadata_path}, skipping.")
                    skipped_no_age += 1
                    continue
                
                # Clean age format if necessary (User said 000Y format, so we use it as is)
                # Ensure filename safe just in case
                age = str(age).strip()
                
                parent_dir_name = os.path.basename(root)
                new_filename = f"{age}_{parent_dir_name}.png"
                dest_path = os.path.join(DEST_DIR, new_filename)
                
                source_image = os.path.join(root, "thorax_image.png")
                shutil.copy2(source_image, dest_path)
                count += 1
                
                if count % 100 == 0:
                    print(f"Processed {count} images...")
                    
            except Exception as e:
                print(f"Error processing {root}: {e}")

    print(f"\nProcessing complete.")
    print(f"Total images copied: {count}")
    print(f"Skipped (no metadata file): {skipped_no_metadata}")
    print(f"Skipped (no age in metadata): {skipped_no_age}")

if __name__ == "__main__":
    main()
