#!/usr/bin/env python3
"""
Heimdallr CLI Upload Client

Command-line tool for uploading DICOM exams to the Heimdallr pipeline.
Supports both ZIP files and directories (auto-zipped).
Features real-time progress bar with transfer speed.

Usage:
    python uploader.py /path/to/exam.zip
    python uploader.py /path/to/dicom_folder/
    python uploader.py /path/to/exam.zip --server http://custom-server:8001/upload
"""

import requests
import argparse
import sys
import os
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.console import Console
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

console = Console()

# Default server URL (change 'thor' to your server hostname)
SERVER_URL = "http://thor:8001/upload"

def create_progress_callback(task_id, progress):
    """
    Create a callback function for tracking upload progress.
    
    Args:
        task_id: Rich progress task ID
        progress: Rich Progress object
    
    Returns:
        Callback function that updates progress bar
    """
    def callback(monitor):
        # Update progress bar with bytes uploaded so far
        progress.update(task_id, completed=monitor.bytes_read)
    return callback

def main():
    """
    Main upload client function.
    
    Handles:
    1. Argument parsing
    2. Directory-to-ZIP conversion if needed
    3. File upload with progress bar
    4. Cleanup of source files after successful upload
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Upload client for Heimdallr Radiology Lab")
    parser.add_argument("file_path", help="Path to ZIP file or DICOM folder")
    parser.add_argument("--server", default=SERVER_URL, help=f"Server URL (default: {SERVER_URL})")
    
    args = parser.parse_args()
    
    # Normalize server URL
    # Ensures we're hitting the /upload endpoint
    target_url = args.server
    
    # Auto-append /upload if user provided base URL (e.g., "http://thor:8001")
    if not target_url.endswith("/upload") and "8001" in target_url and not target_url.endswith("/"):
         console.print(f"[yellow]Detected base URL. Appending /upload to {target_url}[/yellow]")
         target_url = f"{target_url}/upload"

    # Validate file/directory exists
    if not os.path.exists(args.file_path):
        console.print(f"[red]Error: File or folder '{args.file_path}' not found.[/red]")
        sys.exit(1)

    try:
        file_path = args.file_path
        delete_after_upload = False  # Flag to delete temp ZIP after upload
        temp_dir = None

        # If user provided a directory, convert it to a ZIP file
        if os.path.isdir(file_path):
            console.print(f"[yellow]Path '{file_path}' is a folder. Creating ZIP file...[/yellow]")
            import shutil
            import tempfile
            
            # Create temporary directory for ZIP file
            temp_dir = tempfile.mkdtemp()
            # shutil.make_archive needs the base name without extension
            base_name = os.path.basename(file_path.rstrip(os.sep))
            zip_base = os.path.join(temp_dir, base_name)
            
            # Create ZIP archive from directory
            zip_path = shutil.make_archive(zip_base, 'zip', file_path)
            file_path = zip_path
            delete_after_upload = True  # Mark for cleanup
            
            filename = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            console.print(f"[green]ZIP file created: {file_path} ({file_size} bytes)[/green]")
        else:
            # File already exists, just get name and size
            filename = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)

        # Upload file with real-time progress bar
        response = None
        with Progress(
            SpinnerColumn(),                  # Animated spinner
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),                      # Progress bar
            TaskProgressColumn(),             # Percentage complete
            TransferSpeedColumn(),            # MB/s upload speed
            TimeRemainingColumn(),            # Estimated time remaining
            console=console
        ) as progress:
            
            task_id = progress.add_task(f"[cyan]Uploading {filename}...", total=file_size)
            
            # Create multipart encoder for streaming upload
            # This allows efficient progress tracking for large files
            with open(file_path, 'rb') as f:
                encoder = MultipartEncoder(
                    fields={'file': (filename, f, 'application/octet-stream')}
                )
                
                # Wrap encoder with monitor to track upload progress
                monitor = MultipartEncoderMonitor(encoder, create_progress_callback(task_id, progress))
                
                headers = {'Content-Type': monitor.content_type}
                
                # Send POST request to server with 5-minute timeout
                response = requests.post(
                    target_url, 
                    data=monitor, 
                    headers=headers, 
                    timeout=300  # 5 minutes
                )
        
        # Clean up temporary files created during ZIP conversion
        if delete_after_upload:
            if os.path.exists(file_path):
                os.remove(file_path)  # Delete temp ZIP
            if temp_dir and os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir) # Delete temp directory

        # Handle server response
        if response and response.status_code == 200:
            result = response.json()
            console.print(f"[green]Success! File processed: {result.get('generated_nifti')}[/green]")
            console.print(f"[green]Status: {result.get('status')}[/green]")

            # Delete original source file/folder after successful upload
            # This ensures the upload directory stays clean
            try:
                if os.path.exists(args.file_path):
                    if os.path.isdir(args.file_path):
                        import shutil
                        shutil.rmtree(args.file_path)
                        console.print(f"[yellow]Original folder deleted: {args.file_path}[/yellow]")
                    else:
                        os.remove(args.file_path)
                        console.print(f"[yellow]Original file deleted: {args.file_path}[/yellow]")
            except Exception as e:
                console.print(f"[red]Error deleting original: {e}[/red]")
        elif response:
            console.print(f"[bold red]Server error: {response.status_code}[/bold red]")
            console.print(f"Details: {response.text}")
            sys.exit(1)
        else:
             # Should not happen unless exception occurred before response
             sys.exit(1)

    # Error handling for common issues
    except ImportError:
        console.print("[bold red]Error: requests-toolbelt not found.[/bold red]")
        console.print("Please install: pip install requests-toolbelt")
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        console.print(f"[bold red]Error: Could not connect to server at {target_url}.[/bold red]")
        console.print("Verify that the server is running and accessible.")
        sys.exit(1)
    except Exception as ex:
        console.print(f"[bold red]Unexpected error:[/bold red] {ex}")
        sys.exit(1)

if __name__ == "__main__":
    main()
