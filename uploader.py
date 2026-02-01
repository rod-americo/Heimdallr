import requests
import argparse
import sys
import os
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.console import Console
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

console = Console()

SERVER_URL = "http://thor:8001/upload"

def create_progress_callback(task_id, progress):
    def callback(monitor):
        progress.update(task_id, completed=monitor.bytes_read)
    return callback

def main():
    parser = argparse.ArgumentParser(description="Cliente de Upload para Radiology Lab")
    parser.add_argument("file_path", help="Caminho do arquivo para upload")
    parser.add_argument("--server", default=SERVER_URL, help=f"URL do servidor (padrão: {SERVER_URL})")
    
    args = parser.parse_args()
    
    # Use the provided server URL, cleaning up potential trailing slash issues if needed
    target_url = args.server
    
    # Simple check to ensure we are probably hitting the upload endpoint if the user just gave the base url
    if not target_url.endswith("/upload") and "8001" in target_url and not target_url.endswith("/"):
         # If user gave "http://thor:8001", append "/upload"
         console.print(f"[yellow]Considerando url base. Adicionando /upload a {target_url}[/yellow]")
         target_url = f"{target_url}/upload"

    if not os.path.exists(args.file_path):
        console.print(f"[red]Erro: O arquivo '{args.file_path}' não foi encontrado.[/red]")
        sys.exit(1)

    try:
        file_path = args.file_path
        delete_after_upload = False
        temp_dir = None

        if os.path.isdir(file_path):
            console.print(f"[yellow]O caminho '{file_path}' é uma pasta. Criando arquivo zip...[/yellow]")
            import shutil
            import tempfile
            
            # Create a temporary zip file
            temp_dir = tempfile.mkdtemp()
            # shutil.make_archive needs the base name without extension
            base_name = os.path.basename(file_path.rstrip(os.sep))
            zip_base = os.path.join(temp_dir, base_name)
            
            zip_path = shutil.make_archive(zip_base, 'zip', file_path)
            file_path = zip_path
            delete_after_upload = True
            
            filename = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            console.print(f"[green]Arquivo zip criado: {file_path} ({file_size} bytes)[/green]")
        else:
            filename = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)

        response = None
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            
            task_id = progress.add_task(f"[cyan]Enviando {filename}...", total=file_size)
            
            # Create the encoder
            # We open the file here. Note: we need to ensure it closes.
            # MultipartEncoder reads from the file object.
            with open(file_path, 'rb') as f:
                encoder = MultipartEncoder(
                    fields={'file': (filename, f, 'application/octet-stream')}
                )
                
                # Create the monitor with the callback
                monitor = MultipartEncoderMonitor(encoder, create_progress_callback(task_id, progress))
                
                headers = {'Content-Type': monitor.content_type}
                
                # Send the request
                response = requests.post(
                    target_url, 
                    data=monitor, 
                    headers=headers, 
                    timeout=300
                )
        
        # Cleanup temporary files
        if delete_after_upload:
            if os.path.exists(file_path):
                os.remove(file_path)
            if temp_dir and os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir)

        if response and response.status_code == 200:
            result = response.json()
            console.print(f"[green]Sucesso! Arquivo processado: {result.get('generated_nifti')}[/green]")
            console.print(f"[green]Status: {result.get('status')}[/green]")
        elif response:
            console.print(f"[bold red]Erro no servidor: {response.status_code}[/bold red]")
            console.print(f"Detalhes: {response.text}")
            sys.exit(1)
        else:
             # Should not happen unless exception occurred before response
             sys.exit(1)

    except ImportError:
        console.print("[bold red]Erro: requests-toolbelt não encontrado.[/bold red]")
        console.print("Por favor, instale: pip install requests-toolbelt")
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        console.print(f"[bold red]Erro: Não foi possível conectar ao servidor em {target_url}.[/bold red]")
        console.print("Verifique se o servidor está rodando e acessível.")
        sys.exit(1)
    except Exception as ex:
        console.print(f"[bold red]Ocorreu um erro inesperado:[/bold red] {ex}")
        sys.exit(1)

if __name__ == "__main__":
    main()
