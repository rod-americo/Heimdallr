#!/usr/bin/env python3
import sys
import os
import argparse
import requests

SERVER_URL = "http://thor:8001/upload"

def upload_file(file_path):
    if not os.path.exists(file_path):
        print(f"Erro: Arquivo '{file_path}' não encontrado.")
        sys.exit(1)

    print(f"Enviando '{file_path}' para {SERVER_URL}...")
    
    try:
        with open(file_path, "rb") as f:
            files = {"file": f}
            response = requests.post(SERVER_URL, files=files)
            
        if response.status_code == 200:
            print("Sucesso!")
            print(response.json())
        else:
            print(f"Falha no envio. Código: {response.status_code}")
            try:
                print(response.json())
            except:
                print(response.text)
                
    except requests.exceptions.ConnectionError:
        print(f"Erro: Não foi possível conectar ao servidor em {SERVER_URL}")
        print("Verifique se o ingest.py está rodando.")
    except Exception as e:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Envia arquivo ZIP para o serviço de ingestão.")
    parser.add_argument("file_path", help="Caminho para o arquivo (geralmente .zip)")
    args = parser.parse_args()
    
    upload_file(args.file_path)
