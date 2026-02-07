import io
import argparse
import sys
import os
import numpy as np
import pydicom
from PIL import Image

def dicom_to_pil(path: str) -> Image.Image:
    """
    Lê um arquivo DICOM e converte para PIL Image (RGB),
    aplicando a janela (Window Center/Width) e Rescale Slope/Intercept se disponíveis.
    Baseado na lógica do medgemma_rx.py.
    """
    try:
        ds = pydicom.dcmread(path)
        arr = ds.pixel_array.astype(np.float32)

        # Aplicar Rescale Slope/Intercept
        slope = float(getattr(ds, "RescaleSlope", 1.0))
        intercept = float(getattr(ds, "RescaleIntercept", 0.0))
        arr = arr * slope + intercept

        # Aplicar Window Center/Width
        wc = getattr(ds, "WindowCenter", None)
        ww = getattr(ds, "WindowWidth", None)
        
        # Pode ser Multi-value, pega o primeiro
        if wc is not None and ww is not None:
            wc = float(wc[0] if hasattr(wc, "__len__") else wc)
            ww = float(ww[0] if hasattr(ww, "__len__") else ww)
            lo, hi = wc - ww/2, wc + ww/2
            arr = np.clip(arr, lo, hi)

        # Normalização Min-Max para 0-255
        arr -= arr.min()
        arr /= (arr.max() + 1e-6)
        arr = (arr * 255).astype(np.uint8)

        # Retorna como RGB
        return Image.fromarray(arr).convert("RGB")
    except Exception as e:
        raise Exception(f"Erro na conversão DICOM: {e}")

def otimizar_imagem_para_api(caminho_arquivo: str, limite_mb: float = 4.0):
    """
    Lê a imagem (DICOM, PNG, JPG), converte para Escala de Cinza
    e comprime como JPEG até caber no limite.
    
    Args:
        caminho_arquivo (str): Caminho para o arquivo de imagem
        limite_mb (float): Tamanho máximo desejado em MB
        
    Returns:
        tuple: (bytes da imagem comprimida, string do tipo mime "image/jpeg")
    """
    try:
        # 1. Determina o tipo de arquivo e abre a imagem
        ext = os.path.splitext(caminho_arquivo)[1].lower()
        
        if ext in ['.dcm', '.dicom']:
            img = dicom_to_pil(caminho_arquivo)
        else:
            img = Image.open(caminho_arquivo)

        # Força o carregamento da imagem se veio de Image.open (lazy)
        # Se veio do dicom_to_pil já é uma instância em memória.
        if hasattr(img, 'load'):
            img.load()

        # 2. Converte para Escala de Cinza ('L')
        # Raio-X não precisa de cor. Isso reduz o tamanho drasticamente.
        if img.mode != 'L':
            img = img.convert('L')
            
        # 3. Redimensiona se for gigantesca (ex: maior que 2048px)
        max_dimension = 2048
        if max(img.size) > max_dimension:
            ratio = max_dimension / max(img.size)
            new_size = (int(img.width * ratio), int(img.height * ratio))
            img = img.resize(new_size, Image.Resampling.LANCZOS)
        
        # 4. Loop de Compressão: Tenta reduzir qualidade até caber
        quality = 95
        buffer = io.BytesIO()
        
        while quality > 10:
            buffer.seek(0)
            buffer.truncate(0)
            # Salva no buffer como JPEG
            img.save(buffer, format="JPEG", quality=quality)
            
            size_mb = buffer.tell() / (1024 * 1024)
            
            if size_mb < limite_mb:
                return buffer.getvalue(), "image/jpeg"
            
            quality -= 5
            
        raise Exception(f"Não foi possível comprimir a imagem para menos de {limite_mb}MB.")
        
    except Exception as e:
        raise Exception(f"Erro ao processar imagem {caminho_arquivo}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Conversor e Otimizador de Imagens (DICOM/PNG/JPG -> JPG Otimizado).")
    parser.add_argument("input", help="Caminho do arquivo de imagem de entrada")
    parser.add_argument("-o", "--output", help="Caminho do arquivo de saída (opcional)")
    parser.add_argument("--limit", type=float, default=4.0, help="Limite de tamanho em MB (padrão: 4.0)")

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Erro: Arquivo de entrada '{args.input}' não encontrado.")
        sys.exit(1)

    try:
        print(f"Processando '{args.input}'...")
        optimized_bytes, mime_type = otimizar_imagem_para_api(args.input, args.limit)
        
        output_path = args.output
        if not output_path:
            # Se não fornecido, salva no mesmo diretório do arquivo original (para testes locais) 
            # ou no diretório atual? O request diz:
            # "gera arquivo.jpg na pasta do script" -> se entendi bem, seria CWD ou pasta do script.
            # Vou assumir CWD + nome base se não especificado, ou melhor, seguir o padrão do script anterior:
            # base + _otimizado.jpg
            base_name = os.path.splitext(os.path.basename(args.input))[0]
            output_path = f"{base_name}_otimizado.jpg"
            
        with open(output_path, "wb") as f:
            f.write(optimized_bytes)
            
        final_size_mb = len(optimized_bytes) / (1024 * 1024)
        print(f"Sucesso! Imagem salva em '{output_path}' ({final_size_mb:.2f} MB)")
        
    except Exception as e:
        print(f"Erro Crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
