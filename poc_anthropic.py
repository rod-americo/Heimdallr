import base64
import anthropic
import os
import random
import sys
from img_conversor import otimizar_imagem_para_api
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Obter a chave da API do ambiente
api_key = os.getenv("ANTHROPIC_API_KEY")

if not api_key:
    raise ValueError("A chave da API 'ANTHROPIC_API_KEY' não foi encontrada no arquivo .env ou nas variáveis de ambiente.")

client = anthropic.Anthropic(api_key=api_key)

def analisar_rx_leito(caminho_imagem, idade_paciente):
    
    # 1. Otimizar imagem e codificar para Base64
    binary_data, media_type = otimizar_imagem_para_api(caminho_imagem)
    base64_image = base64.b64encode(binary_data).decode("utf-8")

    # 2. O Prompt "Matador" (System Prompt)
    try:
        with open("prompts/rx_thorax_ap.txt", "r", encoding="utf-8") as f:
            system_instruction = f.read()
    except FileNotFoundError:
        return "Erro: Arquivo de prompt 'prompts/rx_thorax_ap.txt' não encontrado."

    # 3. O User Prompt (O caso específico)
    user_message = f"""
    Patient Metadata:
    - Age: {idade_paciente} years old.
    
    Task:
    Extract the radiological data from this image into the required JSON format.
    REMINDER: be extremely conservative with findings. If unsure, return false/null.
    """
    

    # 4. A Chamada à API
    # Modelos disponíveis
    # claude-opus-4-6
    # claude-opus-4-5-20251101
    # claude-haiku-4-5-20251001
    # claude-sonnet-4-5-20250929
    # claude-opus-4-1-20250805
    # claude-opus-4-20250514
    # claude-sonnet-4-20250514
    # claude-3-haiku-20240307
    
    message = client.messages.create(
        model="claude-opus-4-5-20251101",
        max_tokens=1024,
        temperature=0.0, # Temperatura ZERO para máxima precisão e determinismo
        system=system_instruction,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": base64_image,
                        },
                    },
                    {
                        "type": "text",
                        "text": user_message
                    }
                ],
            }
        ],
    )

    return message.content[0].text

def extrair_idade(nome_arquivo):
    """Extrai a idade do nome do arquivo no formato NNNY_..."""
    try:
        nome_base = os.path.basename(nome_arquivo)
        parte_idade = nome_base.split('_')[0]
        idade_str = parte_idade.replace('Y', '').replace('M', '').replace('D', '') # Remove Y, M, D se houver
        return str(int(idade_str)) # Remove zeros à esquerda
    except ValueError:
        return "Desconhecida"

def main():
    diretorio_imagens = "_ap_rxays_png"
    caminho_completo = ""
    
    # Verifica se foi passado um argumento
    if len(sys.argv) > 1:
        caminho_completo = sys.argv[1]
        if not os.path.exists(caminho_completo):
            print(f"Erro: O arquivo '{caminho_completo}' não foi encontrado.")
            return
        arquivo_escolhido = os.path.basename(caminho_completo)
    else:
        # Modo Aleatório
        try:
            arquivos = [f for f in os.listdir(diretorio_imagens) if f.lower().endswith('.png')]
        except FileNotFoundError:
            print(f"Erro: O diretório '{diretorio_imagens}' não foi encontrado.")
            return

        if not arquivos:
            print(f"Erro: Nenhum arquivo .png encontrado em '{diretorio_imagens}'.")
            return

        arquivo_escolhido = random.choice(arquivos)
        caminho_completo = os.path.join(diretorio_imagens, arquivo_escolhido)

    idade_paciente = extrair_idade(arquivo_escolhido)

    print(f"Processando arquivo: {arquivo_escolhido}")
    print(f"Idade do Paciente: {idade_paciente} anos")
    print("-" * 30)

    try:
        laudo = analisar_rx_leito(caminho_completo, idade_paciente)
        print(laudo)
        print("-" * 30)
        print(f"Arquivo usado: {arquivo_escolhido}")
    except Exception as e:
        print(f"Ocorreu um erro ao chamar a API: {e}")

if __name__ == "__main__":
    main()