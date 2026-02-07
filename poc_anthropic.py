import base64
import anthropic
import json
import os
import random
import sys
import time
from anthropic_report_builder import extrair_json_do_texto, montar_laudo_a_partir_json
from img_conversor import otimizar_imagem_para_api
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Obter a chave da API do ambiente
api_key = os.getenv("ANTHROPIC_API_KEY")

if not api_key:
    raise ValueError("A chave da API 'ANTHROPIC_API_KEY' não foi encontrada no arquivo .env ou nas variáveis de ambiente.")

client = anthropic.Anthropic(api_key=api_key)

USD_TO_BRL = 5.50
USD_PER_MTOK_INPUT = 3.0
USD_PER_MTOK_OUTPUT = 15.0


def _extrair_usage(message):
    usage = getattr(message, "usage", None)
    if usage is None:
        return None, None

    if isinstance(usage, dict):
        return usage.get("input_tokens"), usage.get("output_tokens")

    return getattr(usage, "input_tokens", None), getattr(usage, "output_tokens", None)


def _estimar_tokens_texto(texto):
    if not texto:
        return 0
    # Heurística simples: ~4 caracteres por token.
    return max(1, int(len(texto) / 4))


def _calcular_custo_brl(input_tokens, output_tokens):
    custo_input_usd = (input_tokens / 1_000_000) * USD_PER_MTOK_INPUT
    custo_output_usd = (output_tokens / 1_000_000) * USD_PER_MTOK_OUTPUT
    return (custo_input_usd + custo_output_usd) * USD_TO_BRL

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
        model="claude-sonnet-4-5-20250929",
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

    texto = message.content[0].text
    input_tokens, output_tokens = _extrair_usage(message)
    usage_from_api = input_tokens is not None and output_tokens is not None

    if not usage_from_api:
        # Fallback quando usage não vier da API.
        input_tokens = _estimar_tokens_texto(system_instruction + "\n" + user_message)
        output_tokens = _estimar_tokens_texto(texto)

    return {
        "text": texto,
        "input_tokens": int(input_tokens),
        "output_tokens": int(output_tokens),
        "usage_from_api": usage_from_api,
    }

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
    inicio = time.perf_counter()

    try:
        resultado = analisar_rx_leito(caminho_completo, idade_paciente)
        dados_json = extrair_json_do_texto(resultado["text"])
        laudo_textual = montar_laudo_a_partir_json(dados_json)
        fim = time.perf_counter()

        input_tokens = resultado["input_tokens"]
        output_tokens = resultado["output_tokens"]
        total_tokens = input_tokens + output_tokens
        custo_brl = _calcular_custo_brl(input_tokens, output_tokens)
        origem_tokens = "API" if resultado["usage_from_api"] else "estimado"

        print("LAUDO ESTRUTURADO")
        print(laudo_textual)
        print("-" * 30)
        print("JSON EXTRAÍDO")
        print(json.dumps(dados_json, ensure_ascii=False, indent=2))
        print("-" * 30)
        print(f"Arquivo usado: {arquivo_escolhido}")
        print(f"Tempo de processamento: {fim - inicio:.2f} s")
        print(
            "Quantidade de tokens: "
            f"{total_tokens} (input: {input_tokens}, output: {output_tokens}, origem: {origem_tokens})"
        )
        print(f"Valor estimado em R$: {custo_brl:.4f}")
    except Exception as e:
        print(f"Ocorreu um erro ao chamar a API: {e}")

if __name__ == "__main__":
    main()
