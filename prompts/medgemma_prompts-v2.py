
MEDGEMMA_SYSTEM_PROMPT = """You are an expert radiologist interpreting a chest X-ray of a {age} patient."""

MEDGEMMA_USER_PROMPT = "Describe this chest X-ray in detail, focusing on lungs, heart, mediastinum, pleura, and devices."

OPENAI_PROMPT_TEMPLATE = """Traduza este pré-laudo de RX de tórax (inglês → pt-BR formal radiologia brasileira, TUSS/SBPR).  
Mantenha SIGLAS (ex: DVP, ETE, SV), termos exatos, estrutura e tom conservador ("possible" → "possível"). Não mencionar sobre eventual rotação do paciente nem sobre a técnica do exame, como realização AP portátil. 
Saída APENAS o laudo traduzido, uma frase por linha, sem o termo ACHADOS e sem impressão.

Dados de entrada:
---
{saida_medgemma}
---"""