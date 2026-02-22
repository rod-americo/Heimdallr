
MEDGEMMA_SYSTEM_PROMPT = "Expert radiologist for bedside AP CXR of a {age} patient. Account for the inherent AP magnification of the cardiac silhouette and avoid overestimating heart size. Concise findings."

MEDGEMMA_USER_PROMPT = """
One phrase for each: 
Pulmonary fields (lungs+pleura together): [text].
Cardiomediastinal silhouette (heart+mediastinum together): [text].
Devices: [text]."""

OPENAI_PROMPT_TEMPLATE = """Traduza este pré-laudo de RX de tórax (inglês -> pt-BR formal radiologia brasileira.  
Mantenha termos exatos, estrutura e tom conservador ("possible" -> "possível"). Não mencionar sobre eventual rotação do paciente nem sobre a técnica do exame, como realização AP portátil. 

REGRAS DE VOCABULÁRIO (Traduza EXATAMENTE como listado abaixo):
- "enlarged heart" -> "aumento da área cardíaca" (NÃO use cardiomegalia)
- "blunted costophrenic angle" -> "velamento do seio costofrênico"
- "infiltrate" -> "opacidade"
- "patchy opacities" -> "opacidades em focos esparsos"
- "bilateral pleural effusions" -> "derrame pleural bilateral"
- "device" -> "dispositivo"
- "Devices: None" -> "Dispositivos: nenhum identificado"
- "pacemaker" -> "dispositivo de eletroestimulação"
- "Pulmonary fields: Clear" -> "Campos pulmonares: sem alterações relevantes"
- "bilateral pulmonary edema with layering pleural effusions" -> "edema pulmonar e derrame pleural bilaterais"
- "normal heart size" -> "área cardíaca normal"

Saída APENAS o laudo traduzido, uma frase por linha, sem o termo ACHADOS e sem impressão.

Dados de entrada:
---
{saida_medgemma}
---"""