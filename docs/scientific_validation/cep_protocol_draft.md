# Minuta do Protocolo de Pesquisa e Documentos Éticos (Plataforma Brasil)

**Projeto:** Validação de Ferramenta Oportunística Baseada em Inteligência Artificial para Avaliação da Densidade Mineral Óssea (BMD) em Tomografias Computadorizadas de Rotina.
**Investigador Principal:** Rodrigo Américo
**Status:** Rascunho Inicial para Submissão ao Sistema CEP/CONEP

Este documento contém os textos de apoio para agilizar a submissão do projeto na Plataforma Brasil e as minutas das Cartas de Anuência Institucional para as lideranças médicas (Rede D'Or / Rede Américas).

---

## 1. Carta de Anuência Institucional (Termo de Fiel Depositário)
*Esta carta deve ser assinada e carimbada pelo Diretor Médico ou Coordenador de Radiologia de cada hospital participante ANTES da submissão na Plataforma Brasil. Sugere-se imprimir em papel timbrado do hospital.*

---

**CARTA DE ANUÊNCIA E ACORDO DE UTILIZAÇÃO DE DADOS (TERMO DE FIEL DEPOSITÁRIO)**

Ao Comitê de Ética em Pesquisa (CEP),

Eu, [NOME DO DIRETOR MÉDICO/COORDENADOR], RG [NÚMERO], responsável legal pela instituição [NOME DO HOSPITAL / REDE], declaro para os devidos fins que autorizo a realização da pesquisa intitulada **"Validação de Ferramenta Oportunística Baseada em Inteligência Artificial para Avaliação da Densidade Mineral Óssea (BMD) em Tomografias Computadorizadas de Rotina"**, sob a responsabilidade do Pesquisador Principal Rodrigo Américo.

Declaro estar ciente de que:
1. O estudo possui caráter estritamente restrospectivo e observacional.
2. Os dados fornecidos referem-se a exames de tomografia computadorizada realizados no período de [JANEIRO/2023] a [MARÇO/2026], que já foram devidamente avaliados e laudados para fins clínicos primários, não havendo qualquer alteração no fluxo de atendimento ou impacto na terapêutica dos pacientes.
3. Os dados extraídos do sistema PACS/RIS local (incluindo metadados técnicos DICOM, laudos estruturados e características demográficas básicas como idade e sexo) serão submetidos a um rigoroso processo de anonimização automatizada, desvinculando-os dos dados de identificação direta nominal dos pacientes antes de qualquer análise estatística.
4. Assumo o compromisso, na condição de fiel depositário local, de salvaguardar os dados de nossa instituição até a sua correta anonimização, bem como autorizo o envio do conjunto de dados anonimizados para a base central do estudo (Projeto Heimdallr).
5. A instituição concorda que os resultados provenientes desta base de dados anonimizada poderão ser objeto de apresentação em congressos médicos e publicação em revistas científicas indexadas.

[Cidade], [Data].

___________________________________________________
**[NOME DO DIRETOR MÉDICO / COORDENADOR]**
[Cargo]
[CRM]
[Nome do Instituição]

---

## 2. Termo de Justificativa de Dispensa de TCLE
*Documento obrigatório a ser anexado na Plataforma Brasil para estudos retrospectivos sem contato com o paciente.*

---

**JUSTIFICATIVA PARA DISPENSA DE TERMO DE CONSENTIMENTO LIVRE E ESCLARECIDO (TCLE)**

Ao Comitê de Ética em Pesquisa (CEP),

Em atendimento à Resolução CNS nº 466/12 e complementar CNS nº 510/16, o pesquisador responsável solicita a dispensa do Termo de Consentimento Livre e Esclarecido (TCLE) para os sujeitos de pesquisa do estudo *"Validação de Ferramenta Oportunística Baseada em Inteligência Artificial para Avaliação da Densidade Mineral Óssea (BMD) em Tomografias Computadorizadas de Rotina"*.

A dispensa é fundamentada nos seguintes critérios:

1. **Delineamento Estritamente Retrospectivo:** O projeto utilizará exclusivamente dados secundários provenientes de exames de imagens radiológicas e laudos clínicos arquivados no banco de dados digital (PACS/RIS), referentes a procedimentos já concluídos no contexto da rotina assistencial entre os anos de 2023 e 2026.
2. **Inviabilidade Prática:** O banco de dados a ser analisado contará com um *n* amostral elevado (estimado entre 5.000 a 50.000 exames). A localização e contato individual com os milhares de pacientes, muitos dos quais podem ter mudado de contato ou ido a óbito na janela de tempo de até quatro anos, torna-se inviável sob o ponto de vista prático e científico.
3. **Ausência de Riscos à Integridade e Diagnóstico:** Não haverá intervenção sobre o indivíduo. A aplicação do algoritmo de inteligência artificial de leitura óssea ocorrerá posteriormente à assistência médica já prestada. Nenhuma decisão clínica presente ou futura dependerá ou será modificada por estes achados retrospectivos.
4. **Proteção Rigorosa dos Dados e Anonimização:** O acesso primário aos dados far-se-á sob rígido controle em infraestrutura homologada internamente. O pipeline de extração de dados eliminará ativamente todas as etiquetas DICOM com identificadores diretos de saúde (PHI - *Protected Health Information*), resultando num *dataset* de pesquisa anonimizado.

Por tratar-se de uma pesquisa essencialmente baseada em prontuário eletrônico e banco de imagens sem vínculo nominal direto na base de estudos, cujo contato prospectivo tornaria a pesquisa inexequível, contamos com o deferimento da dispensa do TCLE.

[Cidade], [Data].

___________________________________________________
**RODRIGO AMÉRICO**
Pesquisador Principal

---

## 3. Resumo Estruturado (Para a página inicial da Plataforma Brasil / "Informações Básicas do Projeto")

**Resumo:** 
A osteoporose é frequentemente subdiagnosticada até a ocorrência de fraturas por fragilidade. Tomografias computadorizadas (CT) abdominais e torácicas realizadas rotineiramente em ambiente hospitalar contêm dados cruciais sobre a densidade trabecular da coluna vertebral (L1), historicamente ignorados se não há suspeita óssea. O objetivo principal deste estudo é realizar a validação técnica e populacional de um pipeline de inteligência artificial volumétrica automática (*TotalSegmentator*) na predição de densidade mineral óssea (BMD) e rastreio oportunístico de osteoporose. O estudo retrospectivo analisará o arquivo de imagem (PACS) dos centros participantes entre 2023-2026. Espera-se definir os valores médios (normativos) em Unidades Hounsfield (HU) da vértebra L1 por faixa etária e sexo e compará-los aos métodos anatômicos tradicionais (ROI manual). 

**Introdução:**
A detecção oportunística aproveita exames de imagem feitos para indicações não relacionadas para extrair métricas de saúde óssea. Apesar de sua acurácia estabelecida em estudos internacionais (Pickhardt et al., 2013), falta validação multicêntrica formal e com grandes populações baseadas em automação volumétrica tridimensional no cenário técnico hospitalar nacional.

**Riscos e Benefícios:**
- **Riscos:** Classificado como risco Mínimo. O único revés em potencial diz respeito à quebra de confidencialidade de dados retrospectivos, cujo risco é ativamente mitigado pelo pipeline computacional de anonimização (remoção de PHI das tags DICOM). 
- **Benefícios:** Não há benefício direto imediato para o sujeito de pesquisa originário. A sociedade e as instituições se beneficiam por abrir portas à implementação futura e custo-efetiva de sistemas pre-alertivos para osteoporose, além da fixação científica de tabelas normativas validadas populacionalmente. 

**Metodologia de Análise de Dados:**
Os volumes tomográficos (arquivos DICOM) extraídos num ambiente seguro (staging) alimentarão ferramentas validadas de Segmentação e Inferência de Fase (*TotalSegmentator*, referenciado por Wasserthal et al., *Radiology: Artificial Intelligence*, 2023). Serão mensurados valores de Atenuação Trabecular (HU) e correlações geométricas do corpo da vértebra L1, controlados pela fase de contraste gerada pela rede neural da ferramenta. O arquivo será consolidado, e as análises estatísticas utilizarão correlação de Pearson, análise de concordância (Bland-Altman e Kappa) e curvas ROC (Receiver Operating Characteristic) definindo os potenciais pontos de corte diagnósticos para "Normalidade" e "Risco".

---

*Arquivo gerado e organizado diretamente do Heimdallr. Próximos passos operacionais:*
1. Apresentar Carta de Anuência aos Diretores/Coordenadores Médicos das unidades (Américas/D'Or).
2. Definir Hospitais específicos para qualificar O Centro Coordenador.
3. Submissão pelo Investigador Principal na interface da Plataforma Brasil.*
