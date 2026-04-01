# Estudo de Validação: Comparação de Métodos de Medição de BMD em L1 por CT

*"Automated L1 Bone Mineral Density Assessment by CT: Comparison of Volumetric 3D Segmentation, Axial Oval ROI, and Sagittal Oval ROI for Opportunistic Osteoporosis Screening in Outpatient, Emergency, and Inpatient Settings"*

**Status:** Em desenvolvimento  
**Data de início:** Março 2026  
**Responsável:** Rodrigo Américo / Equipe Heimdallr  

---

## 1. Contexto e Motivação

A detecção oportunística de osteoporose por CT utiliza a atenuação trabecular da vértebra L1 como proxy não invasivo da densidade mineral óssea (BMD). Estudos seminais (Pickhardt et al., 2013; Jang et al., 2019 — *Radiology*) estabeleceram valores normativos usando ROI oval axial posicionado manualmente ou semi-automaticamente na porção anterior do trabeculado.

Com o advento de ferramentas de segmentação automática 3D como o **TotalSegmentator**, tornou-se viável obter a máscara volumétrica completa do corpo vertebral sem intervenção manual. Contudo, não existe na literatura uma validação sistemática entre:
1. HU volumétrico (média 3D de toda a máscara)
2. ROI oval axial anterior automático (metodologia Pickhardt)
3. ROI oval sagital automático (plano coronal/sagital, como visualizado clinicamente)

O projeto **Heimdallr** dispõe de pipeline 100% automatizado para calcular as três métricas, representando uma oportunidade única de publicação.

---

## 2. Hipótese

> O HU médio obtido por segmentação volumétrica 3D automática (TotalSegmentator) apresenta correlação e concordância clinicamente aceitáveis com o ROI oval axial anterior (método padrão) e com o ROI oval sagital, podendo substituí-lo para fins de rastreio oportunístico de osteoporose.

---

## 3. Objetivos

### Primário
- Comparar os valores de HU-L1 obtidos pelos três métodos de medição em uma coorte massiva de pacientes submetidos a CT de tórax ou abdômen (ambiente ambulatorial, pronto-socorro e internação).

### Secundários
- Avaliar a reprodutibilidade intra e inter-método (ICC, Bland-Altman)
- Analisar o impacto clínico da diferença entre métodos na classificação de osteopenia/osteoporose (limiar < 135 HU e < 90 HU)
- Estratificar resultados por sexo, faixa etária e uso de contraste IV
- Identificar qual método apresenta menor variabilidade e maior correlação com critérios DXA/FRAX (quando disponíveis)

---

## 4. Desenho do Estudo

| Atributo | Descrição |
|---|---|
| Tipo | Estudo transversal, retrospectivo, observacional |
| População | Pacientes adultos (≥ 18 anos) submetidos a CT de tórax ou abdômen (Ambulatório, Pronto-Socorro e Internação) |
| Origem dos dados | Dataset do Heimdallr (`data/dataset`) extraído de PACS institucionais |
| Critérios de inclusão | CT com cobertura da vértebra L1, resolução ≤ 3mm, sem artefato metálico em L1 |
| Critérios de exclusão | Fraturas patológicas/traumáticas em L1, fixação cirúrgica, metástases ósseas (identificadas via NLP nos laudos) |
| N estimado | ≥ 5.000 exames na amostra inicial (meta final: 50.000 exames para normatização) |

### 4.1. Covariáveis, Fases de Contraste e Metadados Técnicos
Para garantir a robustez da análise e permitir o ajuste estatístico rigoroso, o pipeline extrai e salva 30 campos DICOM (*DICOM tags*) da série diagnóstica principal de cada caso. Além disso, as séries são classificadas por rede neural em pré ou pós-contraste, mitigando os principais viéses radiométricos:
- **Demográficos base:** Idade exata (cálculo via `PatientBirthDate` vs data do exame), Sexo da Ficha DICOM.
- **Fase de Contraste:** Validação cruzada do HU em séries Sem Contraste *vs.* Fases Contrastadas (Arterial, Venosa, Tardia), permitindo a criação de fatores de correção preditivos para o uso de exames com contraste no screening oportunístico e mitigando artefato de elevação do HU induzido pelo iodo intraósseo. A predição de fase é realizada automaticamente pelo algoritmo embarcado no *TotalSegmentator* (Wasserthal et al., 2023).
- **Filtragem baseada no Laudo (NLP):** Exclusão automatizada de pacientes cujos laudos mencionem fraturas vertebrais, metástases líticas/blásticas, pinos de artrodese em L1, ou doença óssea metabólica severa atípica.
- **Parâmetros de Aquisição:** `KVP` (kvP), `TubeCurrent`, `ExposureTime`, `Exposure`. A variação de kVp (ex: 100 vs 120 vs 140) afeta diretamente o valor de atenuação em HU.
- **Parâmetros de Reconstrução:** `SliceThickness`, `SpacingBetweenSlices` (crucial para o volume), `ConvolutionKernel` e matriz da imagem (`Rows`/`Columns`).
- **Equipamento:** `Manufacturer` e `ManufacturerModelName`, permitindo análise de reprodutibilidade e variância de HU multiespecifica entre fornecedores (GE, Siemens, Canon, Philips).
- **Controle de Qualidade Geométrico:** `GantryDetectorTilt` (tilt > 0 pode causar oversampling ou undersampling volumétrico de ROI oval e de L1) e `PatientPosition` (garantir orientação Head-First/Feet-First standardizada).
- **Cobertura:** Avaliação de `BodyPartExamined`, `ProtocolName` e `SeriesDescription` para validação primária de exame do tipo abdominal/tronco vs. outra anatomia.

### 4.2. Justificativa Científica: Calibração de Phantom e Big Data
Historicamente, o QCT clássico requer o uso simultâneo de *phantoms* de calibração hidroxiapatita para compensar a variação de atenuação (drift) de cada tomógrafo individual. 

A abordagem normativa com *Big Data* (nível de metadados extraídos de > 50.000 exames retrospectivos) suprime o ruído do equipamento (*White Noise*) diluindo matematicamente as descalibrações diárias e desgastes pontuais de tubos de raios-X em uma curva de distribuição Gaussiana perfeita por faixa etária e sexo.

Neste estudo, a omissão de *phantom* físico se torna metodologicamente justificável pois:
1. **Contenção Estatística:** A extração das *tags DICOM* técnicas (`Manufacturer` e `KVP`) permitirá a comprovação retrospectiva — por Análise de Variância (ANOVA) com múltiplas curvas de decaimento ósseo (Idade *vs.* HU-L1) —, documentando a ausência de viés sistemático entre fabricantes ou modelos na macro-escala.
2. **Homogeneização:** Regressões isolando varreduras de `120 kVp` isolam a principal variável física que altera o HU do cálcio isoladamente (o kVp), estabilizando a comparação clínica do screening oportunístico real de abdômen/tórax, onde essa dosagem já é amplamente dominante.

---

## 5. Métodos de Medição

### 5.1 Método A — HU Volumétrico 3D (TotalSegmentator)
- Segmentação automática do corpo de L1 com TotalSegmentator
- Máscara binária aplicada ao volume CT original
- Estatísticas: HU médio, mediana, desvio padrão, percentil 25/75
- Inclui córtex e trabeculado → esperado HU sistematicamente mais alto

### 5.2 Método B — ROI Oval Axial Anterior (Pickhardt)
- Fatia axial na altura do meio do corpo vertebral (midpoint)
- Oval inscrito em ~50% da largura e ~33% da profundidade ântero-posterior
- Posicionamento automático baseado nos limites da máscara L1
- Referência: Pickhardt PJ et al., *AJR* 2013; Jang S et al., *Radiology* 2019

### 5.3 Método C — ROI Oval Sagital
- Reconstrução sagital paramédica da máscara L1
- Oval posicionado no trabeculado central (excluindo córtex superior, inferior e posterior)
- Dimensões proporcionais à altura e largura sagital do corpo vertebral
- Metodologia inspirada na visualização clínica utilizada por radiologistas

```
Pipeline:
CT DICOM → TotalSegmentator → Máscara L1 (NIfTI)
                                     ↓
                    ┌────────────────────────────────┐
                    │  A: HU volumétrico (todos voxels) │
                    │  B: Oval axial anterior           │
                    │  C: Oval sagital central          │
                    └────────────────────────────────┘
                                     ↓
                    Comparação estatística entre A, B, C
```

---

## 6. Análise Estatística

| Análise | Finalidade |
|---|---|
| Correlação de Pearson (r) | Relação linear entre métodos |
| ICC (modelo 2,1) | Concordância absoluta inter-método |
| Bland-Altman | Viés sistemático e limites de concordância |
| ROC | Limiar ótimo por método (osteopenia/osteoporose) |
| Regressão múltipla | Ajuste por sexo, idade, contraste, IMC |
| Kappa ponderado | Concordância na classificação categórica (normal / osteopenia / osteoporose) |

| Kappa ponderado | Concordância na classificação categórica (normal / osteopenia / osteoporose) |

**Software:** Python (scipy, pingouin, statsmodels), matplotlib/seaborn para visualizações.
**Variáveis para Regressão Multivariada:** Idade, Sexo, Fabricante (Vendor), kVp, e Espessura do Corte.

---

## 7. Resultados Esperados e Hipóteses Diretivas

| Hipótese | Expectativa |
|---|---|
| HU volumétrico > HU oval axial | Sim — inclusão do córtex eleva a média |
| B e C concordantes (ICC > 0,90) | Provável — ambos medem trabeculado central |
| A classifica mais casos como normais | Sim — viés para cima pelo córtex |
| Diferença A-B ~ 21 HU | Hipótese baseada em Jang 2019 |

---

## 8. Implementação no Heimdallr

### Módulos a implementar/adaptar

| Módulo | Arquivo | Status |
|---|---|---|
| Extração de Metadados DICOM (30 tags Técnicas) | `process_zipped_dicom_dataset.py` | ✅ Concluído |
| Segmentação L1 volumétrica | `metrics/bmd.py` | ✅ TotalSegmentator ativo |
| ROI oval axial anterior | `metrics/bmd.py` | ✅ Concluído (algoritmo implementado) |
| HU volumétrico médio 3D | `metrics/bmd.py` | 🔲 A implementar |
| ROI oval sagital | `metrics/bmd.py` | 🔲 A implementar |
| Script de consolidação CSV (Metadados + HU) | `scripts/bmd_roi_comparison.py` | 🔲 A criar |
| Script de análise estatística e ROC/Bland-Altman | `scripts/bmd_roi_comparison.py` | 🔲 A criar |

### Estrutura de saída esperada

```
output/
└── bmd_comparison/
    ├── results_per_patient.csv
    ├── bland_altman_A_vs_B.png
    ├── bland_altman_A_vs_C.png
    ├── bland_altman_B_vs_C.png
    ├── roc_method_A.png
    ├── roc_method_B.png
    ├── roc_method_C.png
    ├── correlation_matrix.png
    └── report_summary.md
```

---

## 9. Cronograma Estimado

| Fase | Atividade | Prazo estimado |
|---|---|---|
| 1 | Implementar HU volumétrico 3D e ROI sagital | 1–2 semanas |
| 2 | Rodar pipeline em toda a base de dados disponível | 1 semana |
| 3 | Análise estatística e geração de figuras | 1–2 semanas |
| 4 | Redação do manuscrito | 3–4 semanas |
| 5 | Revisão e submissão | 2 semanas |

**Total estimado:** 8–11 semanas

---

## 10. Periódicos-Alvo

| Periódico | Fator de Impacto | Justificativa |
|---|---|---|
| *Radiology* | ~12 | Referência no tema (Jang 2019 publicado aqui) |
| *European Radiology* | ~7 | Alta relevância para métodos automáticos |
| *Radiology: Artificial Intelligence* | ~8 | Foco em IA/automação em radiologia |
| *Osteoporosis International* | ~5 | Público especializado em osteoporose |
| *Scientific Reports* | ~4 | Open access, pipeline technique paper |

---

## 11. Referências Principais

1. Jang S, Graffy PM, Ziemlewicz TJ, Lee SJ, Summers RM, Pickhardt PJ. *Opportunistic Osteoporosis Screening at Routine Abdominal and Thoracic CT.* Radiology. 2019;291(2):360–367.
2. Pickhardt PJ, Lee SJ, Liu J, et al. *Population-based opportunistic osteoporosis screening: validation of a fully automated CT tool for assessing longitudinal BMD changes.* Br J Radiol. 2019.
3. Pickhardt PJ, Pooler BD, Bhalla S, et al. *Opportunistic Screening for Osteoporosis Using the Trabecular Attenuation Value of L1 on CT Colonography.* AJR. 2013.
4. Wasserthal J, Breit HC, Meyer MT, Pradella M, Hinck D, Sauter AW, Heye T, Boll DT, Cyriac J, Yang S, Bach M, Segeroth M. *TotalSegmentator: Robust Segmentation of 104 Anatomical Structures in CT Images.* Radiology: Artificial Intelligence. 2023;5(5):e230024. doi:10.1148/ryai.230024. (Ferramenta utilizada para a segmentação volumétrica 3D primária e inferência de fase de contraste PI-Time).

---

## 12. Notas e Decisões em Aberto

- [ ] Definir se DXA está disponível para subgrupo de validação externa
- [ ] Confirmar critério de exclusão para contraste IV (incluir ou estratificar?)
- [ ] Avaliar inclusão de L2 e L3 para aumentar robustez
- [ ] Verificar aprovação ética (CEP) para uso retrospectivo dos dados

---

*Documento criado em: 15/03/2026*  
*Próxima revisão: após implementação dos Métodos A e C (volumétrico e sagital)*
