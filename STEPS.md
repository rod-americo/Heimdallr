# Passo a Passo do Pipeline de Sarcopenia

Este documento descreve o fluxo de execução dos scripts `preparar_volume.py` e `run.py`.

## 1. Preparação do Volume (`preparar_volume.py`)

O objetivo deste script é pegar arquivos DICOM brutos e convertê-los em um único arquivo NIfTI (`ct_original.nii.gz`) adequado para análise.

1.  **Extração**:
    -   O script varre a pasta `dcm/`.
    -   Se encontrar arquivos `.zip`, eles são extraídos.
    -   Arquivos DICOM soltos são copiados para uma pasta temporária.
2.  **Análise de Séries**:
    -   O script agrupa todos os arquivos DICOM pelo `SeriesInstanceUID`.
    -   Identifica parâmetros como Modalidade (CT), Espessura do Corte e Kernel de Convolução.
3.  **Seleção da Melhor Série**:
    -   Um sistema de pontuação escolhe a melhor série de Tomografia (CT):
        -   Prioriza séries com mais fatias (maior cobertura).
        -   Prioriza espessuras entre 0.3mm e 7mm.
        -   Penaliza kernels de reconstrução óssea ou pulmonar (para focar em tecidos moles).
4.  **Conversão (dcm2niix)**:
    -   Usa a ferramenta externa `dcm2niix` para converter a série escolhida para NIfTI.
    -   O arquivo final é salvo como `ct_original.nii.gz` na raiz do projeto.

---

## 2. Segmentação e Análise (`run.py`)

O objetivo deste script é segmentar tecidos e calcular métricas de sarcopenia em L3.

1.  **Configuração**:
    -   Define caminhos de entrada (`ct_original.nii.gz`) e saída (`output/`).
2.  **Segmentação (TotalSegmentator)**:
    -   **Passo 1 (Estrutural)**: Executa `TotalSegmentator` na tarefa `total`. Isso segmenta ossos e órgãos. O objetivo principal aqui é obter a segmentação das vértebras para localizar L3.
    -   **Passo 2 (Tecidos)**: Executa `TotalSegmentator` na tarefa `tissue_types`. Isso segmento músculo esquelético, gordura subcutânea e gordura visceral.
3.  **Localização de L3**:
    -   Carrega a máscara da vértebra L3 (`output/total/vertebrae_L3.nii.gz`).
    -   Identifica o índice da fatia (slice) que corresponde ao centro da vértebra L3.
4.  **Cálculos em L3**:
    -   Na fatia identificada, carrega a máscara de músculo (`output/tissue_types/skeletal_muscle.nii.gz`).
    -   **Área Muscular (SMA)**: Conta os pixels de músculo * tamanho do pixel. Converte para cm².
    -   **Radiodensidade (HU)**: Usa a imagem CT original para medir a densidade média (Hounsfield Units) dos pixels dentro da máscara muscular.
5.  **Exportação**:
    -   Salva os resultados em `output/resultados.json`.

## Como Executar

```bash
# 1. Ative o ambiente virtual
source ./venv/bin/activate

# 2. Prepare o volume (converte DICOM -> NIfTI)
python preparar_volume.py

# 3. Execute a análise
python run.py
```

## Arquivos Gerados

-   `ct_original.nii.gz`: Volume CT convertido.
-   `output/total/`: Segmentações de ossos/órgãos.
-   `output/tissue_types/`: Segmentações de tecidos (músculo, gordura).
-   `output/resultados.json`: Métricas finais (SMA, HU, nível L3).
