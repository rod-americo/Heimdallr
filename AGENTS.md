# Heimdallr Agentic Guidelines

Este repositório é manipulado de forma frequente e autônoma por Agentes de IA e ferramentas de AI-Coding (Gemini, Claude, Cursor, Copilot, etc.). Para conter alucinações arquiteturais e preservar a engenharia empregada, **leia e siga religiosamente estas regras antes de realizar qualquer modificação no código.**

> [!CAUTION]
> Ao longo da história do projeto, este repositório sofreu diversas divisões e purgas. Sob nenhuma circunstância reconstrua serviços antigos que foram apagados ou realocados!

## 1. Escopo de Domínio e Divisão de Repositórios

O `Heimdallr` é estrita e exclusivamente focado na **infraestrutura open-source de MLOps de Imagem Radiológica**. Ele é a fundação para a escuta (C-STORE), processamento de pipelines de formato (DICOM → NIfTI), cálculos determinísticos de volumes orgânicos, processamento TotalSegmentator, além de interfaces métricas de banco.

**O que NÃO Pertence ao Heimdallr:**
*   Serviços proprietários de suporte clínico e laudos redigidos;
*   Engenharia de Prompting para LLMs (OpenAI, Anthropic, MedGemma);
*   Camadas de inteligência ou conversão assistida avançada de imagens para essas APIs.

**Agentes:** Caso você seja instruído a lidar com LLMs, NLP ou rotinas "inteligentes" para os laudos finais, **PARE**. O domínio dessas atividades pertence universalmente ao repositório cliente arquitetado como **`Asha`**.

## 2. Paradigmas Cloud-Native / 12-Factor App

*   **Zero `.env` files**: Nunca crie, espere encontrar ou instale dependências atreladas ao `python-dotenv`. A arquitetura foi refatorada sob a óptica cloud-native. Valores sensíveis (como `TOTALSEGMENTATOR_LICENSE`) serão lidos pelo `os.getenv` através de injeção externa do Sistema Hospedeiro, Launchd/Systemd ou Docker, e **nunca** de um artifício `.env`.
*   **Importação Limpa e Json Settings**: Nunca recrie nem busque os legados `app.py`, `run.py` ou `config.py` soltos na raiz. Toda base de configurações nativa e compartilhamentos da biblioteca reside sob os modulares dentro do pacote `heimdallr/` ou nos arquivos JSON em `config/`.

## 3. Diretrizes de Commits

Sempre formule mensagens de commit na linguagem `EN-US` e valendo as marcações semânticas restritas: *(uso no modo imperativo).*
Formato: `type(scope): summary`

**Tipos Reconhecidos (Allowed Types):**
*   `feat`: Funcionalidades estritas
*   `fix`: Quebras ou comportamentos imprevisíveis
*   `docs`: Exclusivo para material na raiz (Markdown)
*   `refactor`: Ajuste limpo interno (arquitetural)
*   `test`: Verificações e scripts isolados de provas  
*   `chore`: Atualizações de versão ou ambiente em `requirements/`
*   *(perf, ci, build, revert).*

> [!WARNING]
> Mantenha os Subjects limitados a *72 caracteres* e em hipótese remota anexe informações PHI/PII nos commits do Git.

## 4. Manipulação de Diretórios
*   **Limpeza da Raiz**: O ecossistema está polido com as normas da comunidade em `.github` (Contributes, Owners, Securities).
*   Não recrie rastros transitórios como `.tmp`, `.pycache_local` ou pastas de filas em disco soltas na raiz (ex: `/output/`, `/data/`). Pastas automáticas devem estar devidamente silenciadas no `.gitignore`.
