# Classificação de Módulos: **HEIMDALLR** vs **ASHA**

Resumo: 38 módulos classificados como **ASHA** e 18 como **HEIMDALLR**.

## 1. DICOM C-STORE Intake Listener
Classificação: ****HEIMDALLR****
Confiança: alta
Justificativa: Endpoint de ingestao DICOM e conectividade de entrada. E infraestrutura reutilizavel, sem inteligencia clinica proprietaria.

## 2. Case Prep and Queue Worker
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: Backbone de fila e processamento assincrono. E orquestracao operacional generica da plataforma.

## 3. Dashboard + API Surface
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: API e visibilidade operacional sao superficie de integracao. Nao carregam conhecimento clinico diferenciado.

## 4. HL7-Triggered Smart Prefetch
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: Consome eventos HL7 e faz prefetch de exames. E conectividade e orquestracao entre sistemas.

## 5. Unified Worklist Orchestrator
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: Orquestra distribuicao de trabalho e fairness. E infraestrutura de workflow generica para qualquer operacao.

## 6. SLA Policy Engine
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: Monitora risco de SLA e aciona escalonamento operacional. Nao depende de interpretacao clinica.

## 7. AP Chest X-ray Assist APIs
Classificação: **ASHA**
Confiança: alta
Justificativa: API assistiva para fluxo de laudo em RX de torax. Entrega valor clinico e depende de inteligencia aplicada ao exame.

## 8. TotalSegmentator Core Pipeline
Classificação: **ASHA**
Confiança: alta
Justificativa: Segmentacao produz saida estruturada clinica. E base de quantificacao e diferencial assistivo.

## 9. L3 Muscle Area + SMI Calculator
Classificação: **ASHA**
Confiança: alta
Justificativa: Gera metrica clinica estruturada de composicao corporal. Tem utilidade diagnostica e analitica direta.

## 10. Automated Organ Volumetry
Classificação: **ASHA**
Confiança: alta
Justificativa: Volumetria de orgaos e inteligencia quantitativa clinica. Produz dados estruturados com valor assistencial.

## 11. Intracranial Hemorrhage Detection
Classificação: **ASHA**
Confiança: alta
Justificativa: Detector de hemorragia intracraniana e interpretacao clinica explicita. Tem alto valor assistivo e diferencial.

## 12. Structured Report Copilot
Classificação: **ASHA**
Confiança: alta
Justificativa: Copiloto de laudo usa IA e NLP para gerar conteudo clinico. Esta diretamente no nucleo de valor clinico.

## 13. Urgency Flagging and Reordering
Classificação: **ASHA**
Confiança: media
Justificativa: Embora afete workflow, a repriorizacao depende de sinais de detectores e regras clinicas. O valor diferenciado vem da inteligencia de triagem.

## 14. Opportunistic Liver Quant
Classificação: **ASHA**
Confiança: alta
Justificativa: Quantifica figado e esteatose a partir de CT. E saida clinica estruturada e monetizavel.

## 15. Opportunistic Bone Quant
Classificação: **ASHA**
Confiança: alta
Justificativa: Quantificacao oportunistica para osteoporose e inteligencia clinica aplicada. Produz sinal estruturado de suporte.

## 16. Opportunistic Coronary Calcium (CAC-DRS)
Classificação: **ASHA**
Confiança: alta
Justificativa: Deteccao e quantificacao CAC-DRS e claramente clinica. Representa analise especializada e potencial de receita.

## 17. Hippocampal Volumetry Project
Classificação: **ASHA**
Confiança: alta
Justificativa: Volumetria longitudinal com foco neurodegenerativo. E conhecimento clinico especializado.

## 18. Opportunistic Emphysema Quant
Classificação: **ASHA**
Confiança: alta
Justificativa: Mede burden de enfisema a partir de imagem. Produz valor clinico estruturado.

## 19. Agentic Workflow Coordinator
Classificação: **ASHA**
Confiança: media
Justificativa: Apesar do nome de orquestracao, o modulo e voltado a triagem, laudo e handoff com agentes. O nucleo do valor e IA clinica e operacional proprietaria.

## 20. Foundation Model Fine-Tuning Layer
Classificação: **ASHA**
Confiança: alta
Justificativa: Fine-tuning institucional de VLM e LLM e inteligencia proprietaria. E claramente diferencial competitivo.

## 21. Temporal Imaging Intelligence (Delta Engine)
Classificação: **ASHA**
Confiança: alta
Justificativa: Faz analise longitudinal e suporte interpretativo. E inteligencia semantica e clinica.

## 22. Causal Triage Simulator
Classificação: **HEIMDALLR**
Confiança: media
Justificativa: Simula politicas antes de producao para SLA, fairness e latencia. E camada operacional e de governanca de workflow, nao interpretacao clinica.

## 23. Synthetic + Federated Validation Sandbox
Classificação: **HEIMDALLR**
Confiança: media
Justificativa: Ambiente de validacao federada e benchmarking e infraestrutura de governanca. Suporta a plataforma inteira, nao so um produto clinico.

## 24. Autonomous Follow-up Orchestrator (Human-Gated)
Classificação: **ASHA**
Confiança: media
Justificativa: Orquestra follow-up orientado por inteligencia e regras clinicas. O valor esta na decisao e no encaminhamento diferenciado.

## 25. Prospective Trial Mode
Classificação: **HEIMDALLR**
Confiança: media
Justificativa: Modo de captura e reprodutibilidade para avaliacao e infraestrutura de evidencia e governanca. Pode ser reutilizado genericamente.

## 26. De-identification Gateway
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: Desidentificacao e controle de PHI sao seguranca e plataforma. E componente generico e nao clinico.

## 27. Patient Follow-up Navigator
Classificação: **ASHA**
Confiança: media
Justificativa: Navegacao de follow-up entrega valor operacional diferenciado sobre eventos clinicos. Esta mais perto do produto assistencial do que da infra generica.

## 28. Urology Navigation Module
Classificação: **ASHA**
Confiança: alta
Justificativa: Roteamento por achados urologicos incorpora logica clinica. E modulo vertical de valor assistencial.

## 29. Lung Nodule Longitudinal Tracker
Classificação: **ASHA**
Confiança: alta
Justificativa: Tracking de nodulo com growth e follow-up e inteligencia clinica longitudinal. Produz suporte a decisao.

## 30. Aortic Aneurysm Surveillance Pipeline
Classificação: **ASHA**
Confiança: alta
Justificativa: Vigilancia com regras alinhadas a guideline e claramente clinica. O valor esta na interpretacao e escalonamento.

## 31. Kidney Stone Burden Longitudinal Module
Classificação: **ASHA**
Confiança: alta
Justificativa: Acompanha burden longitudinal e follow-up de calculo renal. E produto clinico e operacional especializado.

## 32. Incidental Findings Closure Engine
Classificação: **ASHA**
Confiança: media
Justificativa: Faz closure de achados incidentais e captura valor operacional diferenciado. Esta acima da infraestrutura bruta.

## 33. Prostate MRI Longitudinal PI-RADS Tracker
Classificação: **ASHA**
Confiança: alta
Justificativa: PI-RADS e tendencia temporal sao conteudo clinico especializado. E diferencial claro.

## 34. General Surgery Navigation Module
Classificação: **ASHA**
Confiança: media
Justificativa: Roteamento por achados cirurgicos comuns usa contexto clinico. E um vertical de navegacao, nao uma camada generica.

## 35. Oncology High-Suspicion Navigation Router
Classificação: **ASHA**
Confiança: alta
Justificativa: Encaminhamento oncologico por suspeicao e inteligencia clinica e operacional de alto valor. Tem forte potencial competitivo.

## 36. Gynecology Navigation Module
Classificação: **ASHA**
Confiança: alta
Justificativa: Estrutura follow-up por achados ginecologicos suspeitos. E modulo clinico vertical.

## 37. Fracture Detection and Triage Module
Classificação: **ASHA**
Confiança: alta
Justificativa: Detector especifico de fratura com triagem e interpretacao clinica. O componente de valor esta no modelo e no sinal clinico.

## 38. Bandwidth-Aware Transfer Scheduling
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: Scheduling de transferencia por banda e infraestrutura de transporte. Totalmente generico e sem conteudo clinico.

## 39. Deterministic Pseudonymization + Crosswalk
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: Pseudonimizacao e linkage entre sistemas sao governanca e seguranca. E infraestrutura transversal.

## 40. DICOMweb-Native Transport Layer
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: Camada de transporte e interoperabilidade DICOMweb e claramente conectividade. Deve ficar no componente open source.

## 41. Follow-up Recommendation Extraction
Classificação: **ASHA**
Confiança: alta
Justificativa: Extracao NLP de recomendacoes e analise semantica sobre texto clinico. Produz dado estruturado e valor diferenciado.

## 42. On-prem AI Policy Enforcement
Classificação: **HEIMDALLR**
Confiança: media
Justificativa: Apesar de mencionar AI, o foco e enforcement, outbound controls e enterprise deployment. E governanca operacional de plataforma.

## 43. Pre-classification + Priority Flag (CXR)
Classificação: **ASHA**
Confiança: alta
Justificativa: Pre-classificacao de RX e flag de prioridade dependem de inteligencia sobre imagem clinica. E modulo assistivo.

## 44. Drift and Hallucination Control Framework
Classificação: **ASHA**
Confiança: media
Justificativa: E camada de controle especifica para modelos de laudo. Embora seja governanca, esta acoplada ao comportamento do produto de IA.

## 45. Bone Lesion CT Pipeline
Classificação: **ASHA**
Confiança: alta
Justificativa: Pipeline de lesao ossea e metastase e diretamente clinico. Envolve deteccao e valor assistencial diferenciado.

## 46. Radiology Triage Agent
Classificação: **ASHA**
Confiança: alta
Justificativa: Agente que combina detectores, contexto previo e regras e inteligencia clinica e operacional proprietaria. E nucleo competitivo.

## 47. Multi-Agent Report Orchestrator
Classificação: **ASHA**
Confiança: alta
Justificativa: Orquestracao multiagente para montagem de laudo e IA aplicada ao conteudo clinico. Nao e infraestrutura generica.

## 48. Agentic Report QA Gate
Classificação: **ASHA**
Confiança: media
Justificativa: Gate de QA e governanca, mas governanca do produto de laudo por IA. O escopo e especifico da camada inteligente.

## 49. Guideline-Cited Recommendation Agent
Classificação: **ASHA**
Confiança: alta
Justificativa: Gera recomendacoes clinicas com grounding em guideline. E claramente suporte a decisao.

## 50. Subtle-Finding Triage Safeguard
Classificação: **ASHA**
Confiança: media
Justificativa: Salvaguarda especifica para sistemas de triagem clinica. Mesmo sendo safety layer, esta acoplada ao comportamento da IA clinica.

## 51. Prospective Shadow-Mode Validator
Classificação: **HEIMDALLR**
Confiança: media
Justificativa: Shadow mode pre-producao e mecanismo de validacao operacional e rollout seguro. Pode servir genericamente a plataforma.

## 52. Agent Drift and Bias Sentinel
Classificação: **HEIMDALLR**
Confiança: media
Justificativa: Monitoramento continuo de drift e bias e uma capacidade de observabilidade e governanca. Mantive na plataforma por ser controle transversal.

## 53. CTR Extraction (ICT via CXAS)
Classificação: **ASHA**
Confiança: alta
Justificativa: Extracao de CTR e quantificacao clinica estruturada. Tem utilidade assistiva direta.

## 54. Renal Stone Burden Quantification
Classificação: **ASHA**
Confiança: alta
Justificativa: Quantifica burden de calculo renal com metricas clinicas uteis. E inteligencia de produto.

## 55. Segmentation Service API (HTTP)
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: API de servico e runtime HTTP sao infraestrutura de execucao. Mesmo servindo modelos clinicos, a camada em si e generica.

## 56. Retroactive Cohort Reprocessing Toolkit
Classificação: **HEIMDALLR**
Confiança: alta
Justificativa: Toolkit de reprocessamento e backfill e capacidade operacional. Serve validacao, correcao e rerun da plataforma.
