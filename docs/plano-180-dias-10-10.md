# Plano 180 Dias para Nota 10/10

## Objetivo

Elevar o projeto Heimdallr de maturidade atual para padrão de diligência institucional, com evidência auditável em segurança, contratos técnicos, operabilidade, governança clínica e qualidade de engenharia.

## Princípios de execução

1. Sem evidência, não conta.
2. Mudança clínica sem gate formal é proibida.
3. Narrativa pública só pode refletir capacidade comprovada.
4. Cada fase fecha com critérios de aceite mensuráveis.

## Métricas norte (meta em 180 dias)

- Disponibilidade API (`/docs` + endpoints críticos): >= 99.5% em janela mensal.
- Erro 5xx em endpoints críticos: < 1.0% mensal.
- P95 latência endpoints críticos: limite definido por endpoint e monitorado.
- Cobertura de testes (núcleo API + ingest + deid): >= 80%.
- Change failure rate: < 10%.
- MTTR incidentes SEV-1/2: alvo definido e reportado mensalmente.
- 100% de releases com rollback validado.
- 0 segredos expostos em branch protegida.
- 100% de módulos com pacote de validação quando marcados `validation-ready` ou acima.

## Stage 1 (Dias 0-30): Contenção de risco e baseline obrigatório

### Entregáveis

1. Higiene de segredos
- Rotacionar todas as chaves ativas.
- Remover segredos do histórico Git.
- Adotar secret scanning bloqueante em CI e pre-commit.

2. Hardening de acesso
- Exigir autenticação no `/upload` (token validado no servidor ou mTLS via gateway).
- Documentar arquitetura de acesso (origem confiável, rede, proxy, identidade).

3. Contrato DB consistente
- Criar migração versionada para colunas `Weight`, `Height`, `SMI`.
- Alinhar `database/schema.sql`, código e docs.
- Teste automático de migração em ambiente limpo e legado.

4. Erros e observabilidade mínima
- Padronizar respostas de erro (`code`, `message`, `trace_id`).
- Remover detalhes de exceção crua de respostas públicas.
- Incluir correlação por `trace_id` nos logs.

### Critérios de aceite

- Nenhum secret válido presente no repositório/histórico principal.
- `/upload` rejeita chamada sem credencial.
- Migração executa sem erro em base nova e existente.
- 100% dos erros HTTP seguem envelope padronizado.

## Stage 2 (Dias 31-60): Contratos e quality gates de engenharia

### Entregáveis

1. API governance
- Publicar versão explícita (`/api/v1`).
- Política de compatibilidade/depreciação em `docs/API.md`.
- Catálogo de erros por endpoint.

2. CI bloqueante real
- Rodar testes unitários e integração.
- Lint/format/check estático.
- Dependency audit e SAST básico.
- Gate de cobertura mínima.

3. Correção de artefatos desatualizados
- Atualizar testes para endpoints reais.
- Eliminar exemplos/documentos que não batem com implementação atual.

### Critérios de aceite

- PR sem testes/lint/security não mergeia.
- Todos endpoints documentados respondem conforme contrato.
- Teste de integração legado inconsistente removido ou corrigido.

## Stage 3 (Dias 61-90): Operabilidade de produção

### Entregáveis

1. SRE baseline
- Definir SLI/SLO por serviço crítico.
- Dashboards e alertas com runbook vinculado.
- Health checks operacionais documentados.

2. Incidente e rollback
- Runbook SEV-1/2/3 com owner e rito de comunicação.
- Simulação de incidente com postmortem.
- Rollback testado por release.

3. Backup e restore comprovados
- Rotina de backup com retenção.
- Restore testado periodicamente com evidência.

### Critérios de aceite

- Alertas críticos acionam e são tratáveis por runbook.
- Pelo menos 1 game day executado com evidência.
- Restore validado dentro de RTO/RPO definidos.

## Stage 4 (Dias 91-120): Governança clínica e validação auditável

### Entregáveis

1. Pacote de validação por módulo
- Estruturar `docs/validation-evidence/<modulo>/<data>/`.
- Incluir `protocol.md`, `run-log.md`, `metrics`, `failure-cases`, `rollback-test`, `decision`.

2. Change control clínico
- Template obrigatório para mudanças com impacto clínico.
- Aprovação explícita de responsável técnico.
- Registro de contraindicações e limitações por módulo.

3. Matriz de risco clínico-operacional
- Risco por módulo (segurança, eficácia, operabilidade, privacidade).
- Gate de promoção para `validation-ready` e `production-candidate`.

### Critérios de aceite

- 100% dos módulos promovidos têm dossiê completo.
- Mudança clínica sem aprovação bloqueada por processo.
- Matriz de risco atualizada em toda release relevante.

## Stage 5 (Dias 121-150): Transparência institucional e release discipline

### Entregáveis

1. Trust Center do projeto
- Página consolidando segurança, disponibilidade, validação e políticas.
- Declaração clara de escopo clínico suportado vs roadmap.

2. Release management
- SemVer com tags e release notes por risco.
- Changelog por versão, não apenas `Unreleased`.
- Matriz de compatibilidade (API, schema, serviços).

3. Governança de documentação
- Dono por documento crítico.
- SLA de atualização documental.
- Verificação automática de links e consistência entre docs.

### Critérios de aceite

- Toda release tem notas completas e plano de rollback.
- Trust Center atualizado e coerente com evidências internas.
- Drift de documentação detectado automaticamente.

## Stage 6 (Dias 151-180): Consolidação 10/10 e auditoria externa

### Entregáveis

1. Auditoria técnica simulada (pré-diligence)
- Rodar checklist completo de diligência.
- Registrar gaps residuais com plano e prazo.

2. Evidência de estabilidade operacional
- Três ciclos mensais com KPIs dentro de meta.
- Relatório executivo com tendência e ações corretivas.

3. Política de melhoria contínua
- Cadência fixa de revisão de risco, segurança e contratos.
- Backlog de dívida técnica priorizado por impacto clínico-operacional.

### Critérios de aceite

- >= 90% do checklist de diligência aprovado sem ressalva crítica.
- KPIs-chave estabilizados por 90 dias.
- Nenhum risco crítico aberto sem owner e prazo.

## RACI mínimo

- Owner técnico (Head/CTO): decisão de arquitetura, segurança e release.
- Owner clínico: validação assistiva, limites de uso e contraindicações.
- Owner de operações: SLO, incidentes, backup/restore, observabilidade.
- Owner de documentação/governança: consistência editorial e evidências.

## Cadência de governança

- Semanal: status de entregáveis e riscos.
- Quinzenal: revisão de métricas e incidentes.
- Mensal: checkpoint executivo (go/no-go de estágio).
- Trimestral: auditoria interna completa.

## Checklist final para considerar 10/10

1. Segurança: segredos, acesso, trilha de auditoria, resposta a vulnerabilidades.
2. Contratos: API versionada, erros padronizados, compatibilidade explícita.
3. Engenharia: CI bloqueante, cobertura adequada, release disciplinado.
4. Operações: SLO ativo, incidentes com postmortem, rollback e restore testados.
5. Clínica: evidência validada por módulo, governança de mudança, limites claros.
6. Narrativa: comunicação pública fiel ao que está implementado e comprovado.
