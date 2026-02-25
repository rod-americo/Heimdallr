# Prompts — Versionamento

Todos os prompts neste diretório devem ser versionados antes de qualquer modificação.

## Regra de versionamento

Antes de editar um prompt, copiar o arquivo atual para a pasta `versions/` com o seguinte padrão de nome:

```
versions/nome_do_arquivo_AAAAMMDDHHmm.txt
```

| Campo | Formato | Exemplo |
|---|---|---|
| `AAAA` | Ano com 4 dígitos | 2026 |
| `MM` | Mês com 2 dígitos | 02 |
| `DD` | Dia com 2 dígitos | 24 |
| `HH` | Hora com 2 dígitos (24h) | 10 |
| `mm` | Minuto com 2 dígitos | 51 |

### Exemplo

Ao editar `ap_rx_thorax_openai.txt` em 24/02/2026 às 10:51:

```
cp ap_rx_thorax_openai.txt versions/ap_rx_thorax_openai_202602241051.txt
```

A versão anterior fica preservada em `versions/` e o arquivo sem sufixo é sempre a versão **corrente**.
