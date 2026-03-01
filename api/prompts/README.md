# Prompts Versioning

All prompts in this directory must be versioned before any modification.

## Versioning Rule

Before editing a prompt, copy the current file into `versions/` using the following naming pattern:

```text
versions/file_name_YYYYMMDDHHmm.txt
```

| Field | Format | Example |
|---|---|---|
| `YYYY` | 4-digit year | 2026 |
| `MM` | 2-digit month | 02 |
| `DD` | 2-digit day | 24 |
| `HH` | 2-digit hour (24h) | 10 |
| `mm` | 2-digit minute | 51 |

### Example

When editing `ap_rx_thorax_openai.txt` on February 24, 2026 at 10:51:

```bash
cp ap_rx_thorax_openai.txt versions/ap_rx_thorax_openai_202602241051.txt
```

The previous revision remains preserved in `versions/`, and the filename without a suffix is always the current active version.
