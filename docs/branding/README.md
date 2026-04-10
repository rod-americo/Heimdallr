# Heimdallr Branding Assets

This directory stores the imported Heimdallr visual identity package in a
repository-friendly structure.

## Brandbook Summary

- Core concepts: control, technology, and stability.
- Symbol: structural arches that evoke a portal and continuous watchfulness,
  aligned with the guardian archetype and the idea of trusted radiology
  infrastructure.
- Visual tone: cold green and restrained neutral tones to communicate
  operational control, technical precision, and system reliability.
- Typography:
  - `Exo 2` for high-emphasis brand expression.
  - `Montserrat` for complementary supporting typography.

## Color Palette

- `#FFFFFF`
- `#486864`
- `#606060`
- `#9D9D9C`
- `#EDEDED`

## Imported Structure

- [`brandbook/`](./brandbook/)
  - Brandbook PDF delivery.
- [`fonts/`](./fonts/)
  - Original upstream font archives retained as delivered, renamed in
    `snake_case` only at the archive level.
- [`mockups/`](./mockups/)
  - Presentation and application mockups.
- [`vectors/`](./vectors/)
  - Vector logo delivery in PDF form.

Reusable branding assets now live under [`static/branding/`](../../static/branding/):

- `logos/` for PNG logo variants.
- `watermarks/` for transparent PNG watermark variants.
- `vectors/` for page-by-page SVG extractions derived from the vendor PDF
  vector delivery.

## Notes

- The received package did not include raw editable source files such as
  `.ai`, `.svg`, or `.eps`.
- The vector delivery currently available in-repo is
  [`heimdallr_logo_vector_variants.pdf`](./vectors/heimdallr_logo_vector_variants.pdf).
- The 20 extracted SVG files under [`../../static/branding/vectors/`](../../static/branding/vectors/)
  were generated from that PDF with `pdftocairo -svg`.
