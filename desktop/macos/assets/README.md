# macOS Icon Assets

Generated from:

- app icon source: `docs/branding/vectors/heimdallr_icon_variant_01.svg`
- menu bar source: `docs/branding/vectors/heimdallr_icon_variant_01_black.svg`

Primary outputs:

- `Heimdallr.icns`: app icon for local bundle tests.
- `AppIcon.iconset/`: macOS iconset used to regenerate `Heimdallr.icns`.
- `MenuBarIcon.imageset/`: black template image set for the menu bar.
- `renders/HeimdallrAppIcon_1024.png`: high-resolution rendered preview.

The app icon places the colored vector mark on a rounded macOS-style base so it
remains visible in the Dock. The menu icon stays black with transparency so the
Swift app can use it as a template image.

## Menu Bar Baseline

The accepted menu bar icon baseline is a direct rasterization of
`heimdallr_icon_variant_01_black.svg`. Do not redraw the mark by hand in Swift
or with approximate CoreGraphics paths; the SVG is the source of truth for
geometry and proportions.

Use this sizing contract:

| Asset | Canvas | SVG render size | Purpose |
| --- | --- | --- | --- |
| `heimdallr_menu_template.png` | 18 x 18 px | 16 x 16 px | 1x status item image. |
| `heimdallr_menu_template@2x.png` | 36 x 36 px | 32 x 32 px | Retina status item image. |
| `heimdallr_menu_template@3x.png` | 54 x 54 px | 48 x 48 px | High-density status item image. |

The one-pixel logical padding keeps the thin mark from clipping while matching
the visual weight of neighboring macOS menu bar icons. The image must remain
black with transparency and must be used as a template image.

Reference generation commands:

```bash
magick -background none -density 1200 \
  docs/branding/vectors/heimdallr_icon_variant_01_black.svg \
  -resize 16x16 -gravity center -extent 18x18 \
  PNG32:desktop/macos/assets/MenuBarIcon.imageset/heimdallr_menu_template.png

magick -background none -density 1200 \
  docs/branding/vectors/heimdallr_icon_variant_01_black.svg \
  -resize 32x32 -gravity center -extent 36x36 \
  PNG32:desktop/macos/assets/MenuBarIcon.imageset/heimdallr_menu_template@2x.png

magick -background none -density 1200 \
  docs/branding/vectors/heimdallr_icon_variant_01_black.svg \
  -resize 48x48 -gravity center -extent 54x54 \
  PNG32:desktop/macos/assets/MenuBarIcon.imageset/heimdallr_menu_template@3x.png
```

After regenerating, verify that the 1x image has meaningful alpha coverage. A
blank or nearly blank menu icon usually means the rasterization collapsed the
thin SVG strokes.

```bash
magick desktop/macos/assets/MenuBarIcon.imageset/heimdallr_menu_template.png \
  -alpha extract txt:- | awk -F'[(),]' '$4>0{n++} END{print n+0}'
```

The accepted 1x baseline should report approximately `157` non-transparent
pixels.
