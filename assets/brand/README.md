# a0-plugin-durable brand assets

PROPOSAL: the owner signs off logos. Nothing here is final until then.

## Family construction (Patchbay)

All five a0 plugins share one build (`family-build.py`):

- 64 x 64 grid.
- Frame: rounded square, outer edge 4..60, outer corner radius 8, 6-unit stroke, no fill.
- Pin: 14 x 14 block in signal colour in the bottom-right corner (x 46..60, y 46..60), continuing the frame outline.
- Glyph box: 28 x 28 at x 16..44, y 16..44, 4-unit `ink` strokes, butt caps, mitre joins.
- Straight segments only; the signal colour is used only for the pin.

## This plugin's glyph

Three horizontal journal rows, the last one shorter. Rows are journaled steps; the short row is the first unfinished step, where replay stops and execution resumes. Rows at y 21, 30 and 39 from x 19; the third ends at x 31.

## Palette

| Token | Light | Dark |
|---|---|---|
| canvas | #F4F6F7 | #0C1116 |
| ink | #0F1720 | #E8EEF1 |
| ink-muted | #4A5663 | #9AA7B2 |
| signal (pin) | #00766E | #3FD3C4 |

Full token and contrast tables are in `DESIGN.md`.

## Files

`logo.svg`, `logo-dark.svg` (mark), `wordmark.svg`, `wordmark-dark.svg`, `social-card.svg`, `social-card-dark.svg`, PNG previews of each, `family-sheet.png` (all five marks at 16, 32 and 128 px).

## Rebuild

```bash
python3 assets/brand/family-build.py durable assets/brand
# then rasterise and optimise, for example:
resvg assets/brand/wordmark.svg assets/brand/wordmark.png
oxipng -o 4 assets/brand/*.png
```
