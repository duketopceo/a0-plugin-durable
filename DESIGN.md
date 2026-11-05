---
version: alpha
name: Patchbay (Agent Zero plugin family)
description: "A shared identity for the duketopceo Agent Zero plugins: cool paper and ink, one signal-teal accent, hairline rules, monospace labels, and a patch-point mark where each plugin is a distinct glyph seated in the same socket frame."
colors:
  paper: "#F4F6F7"
  surface: "#E6EBED"
  ink: "#0F1720"
  ink-muted: "#4A5663"
  hairline: "#C5CDD2"
  signal: "#00766E"
  on-signal: "#F4F6F7"
  warn: "#A33A00"
  night: "#0C1116"
  night-surface: "#161D24"
  night-ink: "#E8EEF1"
  night-ink-muted: "#9AA7B2"
  night-hairline: "#2A343D"
  night-signal: "#3FD3C4"
  on-night-signal: "#0C1116"
  night-warn: "#FF9F6B"
typography:
  display:   { fontFamily: "JetBrains Mono, ui-monospace, monospace", fontSize: 28px, fontWeight: 700, lineHeight: 1.15, letterSpacing: -0.01em }
  heading:   { fontFamily: "JetBrains Mono, ui-monospace, monospace", fontSize: 18px, fontWeight: 600, lineHeight: 1.3 }
  body:      { fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif", fontSize: 15px, fontWeight: 400, lineHeight: 1.55 }
  label:     { fontFamily: "JetBrains Mono, ui-monospace, monospace", fontSize: 12px, fontWeight: 500, lineHeight: 1.2, letterSpacing: 0.04em }
  code:      { fontFamily: "JetBrains Mono, ui-monospace, monospace", fontSize: 13px, fontWeight: 400, lineHeight: 1.5 }
rounded: { frame: 8px, control: 4px, chip: 2px }
spacing: { unit: 4px, panel-gap: 16px, row: 8px }
---

# Patchbay: family design system

Shared by `a0-plugin-durable`, `a0-plugin-glitchtip`, `a0-plugin-notion`, `a0-plugin-device-sync` and `a0-plugin-jev-compact`. This file is identical in all five repos above the per-plugin section at the bottom. Change it in all five or in none.

Status: proposal, written 2026-10-10. The owner signs off logos.

## 1. Direction

**Patchbay.** An Agent Zero plugin is a module that seats into a host: a `plugin.yaml`, a settings panel, an API route, an extension hook. A patchbay is the honest picture of that: labelled sockets, short cables, nothing decorative. The family looks like instrument-rack labelling. Cool paper, near-black ink, one teal signal colour that means "this is live or selected", monospace for anything a machine said, system sans for anything a person reads.

The memorable move: **every plugin is a 32-unit glyph seated in the same 64-unit socket frame, with one filled corner "pin"**. Five repos, one silhouette, five different insides.

## 2. Research, reference lock and decision ledger

Refero MCP was not connected on this machine (the separate design-research notes were not committed to this repo). Research ran on the local `awesome-design-md` analyses plus the bundled refero craft references, which is the fallback the skill allows.

References studied (all in `awesome-design-md/design-md/<name>/DESIGN.md`):

| Reference | Used for | Rejected from it |
|---|---|---|
| linear.app | surface ladder (canvas, surface, hairline), accent scarcity, tight type scale | lavender accent, near-black-only canvas, custom display face |
| hashicorp | one system holding several product identities | per-product accent colours (would break "reads as a set") |
| raycast | key-cap chips, command-palette density, hairline-only borders | gradient hero stripes, Inter ss03 |
| sentry | error-tool voice: plain status words, severity as text first | violet/lime palette, illustrated mascot style |
| warp | monospace-forward terminal framing | glossy gradient chrome |

Reference lock:

```text
Primary direction: Linear's surface ladder and accent scarcity, translated to a light-first palette with a dark twin.
Preserve: (1) one accent only; (2) 1px hairlines instead of shadows; (3) monospace labels; (4) flat fills, no gradients; (5) the same mark frame in every repo.
Borrow only: Raycast's key-cap chip for status tokens; HashiCorp's one-system-many-products idea, carried by glyph not colour.
Role rules: signal = live/selected/primary action only. warn = degraded/failed only. Never use signal as a decorative fill.
Media strategy: code-native SVG only. No raster, no stock, no generated imagery.
Reject: indigo/violet, glassmorphism, gradient text, cream+terracotta "calm editorial", drop-shadow cards, emoji as icons, decorative serif word swaps.
Token commitments: paper/ink/signal as in the front matter; 8px frame radius, 4px controls; hairline 1px.
```

Decision ledger:

| Decision | Source | Role to preserve | Why |
|---|---|---|---|
| Single teal accent for the whole family | linear.app accent scarcity; reject hashicorp per-product colour | live/selected only | five plugins must read as one set; colour is spent on state, not brand |
| Light-first with dark twin | refero default, overridden by a0's own dark UI | n/a | Agent Zero's web UI is commonly dark; every token has a dark pair |
| Monospace for labels and headings | warp, raycast terminal framing | machine-voice text | plugin surfaces are logs, ids, paths and config keys |
| System sans for body | craft rule: do not ship a webfont inside a plugin panel | prose only | zero font loading inside a0's iframe panels |
| Hairlines not shadows | linear.app, raycast | separation | panels live inside another app's chrome; shadows fight it |
| Socket frame + corner pin mark | patchbay concept (user plugin model) | brand mark only | gives five different glyphs a shared silhouette |
| Warn is orange-brown, not red | craft: avoid red/green only status | failure/degraded | colour-blind safe against teal; AA on both canvases |

## 3. Tokens and contrast

Light:

| Pair | Foreground | Background | Ratio | Use |
|---|---|---|---|---|
| ink on paper | #0F1720 | #F4F6F7 | 16.65:1 | body, headings |
| ink-muted on paper | #4A5663 | #F4F6F7 | 6.91:1 | secondary text |
| signal on paper | #00766E | #F4F6F7 | 5.08:1 | links, live state |
| on-signal on signal | #F4F6F7 | #00766E | 5.08:1 | primary button text |
| warn on paper | #A33A00 | #F4F6F7 | 6.13:1 | error text |
| ink on surface | #0F1720 | #E6EBED | 15.01:1 | panels |
| ink-muted on surface | #4A5663 | #E6EBED | 6.23:1 | panel secondary |

Dark:

| Pair | Foreground | Background | Ratio |
|---|---|---|---|
| night-ink on night | #E8EEF1 | #0C1116 | 16.19:1 |
| night-ink-muted on night | #9AA7B2 | #0C1116 | 7.72:1 |
| night-signal on night | #3FD3C4 | #0C1116 | 10.23:1 |
| on-night-signal on night-signal | #0C1116 | #3FD3C4 | 10.23:1 |
| night-warn on night | #FF9F6B | #0C1116 | 9.41:1 |
| night-ink-muted on night-surface | #9AA7B2 | #161D24 | 6.92:1 |

All text pairs clear WCAG AA (4.5:1). Hairlines are non-text and only need 3:1 when they are the sole boundary of an input; inputs use a 1px `ink-muted` border instead of `hairline`.

## 4. Type

- Display and headings: JetBrains Mono (falls back to `ui-monospace`). 28/18 px, weights 700/600.
- Body: the system sans stack, 15 px / 1.55. Plugin panels render inside Agent Zero, so no webfont is loaded.
- Labels and chips: mono 12 px, +0.04em tracking, uppercase only for the status chip.
- Code and ids: mono 13 px. Task ids, trace ids, page ids and peer names are always mono.
- Use `text-wrap: balance` on headings and `text-wrap: pretty` on README prose blocks.

## 5. Glyph grammar (marks and in-panel icons)

All marks, and all in-panel icons, follow one construction. The build script `assets/brand/family-build.py` generates them.

- Canvas 64 x 64. Frame: rounded square, outer edge 4..60 (56 x 56), outer corner radius 8, stroke 6, no fill.
- Pin: a filled signal-colour block 14 x 14 occupying the bottom-right corner of the frame (x 46..60, y 46..60, outer corner radius 8 so it continues the frame outline). It is the "seated" indicator and appears in every mark. In-panel icons drop the pin.
- Glyph box: 28 x 28 at x 16..44, y 16..44 (offset up-left to balance the pin). Strokes are `ink`, width 4, butt caps, mitre joins, centrelines kept inside 19..41 so outer edges stay in the box. Straight segments only; no curves except the frame corner.
- At 16 px the 6-unit frame renders at 1.5 px and the 4-unit glyph stroke at 1 px. Every glyph is checked at 16, 32 and 128 px on the family sheet.
- One glyph per plugin, one metaphor each (see the per-plugin section). Glyphs never reuse the signal colour except the shared pin.
- Wordmark: the plugin name in JetBrains Mono Bold, converted to outlines, set to the right of the mark at the cap height of the frame. Name form: `a0-plugin-<name>` in `ink`, with the `a0-plugin-` prefix in `ink-muted`.

## 6. Motion

- Only state changes move. A status chip crossfades in 120 ms; a panel section expands in 160 ms with `ease-out`.
- No looping animation, no skeleton shimmer longer than 1 s, no motion on load.
- Honour `prefers-reduced-motion`: replace every transition with an instant change.
- The pin never animates.

## 7. Surfaces (family-wide)

| Surface | Exists in family | Rule |
|---|---|---|
| README | all five | logo + wordmark top, one sentence, install, how it works, status |
| Agent Zero settings panel (`settings_sections: external`) | all five | one card per config group, hairline borders, mono key names, signal only on the enabled toggle |
| API (`api/*.py` JSON) | all five | `{ok, error}` envelope, no UI |
| Agent-facing tool output | notion, jev-compact | plain text, ids in mono-style backticks, no emoji |
| Log lines | all five | `[plugin] verb noun id=... ` one line, no colour codes |
| Social card | all five | SVG, 1280 x 640, paper or night canvas, mark left, name right |

Status chip tokens (Raycast key-cap borrow): 2px radius, 1px hairline, mono 12 px label. States: `LIVE` (signal text), `IDLE` (ink-muted), `FAILED` (warn text). Never colour alone: the word is always present.

## 8. Do and don't

Do: spend signal only on live/selected/primary; keep every mark on the 64 grid; ship paper and night variants of every asset.
Don't: add a second accent; add gradients or shadows to marks; use emoji or stock icon sets; change a glyph's stroke weight to "fit".

## 9. Known gaps

- No Refero live styles were read (MCP not connected); the lock rests on local analyses.
- JetBrains Mono is not bundled; panels fall back to `ui-monospace`. Verify the fallback in a real a0 panel before the next design pass.
- Marks and wordmarks are proposals pending owner sign-off.

## Plugin surfaces: durable

Glyph: three journal rows, the last one shorter. Each row is a journaled step; the short third row is the first unfinished step, where replay stops and execution resumes. It is three straight 4-unit strokes inside the 28 x 28 glyph box (x 19..41, rows at y 21, 30, 39; the last ends at x 31), seated in the shared frame with the signal pin.

Surfaces this plugin really has:

- Settings panel (`settings_sections: external`), keys from `default_config.yaml`: `enabled`, `engine`, `max_iterations`, `tasks`, `journal_path`, `step_timeout_s`, `max_concurrent`, `restate_ingress`, `restate_admin`, `listen_port`. Group as General (`enabled`, `engine`, `max_iterations`, `tasks`), Local engine (`journal_path`, `step_timeout_s`, `max_concurrent`), Restate engine (the three `restate_*`/`listen_port` keys). Signal colour only on `enabled`.
- API: `durable_submit`, `durable_status`, `durable_signal` (POST, `{ok, error}` envelope). Task ids and states render in mono.
- Status chip words: task states map to the family chips; `completed` and `executing` read as LIVE only while running, `paused` as IDLE, `failed` (including cancelled) as FAILED.
- Log lines: the plugin logs through `logging` with the `durable:` prefix, for example `durable: unknown engine ... plugin inert`. Keep one line, no colour.
- README with wordmark, engine table, limits.
- No standalone web UI exists; a console is a roadmap item, not shipped.
