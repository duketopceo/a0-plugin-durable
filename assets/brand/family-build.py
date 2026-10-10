#!/usr/bin/env python3
"""Patchbay family mark builder. Usage: family-build.py <plugin-name> <out-dir>
Writes logo.svg, logo-dark.svg, wordmark.svg, wordmark-dark.svg, social-card.svg.
Wordmark text is emitted as <text> and outlined afterwards with inkscape."""
import subprocess, sys, pathlib

GLYPHS = {  # centrelines inside 19..41, stroke 4
    "durable":     [("M19 21H41M19 30H41M19 39H31", None)],
    "glitchtip":   [("M19 35H25L29 22L34 40L37 31H41", None)],
    "notion":      [("M21 20H32L39 27V40H21Z M26 34H34", None)],
    "device-sync": [("M19 24H41M36 19L41 24L36 29 M41 36H19M24 31L19 36L24 41", None)],
    "jev-compact": [("M20 19L30 25L40 19 M19 30H41 M20 41L30 35L40 41", None)],
}
PAL = {
    "light": dict(ink="#0F1720", muted="#4A5663", signal="#00766E", bg="#F4F6F7"),
    "dark":  dict(ink="#E8EEF1", muted="#9AA7B2", signal="#3FD3C4", bg="#0C1116"),
}

def mark(name, c, x=0, y=0, s=1.0):
    d = " ".join(g[0] for g in GLYPHS[name])
    return (f'<g transform="translate({x} {y}) scale({s})">'
            f'<rect x="7" y="7" width="50" height="50" rx="5" fill="none" stroke="{c["ink"]}" stroke-width="6"/>'
            f'<path d="{d}" fill="none" stroke="{c["ink"]}" stroke-width="4" stroke-linejoin="miter" stroke-linecap="butt"/>'
            f'<path d="M46 46H60V52A8 8 0 0 1 52 60H46Z" fill="{c["signal"]}"/></g>')

def svg(w, h, body, title):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img">'
            f'<title>{title}</title>{body}</svg>\n')

def text(label, c, x, y, size):
    font = 'font-family="JetBrainsMono NF, JetBrains Mono, monospace" font-weight="700"'
    return (f'<text x="{x}" y="{y}" {font} font-size="{size}">'
            f'<tspan fill="{c["muted"]}">a0-plugin-</tspan><tspan fill="{c["ink"]}">{label}</tspan></text>')

def outline(path):
    subprocess.run(["inkscape", str(path), "--export-text-to-path", "--export-plain-svg",
                    "--export-filename", str(path)], check=True, capture_output=True)

def main(name, out):
    out = pathlib.Path(out); out.mkdir(parents=True, exist_ok=True)
    title = f"a0-plugin-{name}"
    for variant, suffix in (("light", ""), ("dark", "-dark")):
        c = PAL[variant]
        (out / f"logo{suffix}.svg").write_text(svg(64, 64, mark(name, c), title))
        n = len(name) + 10
        w = 64 + 16 + round(n * 0.6 * 28) + 8
        body = mark(name, c) + text(name, c, 80, 43, 28)
        p = out / f"wordmark{suffix}.svg"; p.write_text(svg(w, 64, body, title)); outline(p)
        body = (f'<rect width="1280" height="640" fill="{c["bg"]}"/>' + mark(name, c, 96, 192, 4.0)
                + text(name, c, 400, 352, 56))
        p = out / f"social-card{suffix}.svg"; p.write_text(svg(1280, 640, body, title)); outline(p)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
