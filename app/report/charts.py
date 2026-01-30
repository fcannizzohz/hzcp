from __future__ import annotations

import html as _html
from typing import List, Tuple


def svg_bar_labeled(
    values: List[Tuple[str, float]],
    width: int = 620,
    height: int = 160,
    *,
    max_bars: int = 16,
    show_values: bool = True,
    show_x_labels: bool = True,
    x_label_max: int = 10,
) -> str:
    if not values:
        return ""

    vals = values[:max_bars]
    maxv = max(v for _, v in vals) or 1.0
    pad = 10
    bottom = 30 if show_x_labels else 16
    top = 14 if show_values else 8
    chart_h = height - pad - bottom - top
    n = len(vals)
    bw = max(10, (width - 2 * pad) // max(1, n) - 4)

    parts = [f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">']
    parts.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="transparent"/>')

    x = pad
    for label, v in vals:
        h = int(chart_h * (v / maxv))
        y = top + (chart_h - h)
        short = label[:x_label_max] + ("…" if len(label) > x_label_max else "")

        parts.append("<g>")
        parts.append(f'<rect x="{x}" y="{y}" width="{bw}" height="{h}" fill="currentColor" opacity="0.30">')
        parts.append(f"<title>{_html.escape(label)}: {v:g}</title>")
        parts.append("</rect>")

        if show_values:
            parts.append(
                f'<text x="{x + bw/2:.1f}" y="{y - 2:.1f}" text-anchor="middle" '
                f'font-size="10" fill="currentColor" opacity="0.9">{v:g}</text>'
            )

        if show_x_labels:
            parts.append(
                f'<text x="{x + bw/2:.1f}" y="{height - 10:.1f}" text-anchor="middle" '
                f'font-size="10" fill="currentColor" opacity="0.75">{_html.escape(short)}</text>'
            )

        parts.append("</g>")
        x += bw + 4

    parts.append(
        f'<text x="{pad}" y="{top + chart_h + 12}" font-size="11" fill="currentColor" opacity="0.75">'
        f"max {maxv:g}</text>"
    )
    parts.append("</svg>")
    return "".join(parts)

