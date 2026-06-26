"""Generates two 512x512 avatar PNGs: one for the bot, one for the channel."""

import math
from PIL import Image, ImageDraw, ImageFont, ImageFilter

SIZE = 512
FONT_PATH = "/System/Library/Fonts/Supplemental/Arial Unicode.ttf"


def radial_gradient(size, inner, outer):
    img = Image.new("RGB", (size, size), outer)
    draw = ImageDraw.Draw(img)
    max_r = size * 0.75
    cx, cy = size / 2, size / 2
    steps = 200
    for i in range(steps, 0, -1):
        r = max_r * i / steps
        t = i / steps
        color = tuple(int(inner[c] * (1 - t) + outer[c] * t) for c in range(3))
        bbox = [cx - r, cy - r, cx + r, cy + r]
        draw.ellipse(bbox, fill=color)
    return img


def add_glow_ring(img, color, width=10, radius_ratio=0.92, alpha=120):
    size = img.size[0]
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    r = size * radius_ratio / 2
    cx, cy = size / 2, size / 2
    draw.ellipse([cx - r, cy - r, cx + r, cy + r], outline=color + (alpha,), width=width)
    overlay = overlay.filter(ImageFilter.GaussianBlur(6))
    img.paste(Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB"), (0, 0))
    return img


def draw_circuit_lines(draw, size, color, alpha=70):
    rng_points = [
        (40, 420, 130, 420), (130, 420, 130, 470), (130, 470, 220, 470),
        (480, 90, 400, 90), (400, 90, 400, 40),
        (40, 90, 90, 90), (90, 90, 90, 150),
        (470, 430, 420, 430), (420, 430, 420, 480),
    ]
    c = color + (alpha,)
    for x1, y1, x2, y2 in rng_points:
        draw.line([(x1, y1), (x2, y2)], fill=c, width=4)
        draw.ellipse([x1 - 5, y1 - 5, x1 + 5, y1 + 5], fill=c)
        draw.ellipse([x2 - 5, y2 - 5, x2 + 5, y2 + 5], fill=c)


BOLD_FONT_PATH = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"


def draw_btc_glyph(base, cx, cy, scale, color):
    """Draw a hand-built Bitcoin glyph (bold 'B' + two ticks), since '₿' has no
    glyph in the available system fonts and renders as a tofu box."""
    draw = ImageDraw.Draw(base, "RGBA")
    size = int(220 * scale)
    font = ImageFont.truetype(BOLD_FONT_PATH, size)
    text = "B"
    bbox = draw.textbbox((0, 0), text, font=font)
    w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    x = cx - w / 2 - bbox[0]
    y = cy - h / 2 - bbox[1]
    draw.text((x, y), text, font=font, fill=color)

    tick_w = max(4, int(10 * scale))
    tick_len = int(34 * scale)
    tick_x = cx + w * 0.06
    top_y = cy - h / 2 - 2
    bottom_y = cy + h / 2 - 6
    draw.line([(tick_x, top_y - tick_len), (tick_x, top_y + 6)], fill=color, width=tick_w)
    draw.line([(tick_x, bottom_y - 6), (tick_x, bottom_y + tick_len)], fill=color, width=tick_w)


def make_bot_avatar(path):
    img = radial_gradient(SIZE, (18, 28, 58), (6, 8, 18))
    draw = ImageDraw.Draw(img, "RGBA")

    draw_circuit_lines(draw, SIZE, (0, 224, 255), alpha=90)

    # outer ring (bot "frame")
    r = SIZE * 0.40
    cx, cy = SIZE / 2, SIZE / 2
    draw.ellipse([cx - r, cy - r, cx + r, cy + r], outline=(0, 224, 255, 230), width=10)

    # small "antenna" dots top
    draw.line([(cx, cy - r - 18), (cx, cy - r - 50)], fill=(255, 184, 28, 255), width=6)
    draw.ellipse([cx - 10, cy - r - 65, cx + 10, cy - r - 45], fill=(255, 184, 28, 255))

    # Bitcoin glyph in the center (the bot's "eye"/core)
    draw_btc_glyph(img, cx, cy + 6, 1.0, (255, 184, 28, 255))

    img = add_glow_ring(img, (0, 224, 255), width=14, alpha=140)
    img.save(path, "PNG")


def make_channel_avatar(path):
    img = radial_gradient(SIZE, (40, 18, 70), (10, 6, 22))
    draw = ImageDraw.Draw(img, "RGBA")

    cx, cy = SIZE / 2, SIZE / 2

    # Candlestick mini-chart along the bottom half, trending up
    base_y = cy + 120
    xs = [cx - 150, cx - 95, cx - 40, cx + 15, cx + 70, cx + 125]
    heights = [50, 75, 60, 95, 80, 130]
    bull = [True, True, False, True, True, True]
    for x, h, up in zip(xs, heights, bull):
        color = (0, 224, 137, 255) if up else (255, 90, 90, 255)
        top = base_y - h
        draw.line([(x, top - 18), (x, base_y + 10)], fill=color, width=3)
        draw.rectangle([x - 12, top, x + 12, base_y], fill=color)

    # Uptrend arrow across the chart
    draw.line([(cx - 160, base_y - 10), (cx + 150, base_y - 150)], fill=(255, 215, 64, 255), width=8)
    ax, ay = cx + 150, base_y - 150
    arrow = [(ax, ay), (ax - 28, ay + 6), (ax - 6, ay + 30)]
    draw.polygon(arrow, fill=(255, 215, 64, 255))

    # Bitcoin glyph badge, upper area
    badge_r = 95
    bx, by = cx, cy - 110
    draw.ellipse([bx - badge_r, by - badge_r, bx + badge_r, by + badge_r], fill=(255, 184, 28, 255))
    draw_btc_glyph(img, bx, by + 4, 0.62, (40, 18, 70, 255))

    img = add_glow_ring(img, (180, 100, 255), width=12, alpha=130)
    img.save(path, "PNG")


if __name__ == "__main__":
    make_bot_avatar("avatar_bot.png")
    make_channel_avatar("avatar_channel.png")
    print("done")
