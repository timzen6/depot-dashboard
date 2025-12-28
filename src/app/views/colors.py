# Define a static color class for consistent use across the app


class Colors:
    # Primary / Neutral (Modern "Inter" Blue)
    blue = "#2563eb"  # Royal Blue (strong, serious)
    light_blue = "#93c5fd"  # Soft Sky

    # Secondary (Distinctive)
    purple = "#7c3aed"  # Violet (modern for Tech/SaaS)
    light_purple = "#c4b5fd"

    # Semantic: Success / Growth (Emerald instead of "Grass Green")
    green = "#059669"  # Emerald 600 (Money color, good readability)
    light_green = "#6ee7b7"  # Emerald 300

    # Semantic: Danger / Loss (Rose/Red instead of "Warning Sign Red")
    red = "#dc2626"  # Red 600 (Clear, but not glaring)
    light_red = "#fca5a5"  # Red 300

    # Semantic: Warning / Attention (Amber instead of Yellow!)
    # IMPORTANT: Pure yellow (#FF0) is invisible on a white background.
    # We use Amber/Gold.
    amber = "#d97706"  # Amber 600 (Good readability)
    yellow = "#f59e0b"  # Amber 500 (Lighter, but visible)

    # Categories
    orange = "#ea580c"  # Burnt Orange
    light_orange = "#fdba74"
    teal = "#0d9488"  # Teal (Nice complement to blue)
    gray = "#4b5563"  # Cool Gray
    light_gray = "#9ca3af"
    white = "#ffffff"


# 1. Diverging Scale (Good vs. Bad)
# Used for: Valuation, Scores
COLOR_SCALE_GREEN_RED = [
    Colors.green,  # Top (Dark green)
    Colors.light_green,  # Good
    Colors.yellow,  # Neutral (Amber) - Replaces unreadable yellow
    Colors.light_red,  # Bad
    Colors.red,  # Critical (Dark red)
]

# 2. Contrast Scale (Categories)
# Used for: Sectors, ticker comparison
# Optimized order for maximum distinguishability
COLOR_SCALE_CONTRAST = [
    Colors.blue,  # 1. Standard
    Colors.orange,  # 2. Strong contrast to blue
    Colors.teal,  # 3. Modern, distinct from blue/green
    Colors.purple,  # 4. Tech vibe
    Colors.red,  # 5. Signal
    Colors.yellow,  # 6. (Amber)
    Colors.gray,  # 7. Neutral
    Colors.green,  # 8. (Only late, to avoid confusion with "Good")
]

# 3. Soft Scale (Stacks / Areas)
COLOR_SCALE_SOFT = [
    Colors.light_blue,
    Colors.light_orange,
    Colors.light_green,
    Colors.light_red,
    Colors.light_purple,
    Colors.light_gray,
]

STRATEGY_FACTOR_COLOR_MAP = {
    "Technology": COLOR_SCALE_CONTRAST[0],
    "Stability": COLOR_SCALE_CONTRAST[1],
    "Real Assets": COLOR_SCALE_CONTRAST[2],
    "Pricing Power": COLOR_SCALE_CONTRAST[3],
    "Unclassified": Colors.light_gray,
}
