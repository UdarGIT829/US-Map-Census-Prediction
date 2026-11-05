extends RefCounted
class_name RooftopTheme

## Builds the "Rooftop" visual theme: flat, square, high-contrast. Red blocks
## with white italic text, a white block with dark text for the selected
## item, a dark slate main panel with a thick red stripe on its left edge, and
## no rounded corners or glow anywhere. Applied by ThemeManager.
##
## The main panel stays dark on purpose: the charts draw light text and
## assume a dark backdrop.

const RED        := Color("#e8241a")
const RED_HOVER  := Color("#ff4a3d")
const RED_DIM    := Color("#7d2a26")
const WHITE      := Color("#ffffff")
const INK        := Color("#141821")     # dark text on white
const SLATE      := Color(0.09, 0.10, 0.12, 0.92)
const SLATE_SOFT := Color(0.12, 0.13, 0.16, 0.85)
const TEXT       := Color("#f2f4f8")
const TEXT_MUTED := Color("#a7adba")
const GRAIN_BASE := Color(0.105, 0.110, 0.122)   # near-black grey "surface"
const GRAIN_HOVER := Color(0.150, 0.155, 0.170)

static var _italic: FontVariation
## Near-black surface of animated static (GrainStyleBox), optionally with a
## solid colored bar on its left edge. `base` only sets brightness relative to
## GRAIN_BASE (lighter for hover/selected states).
static func grain_box(edge: Color = Color(0, 0, 0, 0), edge_width: int = 0,
		margin_left: int = 14, margin_right: int = 14, margin_vertical: int = 5,
		base: Color = GRAIN_BASE) -> GrainStyleBox:
	var sb := GrainStyleBox.new()
	sb.edge_color = edge
	sb.edge_width = edge_width
	sb.brightness = base.v / GRAIN_BASE.v
	sb.content_margin_left = margin_left + edge_width
	sb.content_margin_right = margin_right
	sb.content_margin_top = margin_vertical
	sb.content_margin_bottom = margin_vertical
	return sb


## Medium-weight italic (lighter than Arcade's heavy italic).
static func italic_font() -> Font:
	if _italic == null:
		_italic = FontVariation.new()
		_italic.base_font = ThemeDB.fallback_font
		_italic.variation_embolden = 0.35
		_italic.variation_transform = Transform2D(Vector2(1.0, 0.2), Vector2(0.0, 1.0), Vector2.ZERO)
	return _italic


static func build() -> Theme:
	var t := Theme.new()
	var italic := italic_font()

	# ── Buttons: red blocks; pressed turns to dark grain ────────────────────
	t.set_stylebox("normal", "Button", _block(RED))
	t.set_stylebox("hover", "Button", _block(RED_HOVER))
	t.set_stylebox("pressed", "Button", grain_box(RED, 4))
	t.set_stylebox("hover_pressed", "Button", grain_box(RED_HOVER, 4))
	t.set_stylebox("disabled", "Button", _block(Color(RED_DIM, 0.55)))
	t.set_stylebox("focus", "Button", StyleBoxEmpty.new())
	t.set_font("font", "Button", italic)
	t.set_font_size("font_size", "Button", 17)
	t.set_color("font_color", "Button", WHITE)
	t.set_color("font_hover_color", "Button", WHITE)
	t.set_color("font_pressed_color", "Button", WHITE)
	t.set_color("font_hover_pressed_color", "Button", WHITE)
	t.set_color("font_disabled_color", "Button", Color(1, 1, 1, 0.45))
	t.set_color("font_focus_color", "Button", WHITE)

	# ── Dropdowns: dark grain with a red edge ───────────────────────────────
	t.set_stylebox("normal", "OptionButton", grain_box(RED, 4, 10, 10))
	t.set_stylebox("hover", "OptionButton", grain_box(RED_HOVER, 4, 10, 10, 5, GRAIN_HOVER))
	t.set_stylebox("pressed", "OptionButton", grain_box(RED, 4, 10, 10))
	t.set_stylebox("disabled", "OptionButton", grain_box(RED_DIM, 4, 10, 10))
	t.set_stylebox("focus", "OptionButton", StyleBoxEmpty.new())
	t.set_font("font", "OptionButton", italic)
	t.set_font_size("font_size", "OptionButton", 15)
	t.set_color("font_color", "OptionButton", TEXT)
	t.set_color("font_hover_color", "OptionButton", WHITE)
	t.set_color("font_pressed_color", "OptionButton", WHITE)
	t.set_color("font_focus_color", "OptionButton", TEXT)
	t.set_color("font_disabled_color", "OptionButton", TEXT_MUTED)
	t.set_constant("arrow_margin", "OptionButton", 8)

	t.set_stylebox("panel", "PopupMenu", grain_box(RED, 3, 6, 6, 6))
	t.set_stylebox("hover", "PopupMenu", _block(RED))
	t.set_color("font_color", "PopupMenu", TEXT)
	t.set_color("font_hover_color", "PopupMenu", WHITE)
	t.set_font("font", "PopupMenu", italic)

	# ── Tabs: red blocks; the selected one is dark grain with a red edge ────
	t.set_stylebox("panel", "TabContainer", _block(SLATE_SOFT))
	t.set_stylebox("tab_selected", "TabContainer", grain_box(RED, 6, 18, 18, 5, GRAIN_HOVER))
	t.set_stylebox("tab_unselected", "TabContainer", _block(RED, Color(0, 0, 0, 0), 0, 18))
	t.set_stylebox("tab_hovered", "TabContainer", _block(RED_HOVER, Color(0, 0, 0, 0), 0, 18))
	t.set_stylebox("tab_disabled", "TabContainer", _block(RED_DIM, Color(0, 0, 0, 0), 0, 18))
	t.set_stylebox("tab_focus", "TabContainer", StyleBoxEmpty.new())
	t.set_stylebox("tabbar_background", "TabContainer", StyleBoxEmpty.new())
	t.set_font("font", "TabContainer", italic)
	t.set_font_size("font_size", "TabContainer", 16)
	t.set_color("font_selected_color", "TabContainer", WHITE)
	t.set_color("font_unselected_color", "TabContainer", WHITE)
	t.set_color("font_hovered_color", "TabContainer", WHITE)
	t.set_constant("side_margin", "TabContainer", 0)

	## Main panel: dark slate with a thick red stripe down its left edge.
	t.set_type_variation("MainFrame", "TabContainer")
	var frame := _block(SLATE)
	frame.border_color = RED
	frame.border_width_left = 10
	frame.content_margin_left = 18
	t.set_stylebox("panel", "MainFrame", frame)

	# ── Text ────────────────────────────────────────────────────────────────
	t.set_font("font", "Label", italic)
	t.set_color("font_color", "Label", TEXT)

	t.set_font("font", "CheckBox", italic)
	t.set_color("font_color", "CheckBox", TEXT)
	t.set_color("font_hover_color", "CheckBox", WHITE)
	t.set_color("font_pressed_color", "CheckBox", TEXT)
	t.set_color("font_hover_pressed_color", "CheckBox", WHITE)
	t.set_color("font_disabled_color", "CheckBox", TEXT_MUTED)
	for state in ["normal", "hover", "pressed", "hover_pressed", "disabled", "focus"]:
		t.set_stylebox(state, "CheckBox", StyleBoxEmpty.new())
	t.set_constant("h_separation", "CheckBox", 10)
	CircleCheckIcons.apply(t, RED, RED, RED_DIM)

	# ── Inputs ──────────────────────────────────────────────────────────────
	t.set_stylebox("normal", "LineEdit", grain_box(RED, 3, 6, 6))
	t.set_stylebox("focus", "LineEdit", grain_box(RED_HOVER, 3, 6, 6))
	t.set_color("font_color", "LineEdit", TEXT)

	# ── Tooltips ────────────────────────────────────────────────────────────
	t.set_stylebox("panel", "TooltipPanel", grain_box(RED, 5, 10, 10, 6))
	t.set_color("font_color", "TooltipLabel", TEXT)

	# ── Scrollbars: thin flat bars (no button padding, or they get fat) ─────
	for bar in ["VScrollBar", "HScrollBar"]:
		t.set_stylebox("scroll", bar, _thin(Color(1, 1, 1, 0.08)))
		t.set_stylebox("grabber", bar, _thin(RED))
		t.set_stylebox("grabber_highlight", bar, _thin(RED_HOVER))
		t.set_stylebox("grabber_pressed", bar, _thin(Color(1, 1, 1, 0.85)))

	return t


static func _thin(fill: Color) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.bg_color = fill
	sb.set_content_margin_all(3)
	return sb


## Flat square block, optionally with a left edge stripe.
static func _block(fill: Color, edge: Color = Color(0, 0, 0, 0), edge_width: int = 0, side_margin: int = 14) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.bg_color = fill
	sb.border_color = edge
	sb.border_width_left = edge_width
	sb.content_margin_left = side_margin + edge_width
	sb.content_margin_right = side_margin
	sb.content_margin_top = 5
	sb.content_margin_bottom = 5
	return sb
