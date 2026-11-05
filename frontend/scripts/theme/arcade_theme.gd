extends RefCounted
class_name ArcadeTheme

## Builds the "Arcade" visual theme in code: slanted gold-outlined bars,
## solid-yellow selected states, a thick red rounded main frame, and heavy
## italic headings (a FontVariation of the built-in font, so no font files).
##
## Applied to the scene root by ThemeManager; charts keep using the plain
## default font because only specific control types get the heavy font.

const GOLD        := Color("#e8c13a")
const GOLD_BRIGHT := Color("#ffe14d")
const YELLOW_FILL := Color("#f6d31e")
const INK         := Color("#14110a")      # dark text on yellow
const RED_FRAME   := Color("#c8282e")
const TEXT        := Color("#e9ecf5")
const TEXT_MUTED  := Color("#9aa3b8")
const PANEL_FILL  := Color(0.02, 0.025, 0.08, 0.78)
const BAR_FILL    := Color(0.05, 0.045, 0.03, 0.88)
const DISABLED    := Color("#6b6650")

const SKEW := Vector2(0.28, 0.0)          # leans bars like the menu buttons

static var _heavy: FontVariation


## Heavy italic version of the default font (emboldened + slanted).
static func heavy_font() -> Font:
	if _heavy == null:
		_heavy = FontVariation.new()
		_heavy.base_font = ThemeDB.fallback_font
		_heavy.variation_embolden = 0.9
		## Shear in the glyph's own coordinate convention (this is the form
		## Godot documents for synthetic italics).
		_heavy.variation_transform = Transform2D(Vector2(1.0, 0.22), Vector2(0.0, 1.0), Vector2.ZERO)
	return _heavy


## The default look: Godot's stock theme plus round checkboxes.
static func build_default() -> Theme:
	var t := Theme.new()
	CircleCheckIcons.apply(t, Color(0.80, 0.83, 0.90), Color("#6EA8FE"), Color(0.45, 0.47, 0.52))
	t.set_constant("h_separation", "CheckBox", 8)
	t.set_stylebox("focus", "CheckBox", StyleBoxEmpty.new())   # no grey box after clicking
	return t


static func build() -> Theme:
	var t := Theme.new()
	var heavy := heavy_font()

	# ── Buttons (Screen / Train) ────────────────────────────────────────────
	t.set_stylebox("normal", "Button", _bar(BAR_FILL, GOLD, 3))
	t.set_stylebox("hover", "Button", _bar(BAR_FILL.lightened(0.08), GOLD_BRIGHT, 3))
	t.set_stylebox("pressed", "Button", _bar(YELLOW_FILL, GOLD_BRIGHT, 3))
	t.set_stylebox("hover_pressed", "Button", _bar(YELLOW_FILL, GOLD_BRIGHT, 3))
	t.set_stylebox("disabled", "Button", _bar(Color(0.06, 0.06, 0.07, 0.7), DISABLED, 2))
	t.set_stylebox("focus", "Button", _empty())
	t.set_font("font", "Button", heavy)
	t.set_font_size("font_size", "Button", 18)
	t.set_color("font_color", "Button", GOLD)
	t.set_color("font_hover_color", "Button", GOLD_BRIGHT)
	t.set_color("font_pressed_color", "Button", INK)
	t.set_color("font_hover_pressed_color", "Button", INK)
	t.set_color("font_disabled_color", "Button", DISABLED)
	t.set_color("font_focus_color", "Button", GOLD_BRIGHT)

	# ── Dropdowns ───────────────────────────────────────────────────────────
	## Narrower side padding than the big buttons, so short values stay visible.
	t.set_stylebox("normal", "OptionButton", _bar(BAR_FILL, GOLD, 3, 12))
	t.set_stylebox("hover", "OptionButton", _bar(BAR_FILL.lightened(0.08), GOLD_BRIGHT, 3, 12))
	t.set_stylebox("pressed", "OptionButton", _bar(BAR_FILL, GOLD_BRIGHT, 3, 12))
	t.set_stylebox("disabled", "OptionButton", _bar(Color(0.06, 0.06, 0.07, 0.7), DISABLED, 2, 12))
	t.set_stylebox("focus", "OptionButton", _empty())
	t.set_constant("arrow_margin", "OptionButton", 10)
	t.set_font("font", "OptionButton", heavy)
	t.set_font_size("font_size", "OptionButton", 16)
	t.set_color("font_color", "OptionButton", TEXT)
	t.set_color("font_hover_color", "OptionButton", GOLD_BRIGHT)
	t.set_color("font_pressed_color", "OptionButton", GOLD_BRIGHT)
	t.set_color("font_focus_color", "OptionButton", TEXT)
	t.set_color("font_disabled_color", "OptionButton", DISABLED)

	t.set_stylebox("panel", "PopupMenu", _box(Color(0.03, 0.03, 0.08, 0.97), GOLD, 2, 6))
	t.set_stylebox("hover", "PopupMenu", _bar(YELLOW_FILL, YELLOW_FILL, 0))
	t.set_color("font_color", "PopupMenu", TEXT)
	t.set_color("font_hover_color", "PopupMenu", INK)
	t.set_font("font", "PopupMenu", heavy)

	# ── Tabs + frames ───────────────────────────────────────────────────────
	## Nested tab containers (results sub-tabs) get a thin gold frame; the main
	## one uses the "MainFrame" variation with the thick red rounded border.
	t.set_stylebox("panel", "TabContainer", _box(Color(0.02, 0.025, 0.07, 0.55), Color(GOLD, 0.45), 2, 10))
	t.set_stylebox("tab_selected", "TabContainer", _tab(YELLOW_FILL, GOLD_BRIGHT))
	t.set_stylebox("tab_unselected", "TabContainer", _tab(BAR_FILL, Color(GOLD, 0.75)))
	t.set_stylebox("tab_hovered", "TabContainer", _tab(BAR_FILL.lightened(0.1), GOLD_BRIGHT))
	t.set_stylebox("tab_disabled", "TabContainer", _tab(BAR_FILL, DISABLED))
	t.set_stylebox("tab_focus", "TabContainer", _empty())
	t.set_stylebox("tabbar_background", "TabContainer", _empty())
	t.set_font("font", "TabContainer", heavy)
	t.set_font_size("font_size", "TabContainer", 16)
	t.set_color("font_selected_color", "TabContainer", INK)
	t.set_color("font_unselected_color", "TabContainer", GOLD)
	t.set_color("font_hovered_color", "TabContainer", GOLD_BRIGHT)
	t.set_constant("side_margin", "TabContainer", 18)

	t.set_type_variation("MainFrame", "TabContainer")
	t.set_stylebox("panel", "MainFrame", _box(PANEL_FILL, RED_FRAME, 5, 22))

	# ── Text ────────────────────────────────────────────────────────────────
	t.set_font("font", "Label", heavy)
	t.set_color("font_color", "Label", TEXT)
	t.set_color("font_shadow_color", "Label", Color(0, 0, 0, 0.75))
	t.set_constant("shadow_offset_x", "Label", 2)
	t.set_constant("shadow_offset_y", "Label", 2)

	t.set_font("font", "CheckBox", heavy)
	t.set_color("font_color", "CheckBox", TEXT)
	t.set_color("font_hover_color", "CheckBox", GOLD_BRIGHT)
	t.set_color("font_pressed_color", "CheckBox", GOLD)
	t.set_color("font_hover_pressed_color", "CheckBox", GOLD_BRIGHT)
	t.set_color("font_disabled_color", "CheckBox", TEXT_MUTED)
	for state in ["normal", "hover", "pressed", "hover_pressed", "disabled", "focus"]:
		t.set_stylebox(state, "CheckBox", _empty())
	## Room between the box and the (slanted) label so they don't overlap.
	t.set_constant("h_separation", "CheckBox", 10)
	CircleCheckIcons.apply(t, GOLD, YELLOW_FILL, DISABLED)

	# ── Inputs (screening header: Top N spin box) ───────────────────────────
	t.set_stylebox("normal", "LineEdit", _box(BAR_FILL, Color(GOLD, 0.8), 2, 4))
	t.set_stylebox("focus", "LineEdit", _box(BAR_FILL, GOLD_BRIGHT, 2, 4))
	t.set_color("font_color", "LineEdit", TEXT)

	# ── Tooltips (Godot's own; chart tooltips use ChartTooltip) ─────────────
	t.set_stylebox("panel", "TooltipPanel", _box(Color(0.03, 0.03, 0.08, 0.97), Color(0.92, 0.93, 0.97), 2, 4))
	t.set_color("font_color", "TooltipLabel", TEXT)

	# ── Scrollbars ──────────────────────────────────────────────────────────
	for bar in ["VScrollBar", "HScrollBar"]:
		t.set_stylebox("scroll", bar, _box(Color(0, 0, 0, 0.35), Color(0, 0, 0, 0), 0, 4))
		t.set_stylebox("grabber", bar, _box(Color(GOLD, 0.65), Color(0, 0, 0, 0), 0, 4))
		t.set_stylebox("grabber_highlight", bar, _box(GOLD_BRIGHT, Color(0, 0, 0, 0), 0, 4))
		t.set_stylebox("grabber_pressed", bar, _box(YELLOW_FILL, Color(0, 0, 0, 0), 0, 4))

	return t


# ── StyleBox helpers ─────────────────────────────────────────────────────────

## Slanted bar with a bold outline (buttons, dropdowns).
static func _bar(fill: Color, border: Color, width: int, side_margin: int = 22) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.bg_color = fill
	sb.border_color = border
	sb.set_border_width_all(width)
	sb.set_corner_radius_all(9)
	sb.skew = SKEW
	sb.content_margin_left = side_margin
	sb.content_margin_right = side_margin
	sb.content_margin_top = 6
	sb.content_margin_bottom = 6
	sb.shadow_color = Color(border, 0.35) if width > 0 else Color(0, 0, 0, 0)
	sb.shadow_size = 6 if width > 0 else 0
	sb.anti_aliasing = true
	return sb


static func _tab(fill: Color, border: Color) -> StyleBoxFlat:
	var sb := _bar(fill, border, 2)
	sb.content_margin_left = 18
	sb.content_margin_right = 18
	sb.content_margin_top = 4
	sb.content_margin_bottom = 4
	sb.shadow_size = 3
	sb.expand_margin_bottom = 0
	return sb


## Rounded rectangle panel (frames, menus, tooltips).
static func _box(fill: Color, border: Color, width: int, radius: int) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.bg_color = fill
	sb.border_color = border
	sb.set_border_width_all(width)
	sb.set_corner_radius_all(radius)
	sb.content_margin_left = 10
	sb.content_margin_right = 10
	sb.content_margin_top = 6
	sb.content_margin_bottom = 6
	sb.anti_aliasing = true
	return sb


static func _empty() -> StyleBoxEmpty:
	return StyleBoxEmpty.new()
