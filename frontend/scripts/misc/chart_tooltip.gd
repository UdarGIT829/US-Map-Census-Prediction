extends RefCounted
class_name ChartTooltip

## Opaque, styled tooltip shared by the screening charts.
##
## Godot's default tooltip panel is semi-transparent, so chart rows bleed
## through behind the text. Charts return ChartTooltip.make(for_text) from
## _make_custom_tooltip(). The first line (the full feature label) is drawn
## bright; any following lines (codes, scores) are drawn smaller and dimmer.

const BG_COLOR := Color("#15181e")
const BORDER_COLOR := Color(1, 1, 1, 0.18)
const TITLE_COLOR := Color(0.95, 0.96, 0.99, 1.0)
const DETAIL_COLOR := Color(0.70, 0.75, 0.84, 1.0)


static func make(for_text: String) -> Control:
	var panel := PanelContainer.new()

	var theme_name: String = ThemeManager.current
	var title_color := TITLE_COLOR
	var detail_color := DETAIL_COLOR

	var style: StyleBox

	if theme_name == "rooftop":
		## Dark static-grain block with a thick red edge, light text.
		style = RooftopTheme.grain_box(RooftopTheme.RED, 5, 10, 10, 6)
		title_color = RooftopTheme.WHITE
		detail_color = RooftopTheme.TEXT_MUTED
	else:
		var flat := StyleBoxFlat.new()
		flat.bg_color = BG_COLOR
		flat.border_color = BORDER_COLOR
		flat.set_border_width_all(1)
		flat.set_corner_radius_all(4)

		if theme_name == "arcade":
			## Matches the arcade "description box": white 2px border, deep navy.
			flat.bg_color = Color(0.015, 0.02, 0.06, 0.96)
			flat.border_color = Color(0.90, 0.92, 0.97)
			flat.set_border_width_all(2)
			title_color = ArcadeTheme.GOLD_BRIGHT

		flat.content_margin_left = 10
		flat.content_margin_right = 10
		flat.content_margin_top = 6
		flat.content_margin_bottom = 7
		style = flat

	panel.add_theme_stylebox_override("panel", style)

	var box := VBoxContainer.new()
	box.add_theme_constant_override("separation", 2)
	panel.add_child(box)

	var lines := for_text.split("\n")

	for i in lines.size():
		var label := Label.new()
		label.text = lines[i]

		if i == 0:
			label.add_theme_color_override("font_color", title_color)
			label.add_theme_font_size_override("font_size", 14)
		else:
			label.add_theme_color_override("font_color", detail_color)
			label.add_theme_font_size_override("font_size", 12)

		box.add_child(label)

	return panel
