class_name FeaturewiseSunburstChart
extends Control

const FAMILY_COLORS := {
	"DP02": Color("#6EA8FE"),
	"DP03": Color("#75D69C"),
	"DP04": Color("#F2C66D"),
	"DP05": Color("#D38CFF"),
	"OTHER": Color("#AAB2C0"),
}

@export var equal_family_sectors: bool = true
@export var show_legend: bool = true
@export var show_center_summary: bool = true
@export var show_large_labels: bool = true

@export var min_label_angle_span: float = 0.22
@export var inner_hole_ratio: float = 0.14
@export var wedge_gap_degrees: float = 0.35

@export var legend_width: float = 130.0
@export var legend_padding: float = 10.0

@export var tooltip_width: float = 360.0
@export var tooltip_padding: float = 10.0

@export var outline_color: Color = Color(0.08, 0.09, 0.12, 0.85)
@export var empty_color: Color = Color(0.12, 0.13, 0.16, 1.0)
@export var text_color: Color = Color(0.92, 0.94, 0.98, 1.0)
@export var muted_text_color: Color = Color(0.70, 0.74, 0.82, 1.0)
@export var tooltip_bg_color: Color = Color(0.045, 0.05, 0.065, 0.94)
@export var hover_outline_color: Color = Color(1.0, 1.0, 1.0, 0.95)

var _year_data: Dictionary = {}
var _tree: Dictionary = {}
var _score_range: Dictionary = {"min": 0.0, "max": 1.0}
var _year_label: String = ""

var _layout_items: Array = []
var _hovered_item: Dictionary = {}
var _mouse_pos: Vector2 = Vector2.ZERO

var _chart_center: Vector2 = Vector2.ZERO
var _chart_radius: float = 0.0


func _ready() -> void:
	mouse_filter = Control.MOUSE_FILTER_STOP
	custom_minimum_size = Vector2(520, 420)


func set_year_data(year_data: Dictionary) -> void:
	_year_data = year_data
	_year_label = str(year_data.get("year", ""))

	_tree = year_data.get("feature_tree", {})
	_score_range = year_data.get("score_ranges", {}).get(
		"feature_mean_score_r2",
		{"min": 0.0, "max": 1.0}
	)

	_hovered_item = {}
	queue_redraw()


func clear_chart() -> void:
	_year_data.clear()
	_tree.clear()
	_layout_items.clear()
	_hovered_item.clear()
	queue_redraw()


func _gui_input(event: InputEvent) -> void:
	if event is InputEventMouseMotion:
		_mouse_pos = event.position
		_hovered_item = _hit_test(event.position)
		queue_redraw()

	elif event is InputEventMouseButton:
		if event.button_index == MOUSE_BUTTON_LEFT and event.pressed:
			_mouse_pos = event.position
			_hovered_item = _hit_test(event.position)
			queue_redraw()


func _draw() -> void:
	_layout_items.clear()

	if _tree.is_empty():
		_draw_empty_state()
		return

	_compute_layout()
	_draw_sunburst()

	if show_center_summary:
		_draw_center_summary()

	if show_legend:
		_draw_family_legend()

	if not _hovered_item.is_empty():
		_draw_hover_highlight(_hovered_item)
		_draw_tooltip(_hovered_item, _mouse_pos)


# ==============================================================================
# Layout
# ==============================================================================

func _compute_layout() -> void:
	var rect := get_rect()
	var available := rect.size

	var right_reserved := 0.0
	if show_legend:
		right_reserved = legend_width + legend_padding * 2.0

	var chart_rect := Rect2(
		Vector2(0, 0),
		Vector2(max(1.0, available.x - right_reserved), available.y)
	)

	_chart_center = chart_rect.get_center()
	_chart_radius = min(chart_rect.size.x, chart_rect.size.y) * 0.46

	var max_depth := _get_max_depth(_tree)
	var inner_hole := _chart_radius * inner_hole_ratio
	var usable_radius := _chart_radius - inner_hole
	var ring_count :int= max(1, max_depth)
	var ring_width := usable_radius / float(ring_count)

	var root_children: Array = _tree.get("children", [])
	if root_children.is_empty():
		return

	var angle_start := -PI / 2.0
	var angle_end := angle_start + TAU

	if equal_family_sectors:
		var span_each := TAU / float(root_children.size())
		for i in range(root_children.size()):
			var child: Dictionary = root_children[i]
			var a0 := angle_start + span_each * float(i)
			var a1 := a0 + span_each
			var family := _node_family(child, "")
			_layout_node(child, a0, a1, 1, inner_hole, ring_width, family)
	else:
		var total_weight := _children_weight(root_children)
		var cursor := angle_start
		for child_variant in root_children:
			var child: Dictionary = child_variant
			var frac :float= float(child.get("weight", 1.0)) / max(total_weight, 0.000001)
			var span := TAU * frac
			var family := _node_family(child, "")
			_layout_node(child, cursor, cursor + span, 1, inner_hole, ring_width, family)
			cursor += span


func _layout_node(
	node: Dictionary,
	angle_start: float,
	angle_end: float,
	depth: int,
	inner_hole: float,
	ring_width: float,
	family: String
) -> void:
	var gap := deg_to_rad(wedge_gap_degrees)
	var a0 := angle_start + gap * 0.5
	var a1 := angle_end - gap * 0.5

	if a1 <= a0:
		a0 = angle_start
		a1 = angle_end

	var r0 := inner_hole + float(depth - 1) * ring_width
	var r1 := inner_hole + float(depth) * ring_width

	var item := {
		"node": node,
		"family": family,
		"angle_start": a0,
		"angle_end": a1,
		"radius_inner": r0,
		"radius_outer": r1,
		"depth": depth
	}

	_layout_items.append(item)

	var children: Array = node.get("children", [])
	if children.is_empty():
		return

	var total_weight := _children_weight(children)
	var cursor := angle_start
	var span_total := angle_end - angle_start

	for child_variant in children:
		var child: Dictionary = child_variant
		var child_family := family
		if str(child.get("type", "")) == "family":
			child_family = _node_family(child, family)

		var frac :float= float(child.get("weight", 1.0)) / max(total_weight, 0.000001)
		var child_span := span_total * frac

		_layout_node(
			child,
			cursor,
			cursor + child_span,
			depth + 1,
			inner_hole,
			ring_width,
			child_family
		)

		cursor += child_span


func _children_weight(children: Array) -> float:
	var total := 0.0
	for child_variant in children:
		var child: Dictionary = child_variant
		total += max(0.000001, float(child.get("weight", 1.0)))
	return max(total, 0.000001)


func _get_max_depth(node: Dictionary) -> int:
	var children: Array = node.get("children", [])
	if children.is_empty():
		return int(node.get("depth", 0))

	var max_depth := int(node.get("depth", 0))
	for child_variant in children:
		var child: Dictionary = child_variant
		max_depth = max(max_depth, _get_max_depth(child))

	return max_depth


# ==============================================================================
# Drawing
# ==============================================================================

func _draw_sunburst() -> void:
	for item_variant in _layout_items:
		var item: Dictionary = item_variant
		var node: Dictionary = item["node"]

		var family := str(item.get("family", "OTHER"))
		var score := float(node.get("score", 0.0))
		var color := _node_color(family, score)

		_draw_wedge(
			_chart_center,
			float(item["radius_inner"]),
			float(item["radius_outer"]),
			float(item["angle_start"]),
			float(item["angle_end"]),
			color
		)

		_draw_wedge_outline(
			_chart_center,
			float(item["radius_inner"]),
			float(item["radius_outer"]),
			float(item["angle_start"]),
			float(item["angle_end"]),
			outline_color
		)

	if show_large_labels:
		_draw_large_wedge_labels()


func _draw_wedge(
	center: Vector2,
	inner_r: float,
	outer_r: float,
	angle_start: float,
	angle_end: float,
	color: Color
) -> void:
	var points := PackedVector2Array()
	var span := angle_end - angle_start
	var steps := clampi(int(abs(span) / TAU * 160.0), 8, 80)

	for i in range(steps + 1):
		var t := float(i) / float(steps)
		var a :float= lerp(angle_start, angle_end, t)
		points.append(center + Vector2(cos(a), sin(a)) * outer_r)

	for i in range(steps, -1, -1):
		var t := float(i) / float(steps)
		var a :float= lerp(angle_start, angle_end, t)
		points.append(center + Vector2(cos(a), sin(a)) * inner_r)

	draw_colored_polygon(points, color)


func _draw_wedge_outline(
	center: Vector2,
	inner_r: float,
	outer_r: float,
	angle_start: float,
	angle_end: float,
	color: Color,
	width: float = 1.0
) -> void:
	var outer_points := PackedVector2Array()
	var inner_points := PackedVector2Array()

	var span := angle_end - angle_start
	var steps := clampi(int(abs(span) / TAU * 160.0), 8, 80)

	for i in range(steps + 1):
		var t := float(i) / float(steps)
		var a :float= lerp(angle_start, angle_end, t)
		outer_points.append(center + Vector2(cos(a), sin(a)) * outer_r)
		inner_points.append(center + Vector2(cos(a), sin(a)) * inner_r)

	draw_polyline(outer_points, color, width)
	draw_polyline(inner_points, color, width)

	var p0_outer := center + Vector2(cos(angle_start), sin(angle_start)) * outer_r
	var p0_inner := center + Vector2(cos(angle_start), sin(angle_start)) * inner_r
	var p1_outer := center + Vector2(cos(angle_end), sin(angle_end)) * outer_r
	var p1_inner := center + Vector2(cos(angle_end), sin(angle_end)) * inner_r

	draw_line(p0_inner, p0_outer, color, width)
	draw_line(p1_inner, p1_outer, color, width)


func _draw_large_wedge_labels() -> void:
	var font := get_theme_default_font()
	var font_size := 11

	for item_variant in _layout_items:
		var item: Dictionary = item_variant
		var node: Dictionary = item["node"]

		var depth := int(item.get("depth", 0))
		if depth > 2:
			continue

		var a0 := float(item["angle_start"])
		var a1 := float(item["angle_end"])
		var span := a1 - a0

		if span < min_label_angle_span:
			continue

		var label := _pretty_name(str(node.get("name", "")))
		if label == "":
			continue

		var r_in := float(item["radius_inner"])
		var r_out := float(item["radius_outer"])
		var ring_thickness := r_out - r_in

		if ring_thickness < font_size + 4.0:
			continue

		var mid_a := (a0 + a1) * 0.5
		var mid_r := (r_in + r_out) * 0.5
		var pos := _chart_center + Vector2(cos(mid_a), sin(mid_a)) * mid_r

		## Text follows the arc (tangent to the circle), so it only has to fit
		## the arc length; the ring thickness check above covers its height.
		var max_w := span * mid_r * 0.85

		label = _fit_label(font, label, max_w, font_size)
		if label == "":
			continue

		## Tangent angle, flipped on the lower half so text is never upside down.
		var rot := wrapf(mid_a + PI / 2.0, -PI, PI)
		if rot > PI / 2.0 or rot < -PI / 2.0:
			rot += PI

		var text_size := font.get_string_size(label, HORIZONTAL_ALIGNMENT_LEFT, -1, font_size)
		var baseline := Vector2(-text_size.x * 0.5, font_size * 0.35)

		draw_set_transform(pos, rot, Vector2.ONE)
		draw_string(font, baseline + Vector2(1, 1), label, HORIZONTAL_ALIGNMENT_LEFT, -1, font_size, Color(0, 0, 0, 0.7))
		draw_string(font, baseline, label, HORIZONTAL_ALIGNMENT_LEFT, -1, font_size, Color(0.96, 0.97, 1.0, 0.95))
		draw_set_transform(Vector2.ZERO, 0.0, Vector2.ONE)


## Census group names arrive in ALL CAPS ("MARITAL STATUS"); sentence-case
## them for display. Family codes like DP02 are left alone.
func _pretty_name(name: String) -> String:
	if name == "" or name.begins_with("DP") and name.length() == 4:
		return name

	if name != name.to_upper():
		return name

	var lower := name.to_lower()
	return lower.substr(0, 1).to_upper() + lower.substr(1)


## Truncate with "…" until the label fits max_w pixels; "" if even a short
## stub won't fit (better no label than an unreadable one).
func _fit_label(font: Font, label: String, max_w: float, font_size: int) -> String:
	if font.get_string_size(label, HORIZONTAL_ALIGNMENT_LEFT, -1, font_size).x <= max_w:
		return label

	for n in range(label.length() - 1, 5, -1):
		var candidate := label.substr(0, n).strip_edges() + "…"
		if font.get_string_size(candidate, HORIZONTAL_ALIGNMENT_LEFT, -1, font_size).x <= max_w:
			return candidate

	return ""


func _draw_center_summary() -> void:
	var font := get_theme_default_font()
	var title_size := 14
	var small_size := 11

	var root_children: Array = _tree.get("children", [])
	var feature_count := _count_leaf_features(_tree)

	var title := "Feature Scores"
	if _year_label != "":
		title += " " + _year_label

	var subtitle := "%d features" % feature_count


	var bg_radius := _chart_radius * inner_hole_ratio * 0.88
	draw_circle(_chart_center, bg_radius, Color(0.045, 0.05, 0.065, 0.96))
	draw_arc(_chart_center, bg_radius, 0.0, TAU, 80, Color(0.20, 0.22, 0.28, 0.9), 1.0)

	## Title sits top-left (matching the pairwise view): the inner hole is far
	## narrower than the text and it spilled over the family labels.
	draw_string(font, Vector2(14.0, 24.0), title, HORIZONTAL_ALIGNMENT_LEFT, -1, title_size, text_color)
	draw_string(
		font,
		Vector2(14.0, 42.0),
		subtitle + " · ring = Census hierarchy, shade = mean R² of recipes containing it",
		HORIZONTAL_ALIGNMENT_LEFT,
		-1,
		small_size,
		muted_text_color
	)


func _draw_family_legend() -> void:
	var font := get_theme_default_font()
	var font_size := 12
	var small_size := 10

	var x := size.x - legend_width - legend_padding
	var y := legend_padding

	var panel_h := 150.0
	var panel := Rect2(Vector2(x, y), Vector2(legend_width, panel_h))

	draw_rect(panel, Color(0.045, 0.05, 0.065, 0.88), true)
	draw_rect(panel, Color(0.20, 0.22, 0.28, 0.9), false, 1.0)

	## First swatch row starts below the "ACS Family" title (baseline y+16).
	var cursor := y + 40.0
	draw_string(font, Vector2(x + 10.0, y + 16.0), "ACS Family", HORIZONTAL_ALIGNMENT_LEFT, -1, font_size, text_color)

	for family in ["DP02", "DP03", "DP04", "DP05", "OTHER"]:
		var color: Color = FAMILY_COLORS.get(family, FAMILY_COLORS["OTHER"])
		var swatch := Rect2(Vector2(x + 10.0, cursor - 10.0), Vector2(12.0, 12.0))
		draw_rect(swatch, color, true)
		draw_rect(swatch, Color(1, 1, 1, 0.25), false, 1.0)

		draw_string(
			font,
			Vector2(x + 30.0, cursor),
			family,
			HORIZONTAL_ALIGNMENT_LEFT,
			-1,
			font_size,
			text_color
		)

		cursor += 19.0

	var min_score := float(_score_range.get("min", 0.0))
	var max_score := float(_score_range.get("max", 1.0))
	var range_text := "shade = R² %.3f–%.3f" % [min_score, max_score]
	draw_string(
		font,
		Vector2(x + 10.0, y + panel_h - 12.0),
		range_text,
		HORIZONTAL_ALIGNMENT_LEFT,
		-1,
		small_size,
		muted_text_color
	)


func _draw_hover_highlight(item: Dictionary) -> void:
	_draw_wedge_outline(
		_chart_center,
		float(item["radius_inner"]),
		float(item["radius_outer"]),
		float(item["angle_start"]),
		float(item["angle_end"]),
		hover_outline_color,
		2.5
	)


func _draw_tooltip(item: Dictionary, mouse_pos: Vector2) -> void:
	var node: Dictionary = item.get("node", {})
	var font := get_theme_default_font()

	var title_size := 13
	var body_size := 11
	var line_h := 17.0

	var lines: Array[String] = _tooltip_lines(item)

	var title := _pretty_name(str(node.get("name", "")))
	if title == "":
		title = str(node.get("label", "Feature"))

	## Size to content (the full Census path can be long), within the chart.
	var content_w := font.get_string_size(title, HORIZONTAL_ALIGNMENT_LEFT, -1, title_size).x
	for line in lines:
		content_w = max(content_w, font.get_string_size(line, HORIZONTAL_ALIGNMENT_LEFT, -1, body_size).x)

	var tip_w: float = clamp(content_w + tooltip_padding * 2.0 + 16.0, 220.0, max(220.0, size.x - 16.0))

	var tooltip_h := tooltip_padding * 2.0 + 20.0 + line_h * float(lines.size())
	var pos := mouse_pos + Vector2(18, 18)

	if pos.x + tip_w > size.x:
		pos.x = mouse_pos.x - tip_w - 18.0

	if pos.y + tooltip_h > size.y:
		pos.y = size.y - tooltip_h - 8.0

	pos.x = clamp(pos.x, 8.0, max(8.0, size.x - tip_w - 8.0))
	pos.y = clamp(pos.y, 8.0, max(8.0, size.y - tooltip_h - 8.0))

	var rect := Rect2(pos, Vector2(tip_w, tooltip_h))

	draw_rect(rect, tooltip_bg_color, true)
	draw_rect(rect, Color(0.28, 0.31, 0.40, 0.95), false, 1.0)

	var family := str(item.get("family", "OTHER"))
	var family_color: Color = FAMILY_COLORS.get(family, FAMILY_COLORS["OTHER"])
	draw_rect(Rect2(pos, Vector2(5.0, tooltip_h)), family_color, true)

	var title_pos := pos + Vector2(tooltip_padding + 8.0, tooltip_padding + 11.0)
	draw_string(font, title_pos, title, HORIZONTAL_ALIGNMENT_LEFT, tip_w - 24.0, title_size, text_color)

	var y := title_pos.y + 22.0
	for line in lines:
		draw_string(
			font,
			Vector2(pos.x + tooltip_padding + 8.0, y),
			line,
			HORIZONTAL_ALIGNMENT_LEFT,
			tip_w - 24.0,
			body_size,
			muted_text_color
		)
		y += line_h


func _draw_empty_state() -> void:
	var font := get_theme_default_font()
	var message := "No featurewise training results loaded."
	var font_size := 14
	var text_size := font.get_string_size(message, HORIZONTAL_ALIGNMENT_CENTER, -1, font_size)

	draw_rect(Rect2(Vector2.ZERO, size), empty_color, true)
	draw_string(
		font,
		size * 0.5 - text_size * 0.5,
		message,
		HORIZONTAL_ALIGNMENT_LEFT,
		-1,
		font_size,
		muted_text_color
	)


# ==============================================================================
# Hover / hit testing
# ==============================================================================

func _hit_test(pos: Vector2) -> Dictionary:
	if _layout_items.is_empty():
		return {}

	var v := pos - _chart_center
	var r := v.length()

	if r <= 0.001:
		return {}

	var angle := atan2(v.y, v.x)

	# Layout starts at -PI/2 and runs to 3PI/2.
	if angle < -PI / 2.0:
		angle += TAU

	var best: Dictionary = {}
	var best_depth := -1

	for item_variant in _layout_items:
		var item: Dictionary = item_variant

		var r0 := float(item["radius_inner"])
		var r1 := float(item["radius_outer"])
		var a0 := float(item["angle_start"])
		var a1 := float(item["angle_end"])

		if r < r0 or r > r1:
			continue

		if angle < a0 or angle > a1:
			continue

		var depth := int(item.get("depth", 0))
		if depth > best_depth:
			best = item
			best_depth = depth

	return best


# ==============================================================================
# Color
# ==============================================================================

func _node_color(family: String, score: float) -> Color:
	var base: Color = FAMILY_COLORS.get(family, FAMILY_COLORS["OTHER"])

	var min_score := float(_score_range.get("min", 0.0))
	var max_score := float(_score_range.get("max", 1.0))

	var t := 0.5
	if abs(max_score - min_score) > 0.000001:
		t = inverse_lerp(min_score, max_score, score)

	t = clamp(t, 0.0, 1.0)

	# Keep family identity, but use score as brightness/saturation.
	var low := Color(0.12, 0.13, 0.16, 1.0).lerp(base, 0.38)
	var high := base.lightened(0.18)

	return low.lerp(high, t)


func _node_family(node: Dictionary, fallback: String) -> String:
	var node_type := str(node.get("type", ""))
	var name := str(node.get("name", ""))

	if node_type == "family":
		if FAMILY_COLORS.has(name):
			return name
		return "OTHER"

	var path: Array = node.get("path", [])
	if not path.is_empty():
		var first := str(path[0])
		if FAMILY_COLORS.has(first):
			return first

	if fallback != "":
		return fallback

	return "OTHER"


# ==============================================================================
# Tooltip helpers
# ==============================================================================

func _tooltip_lines(item: Dictionary) -> Array[String]:
	var node: Dictionary = item.get("node", {})
	var lines: Array[String] = []

	var family := str(item.get("family", "OTHER"))
	var score := float(node.get("score", 0.0))
	var weight := float(node.get("weight", 0.0))
	var pair_lift := float(node.get("pair_lift_weighted", 0.0))

	var feature := str(node.get("feature", ""))

	## Lead with where this sits in the Census hierarchy; it gives the
	## (often terse) leaf name its meaning.
	var path: Array = node.get("path", [])
	if not path.is_empty():
		lines.append(_path_to_string(path))

	lines.append("Family %s%s" % [family, ("  ·  " + feature) if feature != "" else ""])
	lines.append("Mean R²: %.4f  ·  weight/count: %.0f" % [score, weight])

	if abs(pair_lift) > 0.000001:
		lines.append("Avg pair lift: %.4f" % pair_lift)

	var label := str(node.get("label", ""))
	if label != "" and label != str(node.get("name", "")):
		lines.append("Label: %s" % label)

	return lines


func _path_to_string(path: Array) -> String:
	var parts: Array[String] = []
	for part in path:
		var s := _pretty_name(str(part))
		if s != "":
			parts.append(s)

	return " › ".join(parts)


func _truncate(value: String, max_chars: int) -> String:
	if value.length() <= max_chars:
		return value

	return value.substr(0, max_chars - 1) + "…"


func _count_leaf_features(node: Dictionary) -> int:
	var children: Array = node.get("children", [])
	if children.is_empty():
		if str(node.get("feature", "")) != "":
			return 1
		return 0

	var total := 0
	for child_variant in children:
		var child: Dictionary = child_variant
		total += _count_leaf_features(child)

	return total
