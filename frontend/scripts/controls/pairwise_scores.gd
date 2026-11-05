class_name PairwiseHeatmapChart
extends Control

const FAMILY_COLORS := {
	"DP02": Color("#6EA8FE"),
	"DP03": Color("#75D69C"),
	"DP04": Color("#F2C66D"),
	"DP05": Color("#D38CFF"),
	"OTHER": Color("#AAB2C0"),
}

const METRIC_LIFT := "lift"
const METRIC_PAIR_R2 := "mean_score_r2"
const METRIC_BASELINE_R2 := "baseline_score_r2"
const METRIC_COUNT := "co_occurrence_count"

enum PairScaleMode {
	LINEAR,
	LOG_MAGNITUDE,
	LOG_ZOOMED,
}

@export var metric: String = METRIC_LIFT

## For lift values clustered in a narrow small range, LOG_ZOOMED is usually best.
@export var pair_scale_mode: PairScaleMode = PairScaleMode.LOG_ZOOMED

## Used by LOG_MAGNITUDE / LOG_ZOOMED to avoid log(0).
## For lift values around 0.001–0.015, this usually works well.
@export var lift_log_epsilon: float = 0.0001

## Used only for LOG_ZOOMED. This makes the visible range start near the
## smallest nonzero visible lift instead of zero.
@export var lift_zoom_padding_ratio: float = 0.85

@export var max_visible_features: int = 70
@export var draw_full_matrix: bool = true
@export var draw_diagonal: bool = false

@export var left_axis_width: float = 115.0
@export var top_axis_height: float = 82.0
@export var right_legend_width: float = 130.0
@export var bottom_padding: float = 18.0
@export var chart_padding: float = 10.0

@export var family_band_size: float = 18.0
@export var topic_separator_alpha: float = 0.28
@export var subfamily_separator_alpha: float = 0.13

@export var min_cell_size_for_grid: float = 6.0
@export var min_cell_size_for_hover_outline: float = 3.0

@export var missing_cell_color: Color = Color(0.105, 0.112, 0.135, 1.0)
@export var diagonal_cell_color: Color = Color(0.075, 0.082, 0.100, 1.0)
@export var grid_line_color: Color = Color(0.18, 0.20, 0.25, 0.45)
@export var family_separator_color: Color = Color(0.95, 0.96, 1.0, 0.78)
@export var topic_separator_color: Color = Color(0.95, 0.96, 1.0, 0.32)

@export var text_color: Color = Color(0.92, 0.94, 0.98, 1.0)
@export var muted_text_color: Color = Color(0.68, 0.72, 0.82, 1.0)
@export var panel_bg_color: Color = Color(0.045, 0.05, 0.065, 0.92)
@export var tooltip_bg_color: Color = Color(0.045, 0.05, 0.065, 0.96)
@export var hover_outline_color: Color = Color(1.0, 1.0, 1.0, 0.95)

@export var positive_lift_color: Color = Color(1.0, 0.73, 0.22, 1.0)
@export var neutral_lift_color: Color = Color(0.15, 0.16, 0.20, 1.0)
@export var negative_lift_color: Color = Color(0.25, 0.45, 1.0, 1.0)

@export var tooltip_width: float = 390.0
@export var tooltip_padding: float = 10.0

var _year_data: Dictionary = {}
var _year_label: String = ""

var _features: Dictionary = {}
var _pairs: Dictionary = {}
var _feature_order: Array = []
var _family_blocks: Array = []
var _topic_blocks: Array = []
var _subfamily_blocks: Array = []
var _score_ranges: Dictionary = {}
var _metric_range: Dictionary = {"min": 0.0, "max": 1.0, "abs_max": 1.0}

var _grid_rect: Rect2 = Rect2()
var _cell_size: float = 1.0
var _visible_feature_order: Array = []

var _hovered_cell: Dictionary = {}
var _mouse_pos: Vector2 = Vector2.ZERO


func _ready() -> void:
	mouse_filter = Control.MOUSE_FILTER_STOP
	custom_minimum_size = Vector2(620, 480)


func set_year_data(year_data: Dictionary) -> void:
	_year_data = year_data
	_year_label = str(year_data.get("year", ""))

	_features = year_data.get("features", {})
	_pairs = year_data.get("pairs", {})
	_feature_order = year_data.get("pair_feature_order", [])
	_family_blocks = year_data.get("family_blocks", [])
	_topic_blocks = year_data.get("topic_blocks", [])
	_subfamily_blocks = year_data.get("subfamily_blocks", [])
	_score_ranges = year_data.get("score_ranges", {})

	_visible_feature_order = _make_visible_feature_order(_feature_order)
	_metric_range = _get_metric_range(metric)

	_hovered_cell = {}
	queue_redraw()


func clear_chart() -> void:
	_year_data.clear()
	_features.clear()
	_pairs.clear()
	_feature_order.clear()
	_visible_feature_order.clear()
	_family_blocks.clear()
	_topic_blocks.clear()
	_subfamily_blocks.clear()
	_score_ranges.clear()
	_hovered_cell.clear()
	queue_redraw()


func set_metric(new_metric: String) -> void:
	metric = new_metric
	_metric_range = _get_metric_range(metric)
	queue_redraw()


func set_pair_scale_mode(new_scale_mode: PairScaleMode) -> void:
	pair_scale_mode = new_scale_mode
	queue_redraw()


func _gui_input(event: InputEvent) -> void:
	if event is InputEventMouseMotion:
		_mouse_pos = event.position
		_hovered_cell = _hit_test_cell(event.position)
		queue_redraw()

	elif event is InputEventMouseButton:
		if event.button_index == MOUSE_BUTTON_LEFT and event.pressed:
			_mouse_pos = event.position
			_hovered_cell = _hit_test_cell(event.position)
			queue_redraw()


func _draw() -> void:
	if _pairs.is_empty() or _visible_feature_order.is_empty():
		_draw_empty_state()
		return

	_compute_layout()

	_draw_background_panel()
	_draw_family_bands()
	_draw_heatmap_cells()
	_draw_separators()
	_draw_axis_labels()
	_draw_metric_legend()
	_draw_title()

	if not _hovered_cell.is_empty():
		_draw_hover_cell(_hovered_cell)
		_draw_tooltip(_hovered_cell, _mouse_pos)


# ==============================================================================
# Layout
# ==============================================================================

func _compute_layout() -> void:
	var available_w := size.x
	var available_h := size.y

	var x0 := left_axis_width
	var y0 := top_axis_height

	var grid_available_w := available_w - left_axis_width - right_legend_width - chart_padding
	var grid_available_h := available_h - top_axis_height - bottom_padding - chart_padding

	grid_available_w = max(1.0, grid_available_w)
	grid_available_h = max(1.0, grid_available_h)

	var n :int= max(1, _visible_feature_order.size())

	_cell_size = floor(min(grid_available_w, grid_available_h) / float(n))
	_cell_size = max(1.0, _cell_size)

	var grid_size := _cell_size * float(n)

	_grid_rect = Rect2(
		Vector2(x0, y0),
		Vector2(grid_size, grid_size)
	)


func _make_visible_feature_order(source_order: Array) -> Array:
	var out := source_order.duplicate()

	if max_visible_features > 0 and out.size() > max_visible_features:
		out = out.slice(0, max_visible_features)

	return out


# ==============================================================================
# Main drawing
# ==============================================================================

func _draw_background_panel() -> void:
	var panel_rect := Rect2(Vector2.ZERO, size)
	draw_rect(panel_rect, Color(0.035, 0.040, 0.052, 1.0), true)

	var chart_panel := Rect2(
		Vector2(8, 8),
		Vector2(size.x - 16, size.y - 16)
	)
	draw_rect(chart_panel, Color(0.055, 0.060, 0.075, 0.78), true)
	draw_rect(chart_panel, Color(0.20, 0.22, 0.28, 0.85), false, 1.0)

	draw_rect(_grid_rect, Color(0.075, 0.080, 0.100, 1.0), true)


func _draw_heatmap_cells() -> void:
	var n := _visible_feature_order.size()

	for row in range(n):
		var feature_a := str(_visible_feature_order[row])

		for col in range(n):
			if not draw_full_matrix and row > col:
				continue

			var feature_b := str(_visible_feature_order[col])
			var rect := _cell_rect(row, col)

			if feature_a == feature_b:
				if draw_diagonal:
					draw_rect(rect, diagonal_cell_color, true)
				else:
					draw_rect(
						rect,
						Color(diagonal_cell_color.r, diagonal_cell_color.g, diagonal_cell_color.b, 0.45),
						true
					)
				continue

			var key := _pair_key(feature_a, feature_b)

			if not _pairs.has(key):
				draw_rect(rect, missing_cell_color, true)
				continue

			var pair: Dictionary = _pairs[key]
			var value := _get_pair_metric_value(pair)
			var color := _metric_to_color(value)
			draw_rect(rect, color, true)

	if _cell_size >= min_cell_size_for_grid:
		_draw_light_grid_lines()


func _draw_light_grid_lines() -> void:
	var n := _visible_feature_order.size()

	for i in range(n + 1):
		var x := _grid_rect.position.x + float(i) * _cell_size
		var y := _grid_rect.position.y + float(i) * _cell_size

		draw_line(
			Vector2(x, _grid_rect.position.y),
			Vector2(x, _grid_rect.position.y + _grid_rect.size.y),
			grid_line_color,
			1.0
		)

		draw_line(
			Vector2(_grid_rect.position.x, y),
			Vector2(_grid_rect.position.x + _grid_rect.size.x, y),
			grid_line_color,
			1.0
		)


func _draw_family_bands() -> void:
	var visible_blocks := _remap_blocks_to_visible(_family_blocks)

	for block_variant in visible_blocks:
		var block: Dictionary = block_variant
		var name := str(block.get("name", "OTHER"))
		var start := int(block.get("start", 0))
		var end := int(block.get("end", 0))
		var count :int= max(1, end - start + 1)

		var color: Color = FAMILY_COLORS.get(name, FAMILY_COLORS["OTHER"])
		var x := _grid_rect.position.x + float(start) * _cell_size
		var y := _grid_rect.position.y + float(start) * _cell_size
		var w := float(count) * _cell_size

		var top_rect := Rect2(
			Vector2(x, _grid_rect.position.y - family_band_size - 4.0),
			Vector2(w, family_band_size)
		)

		var left_rect := Rect2(
			Vector2(_grid_rect.position.x - family_band_size - 4.0, y),
			Vector2(family_band_size, w)
		)

		draw_rect(top_rect, Color(color.r, color.g, color.b, 0.90), true)
		draw_rect(top_rect, Color(1, 1, 1, 0.25), false, 1.0)

		draw_rect(left_rect, Color(color.r, color.g, color.b, 0.90), true)
		draw_rect(left_rect, Color(1, 1, 1, 0.25), false, 1.0)

		_draw_block_label(name, top_rect, false)
		_draw_block_label(name, left_rect, true)


func _draw_block_label(label: String, rect: Rect2, vertical: bool) -> void:
	var font := get_theme_default_font()
	var font_size := 10

	var draw_label := label
	if draw_label.length() > 8:
		draw_label = draw_label.substr(0, 7) + "…"

	var text_size := font.get_string_size(draw_label, HORIZONTAL_ALIGNMENT_LEFT, -1, font_size)

	## The row band is a narrow vertical strip, so its label runs bottom-to-top.
	## Skip a label rather than let it spill out of its band.
	var along := rect.size.y if vertical else rect.size.x
	if along < text_size.x + 6.0:
		return

	var rot := -PI / 2.0 if vertical else 0.0

	draw_set_transform(rect.get_center(), rot, Vector2.ONE)
	draw_string(
		font,
		Vector2(-text_size.x * 0.5, font_size * 0.35),
		draw_label,
		HORIZONTAL_ALIGNMENT_LEFT,
		-1,
		font_size,
		Color(0.04, 0.05, 0.07, 0.9)
	)
	draw_set_transform(Vector2.ZERO, 0.0, Vector2.ONE)


func _draw_separators() -> void:
	_draw_block_separators(
		_remap_blocks_to_visible(_subfamily_blocks),
		Color(1, 1, 1, subfamily_separator_alpha),
		1.0
	)

	_draw_block_separators(
		_remap_blocks_to_visible(_topic_blocks),
		Color(topic_separator_color.r, topic_separator_color.g, topic_separator_color.b, topic_separator_alpha),
		1.0
	)

	_draw_block_separators(
		_remap_blocks_to_visible(_family_blocks),
		family_separator_color,
		2.0
	)

	draw_rect(_grid_rect, Color(0.95, 0.96, 1.0, 0.60), false, 1.5)


func _draw_block_separators(blocks: Array, color: Color, width: float) -> void:
	for block_variant in blocks:
		var block: Dictionary = block_variant

		var start := int(block.get("start", 0))
		var end := int(block.get("end", 0)) + 1

		for idx in [start, end]:
			var x := _grid_rect.position.x + float(idx) * _cell_size
			var y := _grid_rect.position.y + float(idx) * _cell_size

			draw_line(
				Vector2(x, _grid_rect.position.y),
				Vector2(x, _grid_rect.position.y + _grid_rect.size.y),
				color,
				width
			)

			draw_line(
				Vector2(_grid_rect.position.x, y),
				Vector2(_grid_rect.position.x + _grid_rect.size.x, y),
				color,
				width
			)


func _draw_axis_labels() -> void:
	var font := get_theme_default_font()
	var font_size := 10

	if _cell_size < 10.0:
		_draw_axis_summary_labels()
		return

	var n := _visible_feature_order.size()
	var label_every := 1

	if _cell_size < 16.0:
		label_every = 2
	if _cell_size < 11.0:
		label_every = 4

	for i in range(n):
		if i % label_every != 0:
			continue

		var feature := str(_visible_feature_order[i])
		var meta: Dictionary = _features.get(feature, {})
		var label := str(meta.get("label", feature))

		label = _truncate(label, 13)

		var left_pos := Vector2(
			_grid_rect.position.x - left_axis_width + 6.0,
			_grid_rect.position.y + float(i) * _cell_size + _cell_size * 0.72
		)

		if _cell_size >= 20.0:
			draw_string(
				font,
				left_pos,
				label,
				HORIZONTAL_ALIGNMENT_LEFT,
				left_axis_width - family_band_size - 12.0,
				font_size,
				muted_text_color
			)


func _draw_axis_summary_labels() -> void:
	## Intentionally empty: when cells are too small for per-feature labels the
	## colored family bands already label both axes, and the old
	## "Columns: grouped features" / "Rows" captions collided with the subtitle.
	pass


func _draw_metric_legend() -> void:
	var font := get_theme_default_font()
	var title_size := 12
	var body_size := 10

	var x := _grid_rect.position.x + _grid_rect.size.x + 16.0
	var y := _grid_rect.position.y

	var legend_rect := Rect2(
		Vector2(x, y),
		Vector2(right_legend_width - 24.0, 210.0)
	)

	draw_rect(legend_rect, panel_bg_color, true)
	draw_rect(legend_rect, Color(0.20, 0.22, 0.28, 0.9), false, 1.0)

	var metric_title := _metric_display_name(metric)
	draw_string(
		font,
		legend_rect.position + Vector2(10.0, 20.0),
		metric_title,
		HORIZONTAL_ALIGNMENT_LEFT,
		legend_rect.size.x - 20.0,
		title_size,
		text_color
	)

	var gradient_rect := Rect2(
		legend_rect.position + Vector2(18.0, 42.0),
		Vector2(18.0, 104.0)
	)

	_draw_vertical_gradient_legend(gradient_rect)

	var min_v := float(_metric_range.get("min", 0.0))
	var max_v := float(_metric_range.get("max", 1.0))
	var abs_max := float(_metric_range.get("abs_max", max(abs(min_v), abs(max_v))))

	var top_label := ""
	var mid_label := ""
	var bot_label := ""

	if metric == METRIC_LIFT:
		top_label = "+%.5f" % abs_max
		mid_label = "0"
		bot_label = "-%.5f" % abs_max
	else:
		top_label = "%.4f" % max_v
		mid_label = ""
		bot_label = "%.4f" % min_v

	draw_string(
		font,
		gradient_rect.position + Vector2(26.0, 8.0),
		top_label,
		HORIZONTAL_ALIGNMENT_LEFT,
		80.0,
		body_size,
		muted_text_color
	)

	if mid_label != "":
		draw_string(
			font,
			gradient_rect.position + Vector2(26.0, gradient_rect.size.y * 0.5 + 4.0),
			mid_label,
			HORIZONTAL_ALIGNMENT_LEFT,
			80.0,
			body_size,
			muted_text_color
		)

	draw_string(
		font,
		gradient_rect.position + Vector2(26.0, gradient_rect.size.y),
		bot_label,
		HORIZONTAL_ALIGNMENT_LEFT,
		80.0,
		body_size,
		muted_text_color
	)

	var n := _visible_feature_order.size()
	var pair_count := _pairs.size()

	var scale_label := "linear"

	match pair_scale_mode:
		PairScaleMode.LINEAR:
			scale_label = "linear"
		PairScaleMode.LOG_MAGNITUDE:
			scale_label = "log magnitude"
		PairScaleMode.LOG_ZOOMED:
			scale_label = "log zoomed"

	var summary := "%d features\n%d pair records" % [n, pair_count]

	if metric == METRIC_LIFT:
		summary += "\n%s scale" % scale_label

	var lines := summary.split("\n")
	var line_y := legend_rect.position.y + 166.0

	for line in lines:
		draw_string(
			font,
			Vector2(legend_rect.position.x + 10.0, line_y),
			line,
			HORIZONTAL_ALIGNMENT_LEFT,
			legend_rect.size.x - 20.0,
			body_size,
			muted_text_color
		)
		line_y += 14.0


func _draw_vertical_gradient_legend(rect: Rect2) -> void:
	var steps := 64

	for i in range(steps):
		var t0 := float(i) / float(steps)
		var y0 := rect.position.y + t0 * rect.size.y
		var y1 := rect.position.y + (float(i + 1) / float(steps)) * rect.size.y

		var value := 0.0

		if metric == METRIC_LIFT:
			value = _legend_t_to_lift_value(t0)
		else:
			var min_v := float(_metric_range.get("min", 0.0))
			var max_v := float(_metric_range.get("max", 1.0))
			value = lerp(max_v, min_v, t0)

		var color := _metric_to_color(value)

		draw_rect(
			Rect2(
				Vector2(rect.position.x, y0),
				Vector2(rect.size.x, y1 - y0 + 1.0)
			),
			color,
			true
		)

	draw_rect(rect, Color(1, 1, 1, 0.35), false, 1.0)


func _draw_title() -> void:
	var font := get_theme_default_font()
	var title_size := 14
	var subtitle_size := 10

	var title := "Pairwise Feature Heatmap"
	if _year_label != "":
		title += " " + _year_label

	var scale_label := ""

	if metric == METRIC_LIFT:
		match pair_scale_mode:
			PairScaleMode.LINEAR:
				scale_label = "linear"
			PairScaleMode.LOG_MAGNITUDE:
				scale_label = "log magnitude"
			PairScaleMode.LOG_ZOOMED:
				scale_label = "log zoomed"

	var subtitle := "cell = %s" % _metric_display_name(metric)

	if metric == METRIC_LIFT:
		## Lift = mean R² of recipes containing both features minus the baseline.
		subtitle = "Does a pair do better together? Orange = the pair raises R² above baseline, blue = lowers it. Blank = pair never sampled together."
	elif scale_label != "":
		subtitle += " | color = %s scale" % scale_label

	draw_string(
		font,
		Vector2(14.0, 24.0),
		title,
		HORIZONTAL_ALIGNMENT_LEFT,
		size.x - 28.0,
		title_size,
		text_color
	)

	draw_string(
		font,
		Vector2(14.0, 42.0),
		subtitle,
		HORIZONTAL_ALIGNMENT_LEFT,
		size.x - 28.0,
		subtitle_size,
		muted_text_color
	)


func _draw_hover_cell(cell: Dictionary) -> void:
	var row := int(cell.get("row", -1))
	var col := int(cell.get("col", -1))

	if row < 0 or col < 0:
		return

	var rect := _cell_rect(row, col)

	if _cell_size >= min_cell_size_for_hover_outline:
		draw_rect(rect, hover_outline_color, false, 2.0)

	var row_rect := Rect2(
		Vector2(_grid_rect.position.x, rect.position.y),
		Vector2(_grid_rect.size.x, _cell_size)
	)
	var col_rect := Rect2(
		Vector2(rect.position.x, _grid_rect.position.y),
		Vector2(_cell_size, _grid_rect.size.y)
	)

	draw_rect(row_rect, Color(1, 1, 1, 0.055), true)
	draw_rect(col_rect, Color(1, 1, 1, 0.055), true)
	draw_rect(rect, hover_outline_color, false, 2.0)


func _draw_empty_state() -> void:
	var font := get_theme_default_font()
	var message := "No pairwise training results loaded."
	var font_size := 14
	var text_size := font.get_string_size(message, HORIZONTAL_ALIGNMENT_CENTER, -1, font_size)

	draw_rect(Rect2(Vector2.ZERO, size), Color(0.035, 0.040, 0.052, 1.0), true)

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
# Tooltip
# ==============================================================================

func _draw_tooltip(cell: Dictionary, mouse_pos: Vector2) -> void:
	var font := get_theme_default_font()
	var title_size := 13
	var body_size := 11
	var line_h := 17.0

	var lines := _tooltip_lines(cell)
	var title := _tooltip_title(cell)

	## Size to content so full Census paths aren't clipped.
	var content_w := font.get_string_size(title, HORIZONTAL_ALIGNMENT_LEFT, -1, title_size).x
	for line in lines:
		content_w = max(content_w, font.get_string_size(line, HORIZONTAL_ALIGNMENT_LEFT, -1, body_size).x)

	var tip_w: float = clamp(content_w + tooltip_padding * 2.0 + 16.0, 240.0, max(240.0, size.x - 16.0))

	var tooltip_h := tooltip_padding * 2.0 + 20.0 + line_h * float(lines.size())
	var pos := mouse_pos + Vector2(18.0, 18.0)

	if pos.x + tip_w > size.x:
		pos.x = mouse_pos.x - tip_w - 18.0

	if pos.y + tooltip_h > size.y:
		pos.y = size.y - tooltip_h - 8.0

	pos.x = clamp(pos.x, 8.0, max(8.0, size.x - tip_w - 8.0))
	pos.y = clamp(pos.y, 8.0, max(8.0, size.y - tooltip_h - 8.0))

	var rect := Rect2(pos, Vector2(tip_w, tooltip_h))

	draw_rect(rect, tooltip_bg_color, true)
	draw_rect(rect, Color(0.28, 0.31, 0.40, 0.95), false, 1.0)

	var feature_a := str(cell.get("feature_a", ""))
	var meta_a: Dictionary = _features.get(feature_a, {})
	var family_a := str(meta_a.get("group", "OTHER"))
	var family_color: Color = FAMILY_COLORS.get(family_a, FAMILY_COLORS["OTHER"])

	draw_rect(Rect2(pos, Vector2(5.0, tooltip_h)), family_color, true)

	var title_pos := pos + Vector2(tooltip_padding + 8.0, tooltip_padding + 11.0)

	draw_string(
		font,
		title_pos,
		title,
		HORIZONTAL_ALIGNMENT_LEFT,
		tip_w - 24.0,
		title_size,
		text_color
	)

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


## "15 to 19 years  ×  Total population": the two leaf names being compared.
func _tooltip_title(cell: Dictionary) -> String:
	var feature_a := str(cell.get("feature_a", ""))
	var feature_b := str(cell.get("feature_b", ""))

	var meta_a: Dictionary = _features.get(feature_a, {})
	var meta_b: Dictionary = _features.get(feature_b, {})

	return "%s  ×  %s" % [
		str(meta_a.get("label", feature_a)),
		str(meta_b.get("label", feature_b)),
	]


func _tooltip_lines(cell: Dictionary) -> Array[String]:
	var lines: Array[String] = []

	var feature_a := str(cell.get("feature_a", ""))
	var feature_b := str(cell.get("feature_b", ""))

	var meta_a: Dictionary = _features.get(feature_a, {})
	var meta_b: Dictionary = _features.get(feature_b, {})

	## Full Census context for each side; the family is already shown by color.
	lines.append("A  %s  (%s)" % [_context_path(meta_a, feature_a), feature_a])
	lines.append("B  %s  (%s)" % [_context_path(meta_b, feature_b), feature_b])

	var pair: Dictionary = cell.get("pair", {})

	if pair.is_empty():
		if feature_a == feature_b:
			lines.append("Same feature")
		else:
			lines.append("Never sampled together")
		return lines

	var lift_value := float(pair.get("lift", 0.0))

	lines.append("Lift %+.4f: pair R² %.4f vs baseline %.4f" % [
		lift_value,
		float(pair.get("mean_score_r2", 0.0)),
		float(pair.get("baseline_score_r2", 0.0)),
	])
	lines.append("Sampled together in %d recipes" % int(pair.get("co_occurrence_count", 0)))

	return lines


## "Sex and age › 15 to 19 years": hierarchy path without the family code.
func _context_path(meta: Dictionary, fallback: String) -> String:
	var path: Array = meta.get("path", [])
	var parts: Array[String] = []

	for part in path:
		var s := str(part)
		if s == "" or (s.begins_with("DP") and s.length() == 4):
			continue
		if s == s.to_upper():
			s = s.to_lower()
			s = s.substr(0, 1).to_upper() + s.substr(1)
		parts.append(s)

	if parts.is_empty():
		return str(meta.get("label", fallback))

	return " › ".join(parts)


# ==============================================================================
# Hit testing
# ==============================================================================

func _hit_test_cell(pos: Vector2) -> Dictionary:
	if not _grid_rect.has_point(pos):
		return {}

	if _cell_size <= 0.0:
		return {}

	var col := int(floor((pos.x - _grid_rect.position.x) / _cell_size))
	var row := int(floor((pos.y - _grid_rect.position.y) / _cell_size))

	var n := _visible_feature_order.size()

	if row < 0 or row >= n or col < 0 or col >= n:
		return {}

	var feature_a := str(_visible_feature_order[row])
	var feature_b := str(_visible_feature_order[col])
	var key := _pair_key(feature_a, feature_b)

	return {
		"row": row,
		"col": col,
		"feature_a": feature_a,
		"feature_b": feature_b,
		"key": key,
		"pair": _pairs.get(key, {})
	}


func _cell_rect(row: int, col: int) -> Rect2:
	return Rect2(
		Vector2(
			_grid_rect.position.x + float(col) * _cell_size,
			_grid_rect.position.y + float(row) * _cell_size
		),
		Vector2(_cell_size, _cell_size)
	)


# ==============================================================================
# Blocks / axis helpers
# ==============================================================================

func _remap_blocks_to_visible(blocks: Array) -> Array:
	var max_index := _visible_feature_order.size() - 1
	var out: Array = []

	if max_index < 0:
		return out

	for block_variant in blocks:
		if typeof(block_variant) != TYPE_DICTIONARY:
			continue

		var block: Dictionary = block_variant

		var start := int(block.get("start", 0))
		var end := int(block.get("end", 0))

		if start > max_index:
			continue

		end = min(end, max_index)

		if end < start:
			continue

		var copy := block.duplicate(true)
		copy["start"] = start
		copy["end"] = end
		copy["count"] = end - start + 1
		out.append(copy)

	return out


# ==============================================================================
# Metric and colors
# ==============================================================================

func _get_pair_metric_value(pair: Dictionary) -> float:
	match metric:
		METRIC_LIFT:
			return float(pair.get("lift", 0.0))
		METRIC_PAIR_R2:
			return float(pair.get("mean_score_r2", 0.0))
		METRIC_BASELINE_R2:
			return float(pair.get("baseline_score_r2", 0.0))
		METRIC_COUNT:
			return float(pair.get("co_occurrence_count", 0))
		_:
			return float(pair.get(metric, 0.0))


func _get_metric_range(metric_name: String) -> Dictionary:
	match metric_name:
		METRIC_LIFT:
			return _score_ranges.get("pair_lift", {"min": 0.0, "max": 1.0, "abs_max": 1.0})
		METRIC_PAIR_R2:
			return _score_ranges.get("pair_mean_score_r2", {"min": 0.0, "max": 1.0})
		METRIC_BASELINE_R2:
			return _score_ranges.get("pair_baseline_score_r2", {"min": 0.0, "max": 1.0})
		METRIC_COUNT:
			return _score_ranges.get("co_occurrence_count", {"min": 0.0, "max": 1.0})
		_:
			return {"min": 0.0, "max": 1.0, "abs_max": 1.0}


func _metric_to_color(value: float) -> Color:
	if metric == METRIC_LIFT:
		return _lift_to_color(value)

	var min_v := float(_metric_range.get("min", 0.0))
	var max_v := float(_metric_range.get("max", 1.0))

	var t := 0.5
	if abs(max_v - min_v) > 0.000001:
		t = inverse_lerp(min_v, max_v, value)

	t = clamp(t, 0.0, 1.0)

	var low := Color(0.11, 0.13, 0.17, 1.0)
	var high := Color(0.35, 0.78, 1.0, 1.0)

	if metric == METRIC_COUNT:
		high = Color(0.72, 0.88, 0.45, 1.0)

	return low.lerp(high, t)


func _lift_to_color(value: float) -> Color:
	var t := _lift_to_signed_intensity(value)

	if t < 0.0:
		return neutral_lift_color.lerp(negative_lift_color, abs(t))

	return neutral_lift_color.lerp(positive_lift_color, t)


func _lift_to_signed_intensity(value: float) -> float:
	if is_equal_approx(value, 0.0):
		return 0.0

	var sign := signf(value)
	var magnitude :float= abs(value)

	match pair_scale_mode:
		PairScaleMode.LINEAR:
			return sign * _lift_linear_intensity(magnitude)

		PairScaleMode.LOG_MAGNITUDE:
			return sign * _lift_log_intensity(magnitude, 0.0)

		PairScaleMode.LOG_ZOOMED:
			return sign * _lift_log_zoomed_intensity(magnitude)

	return sign * _lift_log_intensity(magnitude, 0.0)


func _lift_linear_intensity(magnitude: float) -> float:
	var abs_max := float(_metric_range.get("abs_max", 1.0))
	abs_max = max(abs_max, 0.000001)

	return clamp(magnitude / abs_max, 0.0, 1.0)


func _lift_log_intensity(magnitude: float, min_magnitude: float = 0.0) -> float:
	var abs_max := float(_metric_range.get("abs_max", 1.0))
	abs_max = max(abs_max, lift_log_epsilon)

	var min_score := _log_lift_score(min_magnitude)
	var max_score := _log_lift_score(abs_max)
	var score := _log_lift_score(magnitude)

	if is_equal_approx(max_score, min_score):
		return 1.0

	return clamp(inverse_lerp(min_score, max_score, score), 0.0, 1.0)


func _lift_log_zoomed_intensity(magnitude: float) -> float:
	var min_nonzero := _visible_min_nonzero_lift_magnitude()
	var min_source :float= max(0.0, min_nonzero * lift_zoom_padding_ratio)

	return _lift_log_intensity(magnitude, min_source)


func _log_lift_score(magnitude: float) -> float:
	var safe_mag :float= max(0.0, magnitude)
	var eps :float= max(lift_log_epsilon, 0.000000001)

	return log(1.0 + safe_mag / eps) / log(10.0)


func _visible_min_nonzero_lift_magnitude() -> float:
	var min_mag := INF

	for pair_variant in _pairs.values():
		if typeof(pair_variant) != TYPE_DICTIONARY:
			continue

		var pair: Dictionary = pair_variant
		var a := str(pair.get("feature_a", ""))
		var b := str(pair.get("feature_b", ""))

		if not _visible_feature_order.has(a):
			continue
		if not _visible_feature_order.has(b):
			continue

		var lift :float= abs(float(pair.get("lift", 0.0)))
		if lift > 0.0:
			min_mag = min(min_mag, lift)

	if min_mag == INF:
		return 0.0

	return min_mag


func _legend_t_to_lift_value(t: float) -> float:
	var abs_max := float(_metric_range.get("abs_max", 1.0))
	abs_max = max(abs_max, lift_log_epsilon)

	if pair_scale_mode == PairScaleMode.LINEAR:
		return lerp(abs_max, -abs_max, t)

	var sign := 1.0
	var local_t := 0.0

	if t < 0.5:
		sign = 1.0
		local_t = inverse_lerp(0.5, 0.0, t)
	else:
		sign = -1.0
		local_t = inverse_lerp(0.5, 1.0, t)

	local_t = clamp(local_t, 0.0, 1.0)

	var min_mag := 0.0
	if pair_scale_mode == PairScaleMode.LOG_ZOOMED:
		min_mag = _visible_min_nonzero_lift_magnitude() * lift_zoom_padding_ratio

	var min_score := _log_lift_score(min_mag)
	var max_score := _log_lift_score(abs_max)
	var score :float= lerp(min_score, max_score, local_t)

	var eps :float= max(lift_log_epsilon, 0.000000001)
	var magnitude :float= (pow(10.0, score) - 1.0) * eps

	return sign * magnitude


func _metric_display_name(metric_name: String) -> String:
	match metric_name:
		METRIC_LIFT:
			return "Pairwise Lift"
		METRIC_PAIR_R2:
			return "Pair Mean R²"
		METRIC_BASELINE_R2:
			return "Baseline R²"
		METRIC_COUNT:
			return "Co-occurrence"
		_:
			return metric_name.capitalize()


# ==============================================================================
# General helpers
# ==============================================================================

func _pair_key(a: String, b: String) -> String:
	if a < b:
		return a + "||" + b

	return b + "||" + a


func _path_to_string(path: Array) -> String:
	var parts: Array[String] = []

	for part in path:
		var s := str(part)
		if s != "":
			parts.append(s)

	return " > ".join(parts)


func _truncate(value: String, max_chars: int) -> String:
	if value.length() <= max_chars:
		return value

	return value.substr(0, max_chars - 1) + "…"


func print_debug_summary() -> void:
	print("--- PairwiseHeatmapChart Debug ---")
	print("Year: ", _year_label)
	print("Features dictionary: ", _features.size())
	print("Pairs: ", _pairs.size())
	print("Feature order: ", _feature_order.size())
	print("Visible features: ", _visible_feature_order.size())
	print("Family blocks: ", _family_blocks.size())
	print("Topic blocks: ", _topic_blocks.size())
	print("Subfamily blocks: ", _subfamily_blocks.size())
	print("Metric: ", metric)
	print("Pair scale mode: ", pair_scale_mode)
	print("Metric range: ", _metric_range)
	print("Visible min nonzero lift: ", _visible_min_nonzero_lift_magnitude())
