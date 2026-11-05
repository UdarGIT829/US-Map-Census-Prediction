extends Control
class_name ScreeningSingleBarChart

signal feature_selected(row: Dictionary)

enum ScaleMode {
	LINEAR_ABSOLUTE,
	LINEAR_ZOOMED,
	LOG_DISTANCE_TO_ONE,
}

@onready var DP02_ColorRect :ColorRect= $"../../LegendContainer/HBoxContainer/DP02-VBox/ColorRect"
@onready var DP03_ColorRect :ColorRect= $"../../LegendContainer/HBoxContainer/DP03-VBox/ColorRect"
@onready var DP04_ColorRect :ColorRect= $"../../LegendContainer/HBoxContainer/DP04-VBox/ColorRect"
@onready var DP05_ColorRect :ColorRect= $"../../LegendContainer/HBoxContainer/DP05-VBox/ColorRect"


@export var row_height: float = 28.0
@export var bar_height: float = 16.0

@export var label_width: float = 420.0
@export var value_width: float = 72.0

@export var top_padding: float = 44.0
@export var bottom_padding: float = 18.0
@export var left_padding: float = 16.0
@export var right_padding: float = 16.0

@export var minimum_chart_width: float = 900.0
@export var minimum_empty_height: float = 240.0

@export var scale_mode: ScaleMode = ScaleMode.LOG_DISTANCE_TO_ONE

## Used only for LINEAR_ZOOMED.
@export var zoom_padding_below_threshold: float = 0.02

## Avoids log(0) when abs_r is exactly 1.0.
@export var log_epsilon: float = 0.000001

var rows: Array = []
var threshold: float = 0.0

var hovered_index: int = -1
var row_hitboxes: Array[Rect2] = []

var font: Font


const FAMILY_COLORS := {
	"DP02": Color("#6EA8FE"),
	"DP03": Color("#75D69C"),
	"DP04": Color("#F2C66D"),
	"DP05": Color("#D38CFF"),
	"OTHER": Color("#AAB2C0"),
}


func _ready() -> void:
	font = get_theme_default_font()

	mouse_filter = Control.MOUSE_FILTER_STOP
	size_flags_horizontal = Control.SIZE_EXPAND_FILL
	size_flags_vertical = Control.SIZE_SHRINK_BEGIN

	custom_minimum_size = Vector2(minimum_chart_width, minimum_empty_height)
	set_legend()

	## Non-empty tooltip_text enables _get_tooltip(); the real text is per-row.
	tooltip_text = " "


func _make_custom_tooltip(for_text: String) -> Object:
	return ChartTooltip.make(for_text)


## Full, un-elided label for the hovered row (labels are middle-elided to fit).
func _get_tooltip(at_position: Vector2) -> String:
	var idx := _row_index_at(at_position)

	if idx < 0 or idx >= rows.size():
		return ""

	var row: Dictionary = rows[idx]
	var feature := str(row.get("feature", ""))

	return "%s\n%s  ·  |r| = %.4f  ·  %s" % [
		str(row.get("label", feature)),
		feature,
		float(row.get("abs_r", 0.0)),
		"passes FDR" if bool(row.get("passes_fdr", false)) else "fails FDR",
	]


func set_legend() -> void:
	DP02_ColorRect.color = FAMILY_COLORS.get("DP02")
	DP03_ColorRect.color = FAMILY_COLORS.get("DP03")
	DP04_ColorRect.color = FAMILY_COLORS.get("DP04")
	DP05_ColorRect.color = FAMILY_COLORS.get("DP05")


func set_data(new_rows: Array, new_threshold: float) -> void:
	rows = new_rows.duplicate()
	threshold = new_threshold

	rows.sort_custom(
		func(a: Dictionary, b: Dictionary) -> bool:
			return float(a.get("abs_r", 0.0)) > float(b.get("abs_r", 0.0))
	)

	var wanted_h :float= top_padding + bottom_padding + max(rows.size(), 1) * row_height

	custom_minimum_size = Vector2(
		max(minimum_chart_width, custom_minimum_size.x),
		max(minimum_empty_height, wanted_h)
	)

	queue_redraw()


func _draw() -> void:
	if rows.is_empty():
		_draw_empty()
		return

	row_hitboxes.clear()

	var plot_x := left_padding + label_width
	var plot_y := top_padding
	var plot_w := size.x - plot_x - value_width - right_padding
	var plot_h := rows.size() * row_height

	if plot_w <= 20.0:
		return

	_draw_axis(plot_x, plot_y, plot_w, plot_h)

	for i in rows.size():
		_draw_row(i, plot_x, plot_y + i * row_height, plot_w)

	_draw_threshold(plot_x, plot_y, plot_w, plot_h)


func _draw_empty() -> void:
	draw_string(
		font,
		Vector2(16, 32),
		"No screening rows to display.",
		HORIZONTAL_ALIGNMENT_LEFT,
		-1,
		14,
		Color(0.85, 0.87, 0.92, 1.0)
	)


func _draw_axis(plot_x: float, plot_y: float, plot_w: float, plot_h: float) -> void:
	var label := ""

	#match scale_mode:
		#ScaleMode.LINEAR_ABSOLUTE:
			#label = "|r| scale: 0.000–1.000"
#
		#ScaleMode.LINEAR_ZOOMED:
			#label = "|r| scale: %.3f–1.000" % _linear_display_min()
#
		#ScaleMode.LOG_DISTANCE_TO_ONE:
			#label = "log closeness scale: -log10(1-|r|)"
#
	#draw_string(
		#font,
		#Vector2(plot_x, 24),
		#label,
		#HORIZONTAL_ALIGNMENT_LEFT,
		#plot_w,
		#14,
		#Color(0.92, 0.94, 0.98, 1.0)
	#)

	draw_line(
		Vector2(plot_x, plot_y + plot_h),
		Vector2(plot_x + plot_w, plot_y + plot_h),
		Color(1, 1, 1, 0.18),
		1.0
	)


func _draw_row(index: int, plot_x: float, y: float, plot_w: float) -> void:
	var row :Dictionary= rows[index]

	var abs_r := float(row.get("abs_r", 0.0))
	var feature := str(row.get("feature", ""))
	var family := str(row.get("family", _infer_family(feature)))
	var label := str(row.get("label", feature))
	var passes := bool(row.get("passes_fdr", false))

	var row_rect := Rect2(0, y, size.x, row_height)
	row_hitboxes.append(row_rect)

	if index == hovered_index:
		draw_rect(row_rect, Color(1, 1, 1, 0.065))

	var color := _family_color(family)
	if not passes:
		color.a = 0.35

	## Family is shown by bar color + legend, so the full width goes to the label.
	var label_text := _fit(label, label_width - 8, 14)

	draw_string(
		font,
		Vector2(left_padding, y + 19),
		label_text,
		HORIZONTAL_ALIGNMENT_LEFT,
		label_width - 8,
		14,
		Color(0.88, 0.90, 0.95, 1.0)
	)

	var bar_x0 := plot_x
	var bar_x1 := _value_to_x(abs_r, plot_x, plot_w)
	var bar_w :float= max(1.0, bar_x1 - bar_x0)

	var bar_y := y + (row_height - bar_height) * 0.5

	draw_rect(
		Rect2(bar_x0, bar_y, bar_w, bar_height),
		color
	)

	var value_text := "%.4f" % abs_r

	draw_string(
		font,
		Vector2(plot_x + plot_w + 10, y + 19),
		value_text,
		HORIZONTAL_ALIGNMENT_LEFT,
		value_width,
		14,
		Color(0.92, 0.94, 0.98, 1.0)
	)


func _draw_threshold(plot_x: float, plot_y: float, plot_w: float, plot_h: float) -> void:
	if threshold <= 0.0:
		return

	var x := _value_to_x(threshold, plot_x, plot_w)

	draw_line(
		Vector2(x, plot_y - 6),
		Vector2(x, plot_y + plot_h),
		Color(1, 1, 1, 0.55),
		2.0
	)

	draw_string(
		font,
		Vector2(x + 4, plot_y - 11),
		"threshold %.3f" % threshold,
		HORIZONTAL_ALIGNMENT_LEFT,
		180,
		12,
		Color(1, 1, 1, 0.78)
	)


func _gui_input(event: InputEvent) -> void:
	if event is InputEventMouseMotion:
		var idx := _row_index_at(event.position)

		if hovered_index != idx:
			hovered_index = idx
			queue_redraw()

	elif event is InputEventMouseButton:
		if event.pressed and event.button_index == MOUSE_BUTTON_LEFT:
			var idx := _row_index_at(event.position)

			if idx >= 0 and idx < rows.size():
				feature_selected.emit(rows[idx])


func _row_index_at(pos: Vector2) -> int:
	for i in row_hitboxes.size():
		if row_hitboxes[i].has_point(pos):
			return i

	return -1


# ---------------------------------------------------------------------------
# Scaling
# ---------------------------------------------------------------------------

func _value_to_x(value: float, plot_x: float, plot_w: float) -> float:
	var t := 0.0

	match scale_mode:
		ScaleMode.LINEAR_ABSOLUTE:
			t = clamp(value, 0.0, 1.0)

		ScaleMode.LINEAR_ZOOMED:
			var min_v := _linear_display_min()
			t = inverse_lerp(min_v, 1.0, value)
			t = clamp(t, 0.0, 1.0)

		ScaleMode.LOG_DISTANCE_TO_ONE:
			t = _log_distance_t(value)

	return plot_x + t * plot_w


func _linear_display_min() -> float:
	if threshold > 0.0:
		return max(0.0, threshold - zoom_padding_below_threshold)

	var min_r := 1.0

	for row in rows:
		min_r = min(min_r, float(row.get("abs_r", 0.0)))

	return max(0.0, min_r - zoom_padding_below_threshold)


func _log_distance_t(value: float) -> float:
	var min_source := _log_scale_min_source()
	var max_source := _log_scale_max_source()

	var min_v := _log_distance_score(min_source)
	var max_v := _log_distance_score(max_source)
	var score := _log_distance_score(value)

	if is_equal_approx(max_v, min_v):
		return 1.0

	return clamp(inverse_lerp(min_v, max_v, score), 0.0, 1.0)


func _log_scale_min_source() -> float:
	## Anchor the left side near the threshold so the threshold line is meaningful.
	if threshold > 0.0:
		return max(0.0, threshold)

	var min_r := 1.0

	for row in rows:
		min_r = min(min_r, float(row.get("abs_r", 0.0)))

	return clamp(min_r, 0.0, 1.0 - log_epsilon)


func _log_scale_max_source() -> float:
	var max_r := 0.0

	for row in rows:
		max_r = max(max_r, float(row.get("abs_r", 0.0)))

	## Do not use exactly 1.0 because -log10(0) is infinite.
	return clamp(max_r, 0.0, 1.0 - log_epsilon)


func _log_distance_score(value: float) -> float:
	var safe_value :float= clamp(value, 0.0, 1.0 - log_epsilon)
	var distance_from_one :float= max(1.0 - safe_value, log_epsilon)

	return -log(distance_from_one) / log(10.0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

func _family_color(family: String) -> Color:
	return FAMILY_COLORS.get(family, FAMILY_COLORS["OTHER"])


func _infer_family(feature: String) -> String:
	if feature.contains("_"):
		return feature.split("_")[0]

	if feature.length() >= 4 and feature.begins_with("DP"):
		return feature.substr(0, 4)

	return "OTHER"


## Middle-elide text until it fits max_width pixels at font_size.
func _fit(text: String, max_width: float, font_size: int) -> String:
	if font.get_string_size(text, HORIZONTAL_ALIGNMENT_LEFT, -1, font_size).x <= max_width:
		return text

	var lo := 4
	var hi := text.length()

	while lo < hi:
		var mid := int((lo + hi + 1) / 2.0)
		if font.get_string_size(_shorten(text, mid), HORIZONTAL_ALIGNMENT_LEFT, -1, font_size).x <= max_width:
			lo = mid
		else:
			hi = mid - 1

	return _shorten(text, lo)


## Elide the middle so both the section name and the leaf category stay visible.
func _shorten(text: String, max_chars: int) -> String:
	if text.length() <= max_chars:
		return text

	var head := int((max_chars - 1) * 0.4)
	var tail := max_chars - 1 - head

	return text.substr(0, head).strip_edges() + "…" + text.substr(text.length() - tail).strip_edges()
