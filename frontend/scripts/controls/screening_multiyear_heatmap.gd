extends Control
class_name ScreeningMultiYearHeatmap

signal feature_selected(row: Dictionary)
signal feature_year_selected(row: Dictionary, year: int, cell: Dictionary)

enum ScaleMode {
	LINEAR_ABSOLUTE,
	LINEAR_ZOOMED,
	LOG_DISTANCE_TO_ONE,
}

@export var row_height: float = 30.0
@export var header_height: float = 42.0

@export var label_width: float = 360.0
@export var cell_width: float = 86.0
@export var summary_width: float = 96.0

@export var cell_height: float = 22.0
@export var cell_gap: float = 6.0

@export var top_padding: float = 12.0
@export var bottom_padding: float = 18.0
@export var left_padding: float = 16.0
@export var right_padding: float = 16.0

@export var minimum_chart_width: float = 900.0
@export var minimum_empty_height: float = 240.0

@export var max_rows: int = 120
@export var pass_only: bool = true

## Optional filter. Use "All" to show all ACS families.
@export var family_filter: String = "All"

## Since this report has many values clustered near 1.0, this should usually stay on.
@export var scale_mode: ScaleMode = ScaleMode.LOG_DISTANCE_TO_ONE

## Used only for LINEAR_ZOOMED.
@export var zoom_padding_below_min: float = 0.02

## Avoids log(0) when abs_r is exactly 1.0.
@export var log_epsilon: float = 0.000001

var report: Dictionary = {}

var available_years: Array[int] = []
var rows: Array[Dictionary] = []

var hovered_row_index: int = -1
var hovered_year: int = -1

var row_hitboxes: Array[Rect2] = []
var cell_hitboxes: Array[Dictionary] = []

var font: Font

const FAMILY_COLORS := {
	"DP02": Color("#6EA8FE"),
	"DP03": Color("#75D69C"),
	"DP04": Color("#F2C66D"),
	"DP05": Color("#D38CFF"),
	"OTHER": Color("#AAB2C0"),
}

const PASS_CELL_BASE := Color("#5FD38D")
const FAIL_CELL_BASE := Color("#AAB2C0")
const EMPTY_CELL_COLOR := Color(1, 1, 1, 0.055)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

func _ready() -> void:
	font = get_theme_default_font()

	mouse_filter = Control.MOUSE_FILTER_STOP
	size_flags_horizontal = Control.SIZE_EXPAND_FILL
	size_flags_vertical = Control.SIZE_SHRINK_BEGIN

	custom_minimum_size = Vector2(minimum_chart_width, minimum_empty_height)

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

	return "%s\n%s  ·  mean |r| = %.4f  ·  passes FDR in %d/%d years" % [
		str(row.get("label", feature)),
		feature,
		float(row.get("mean_abs_r", 0.0)),
		int(row.get("stable_count", 0)),
		available_years.size(),
	]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

func set_report(new_report: Dictionary) -> void:
	report = new_report.duplicate(true)

	available_years = _extract_years(report)
	rows = _build_heatmap_rows()

	_update_minimum_size()
	queue_redraw()


func clear_report() -> void:
	report.clear()
	available_years.clear()
	rows.clear()

	hovered_row_index = -1
	hovered_year = -1

	row_hitboxes.clear()
	cell_hitboxes.clear()

	custom_minimum_size = Vector2(minimum_chart_width, minimum_empty_height)
	queue_redraw()


func set_family_filter(new_family_filter: String) -> void:
	family_filter = new_family_filter
	rows = _build_heatmap_rows()

	_update_minimum_size()
	queue_redraw()


func set_pass_only(new_pass_only: bool) -> void:
	pass_only = new_pass_only
	rows = _build_heatmap_rows()

	_update_minimum_size()
	queue_redraw()


func set_max_rows(new_max_rows: int) -> void:
	max_rows = max(1, new_max_rows)
	rows = _build_heatmap_rows()

	_update_minimum_size()
	queue_redraw()


# ---------------------------------------------------------------------------
# Drawing
# ---------------------------------------------------------------------------

func _draw() -> void:
	if rows.is_empty() or available_years.is_empty():
		_draw_empty()
		return

	row_hitboxes.clear()
	cell_hitboxes.clear()

	var chart_x := left_padding
	var chart_y := top_padding

	_draw_title(chart_x, chart_y)
	_draw_header(chart_x, chart_y + header_height)

	var first_row_y := chart_y + header_height + row_height

	for i in rows.size():
		var row_y := first_row_y + i * row_height
		_draw_row(i, chart_x, row_y)


func _draw_empty() -> void:
	draw_string(
		font,
		Vector2(16, 32),
		"No multiyear screening rows to display.",
		HORIZONTAL_ALIGNMENT_LEFT,
		-1,
		14,
		Color(0.85, 0.87, 0.92, 1.0)
	)


func _draw_title(x: float, y: float) -> void:
	var mode_label := ""

	match scale_mode:
		ScaleMode.LINEAR_ABSOLUTE:
			mode_label = "|r| linear scale"

		ScaleMode.LINEAR_ZOOMED:
			mode_label = "|r| zoomed scale"

		ScaleMode.LOG_DISTANCE_TO_ONE:
			mode_label = "log closeness scale: -log10(1-|r|)"

	var title := "Final-round multiyear correlation heatmap"

	draw_string(
		font,
		Vector2(x, y + 16),
		title,
		HORIZONTAL_ALIGNMENT_LEFT,
		500,
		15,
		Color(0.94, 0.95, 0.98, 1.0)
	)

	#draw_string(
		#font,
		#Vector2(x, y + 34),
		#mode_label,
		#HORIZONTAL_ALIGNMENT_LEFT,
		#500,
		#12,
		#Color(0.72, 0.76, 0.84, 1.0)
	#)


func _draw_header(x: float, y: float) -> void:
	var label_x := x
	var cells_x := _cells_start_x()

	draw_string(
		font,
		Vector2(label_x, y + 20),
		"Feature",
		HORIZONTAL_ALIGNMENT_LEFT,
		label_width,
		13,
		Color(0.80, 0.84, 0.92, 1.0)
	)

	for year_index in available_years.size():
		var year := available_years[year_index]
		var cell_x := cells_x + year_index * cell_width

		draw_string(
			font,
			Vector2(cell_x, y + 20),
			str(year),
			HORIZONTAL_ALIGNMENT_CENTER,
			cell_width - cell_gap,
			13,
			Color(0.80, 0.84, 0.92, 1.0)
		)

	var mean_x := _mean_start_x()

	draw_string(
		font,
		Vector2(mean_x, y + 20),
		"Mean",
		HORIZONTAL_ALIGNMENT_LEFT,
		summary_width,
		13,
		Color(0.80, 0.84, 0.92, 1.0)
	)

	var stable_x := _stable_start_x()

	draw_string(
		font,
		Vector2(stable_x, y + 20),
		"Stable",
		HORIZONTAL_ALIGNMENT_LEFT,
		summary_width,
		13,
		Color(0.80, 0.84, 0.92, 1.0)
	)

	var line_y := y + row_height - 3

	draw_line(
		Vector2(x, line_y),
		Vector2(size.x - right_padding, line_y),
		Color(1, 1, 1, 0.16),
		1.0
	)


func _draw_row(index: int, x: float, y: float) -> void:
	var row := rows[index]

	var row_rect := Rect2(0, y, size.x, row_height)
	row_hitboxes.append(row_rect)

	if index == hovered_row_index:
		draw_rect(row_rect, Color(1, 1, 1, 0.06))

	var family := str(row.get("family", "OTHER"))
	var feature := str(row.get("feature", ""))
	var label := str(row.get("label", feature))
	var stable_count := int(row.get("stable_count", 0))
	var mean_abs_r := float(row.get("mean_abs_r", 0.0))

	var family_color := _family_color(family)

	var marker_rect := Rect2(
		left_padding,
		y + 6,
		5,
		row_height - 12
	)

	draw_rect(marker_rect, family_color)

	## Family is shown by the color marker, so the full width goes to the label.
	var label_text := _fit(label, label_width - 18, 13)

	draw_string(
		font,
		Vector2(left_padding + 12, y + 20),
		label_text,
		HORIZONTAL_ALIGNMENT_LEFT,
		label_width - 18,
		13,
		Color(0.88, 0.90, 0.95, 1.0)
	)

	var cells_x := _cells_start_x()
	var years_dict: Dictionary = row.get("years", {})

	for year_index in available_years.size():
		var year := available_years[year_index]
		var cell_x := cells_x + year_index * cell_width
		var cell_rect := Rect2(
			cell_x,
			y + (row_height - cell_height) * 0.5,
			cell_width - cell_gap,
			cell_height
		)

		var cell: Dictionary = {}

		if years_dict.has(year):
			cell = years_dict[year]
		elif years_dict.has(str(year)):
			cell = years_dict[str(year)]

		var is_hovered_cell := index == hovered_row_index and year == hovered_year

		_draw_cell(cell_rect, cell, is_hovered_cell)

		cell_hitboxes.append({
			"rect": cell_rect,
			"row_index": index,
			"year": year,
		})

	var mean_x := _mean_start_x()

	draw_string(
		font,
		Vector2(mean_x, y + 20),
		"%.4f" % mean_abs_r,
		HORIZONTAL_ALIGNMENT_LEFT,
		summary_width,
		13,
		Color(0.92, 0.94, 0.98, 1.0)
	)

	var stable_x := _stable_start_x()
	var stable_text := "%d/%d" % [stable_count, available_years.size()]

	draw_string(
		font,
		Vector2(stable_x, y + 20),
		stable_text,
		HORIZONTAL_ALIGNMENT_LEFT,
		summary_width,
		13,
		Color(0.92, 0.94, 0.98, 1.0)
	)


func _draw_cell(rect: Rect2, cell: Dictionary, is_hovered_cell: bool) -> void:
	if cell.is_empty():
		draw_rect(rect, EMPTY_CELL_COLOR)

		if is_hovered_cell:
			draw_rect(rect, Color(1, 1, 1, 0.18), false, 1.0)

		return

	var abs_r := float(cell.get("abs_r", 0.0))
	var passes := bool(cell.get("passes_fdr", false))

	var color := _value_to_color(abs_r, passes)

	draw_rect(rect, color)

	if not passes:
		draw_line(
			Vector2(rect.position.x, rect.position.y + rect.size.y),
			Vector2(rect.position.x + rect.size.x, rect.position.y),
			Color(0, 0, 0, 0.32),
			1.0
		)

	if is_hovered_cell:
		draw_rect(rect, Color(1, 1, 1, 0.45), false, 2.0)

	var text_color := Color(0.05, 0.07, 0.10, 0.95)

	if _value_to_intensity(abs_r) < 0.42:
		text_color = Color(0.92, 0.94, 0.98, 0.95)

	draw_string(
		font,
		Vector2(rect.position.x, rect.position.y + 16),
		"%.3f" % abs_r,
		HORIZONTAL_ALIGNMENT_CENTER,
		rect.size.x,
		12,
		text_color
	)


# ---------------------------------------------------------------------------
# Input
# ---------------------------------------------------------------------------

func _gui_input(event: InputEvent) -> void:
	if event is InputEventMouseMotion:
		_update_hover(event.position)

	elif event is InputEventMouseButton:
		if event.pressed and event.button_index == MOUSE_BUTTON_LEFT:
			_handle_click(event.position)


func _update_hover(pos: Vector2) -> void:
	var new_row := _row_index_at(pos)
	var new_year := _year_at(pos)

	if hovered_row_index != new_row or hovered_year != new_year:
		hovered_row_index = new_row
		hovered_year = new_year
		queue_redraw()


func _handle_click(pos: Vector2) -> void:
	var cell_info := _cell_info_at(pos)

	if not cell_info.is_empty():
		var row_index := int(cell_info.get("row_index", -1))
		var year := int(cell_info.get("year", -1))

		if row_index >= 0 and row_index < rows.size():
			var row := rows[row_index]
			var years_dict: Dictionary = row.get("years", {})
			var cell: Dictionary = {}

			if years_dict.has(year):
				cell = years_dict[year]
			elif years_dict.has(str(year)):
				cell = years_dict[str(year)]

			feature_year_selected.emit(row, year, cell)
			return

	var row_index := _row_index_at(pos)

	if row_index >= 0 and row_index < rows.size():
		feature_selected.emit(rows[row_index])


func _row_index_at(pos: Vector2) -> int:
	for i in row_hitboxes.size():
		if row_hitboxes[i].has_point(pos):
			return i

	return -1


func _year_at(pos: Vector2) -> int:
	var cell_info := _cell_info_at(pos)

	if cell_info.is_empty():
		return -1

	return int(cell_info.get("year", -1))


func _cell_info_at(pos: Vector2) -> Dictionary:
	for cell_info in cell_hitboxes:
		var rect: Rect2 = cell_info.get("rect", Rect2())

		if rect.has_point(pos):
			return cell_info

	return {}


# ---------------------------------------------------------------------------
# Parsing / pivoting
# ---------------------------------------------------------------------------

func _build_heatmap_rows() -> Array[Dictionary]:
	var by_feature := {}

	for year in available_years:
		var year_data := _get_year_data(year)
		var final_round := _get_final_round(year_data)

		if final_round.is_empty():
			continue

		var round_number := int(final_round.get("round", 0))
		var columns := _get_round_columns(final_round)

		for raw_col in columns:
			if typeof(raw_col) != TYPE_DICTIONARY:
				continue

			var col: Dictionary = raw_col
			var passes := bool(col.get("passes_fdr", false))

			if pass_only and not passes:
				continue

			var feature := str(col.get("feature", ""))

			if feature == "":
				continue

			var family := _get_column_family(col)

			if family_filter != "All" and family != family_filter:
				continue

			if not by_feature.has(feature):
				by_feature[feature] = _make_empty_feature_row(col)

			var feature_row: Dictionary = by_feature[feature]
			var years_dict: Dictionary = feature_row.get("years", {})

			years_dict[year] = {
				"abs_r": float(col.get("abs_r", 0.0)),
				"passes_fdr": passes,
				"round": round_number,
				"feature": feature,
				"label": str(col.get("label", feature)),
				"raw_label": str(col.get("raw_label", "")),
				"concept": str(col.get("concept", "")),
				"family": family,
			}

			feature_row["years"] = years_dict
			by_feature[feature] = feature_row

	var built_rows: Array[Dictionary] = []

	for feature in by_feature.keys():
		var row: Dictionary = by_feature[feature]
		_finalize_feature_row(row)
		built_rows.append(row)

	built_rows.sort_custom(_sort_heatmap_rows)

	if max_rows > 0 and built_rows.size() > max_rows:
		built_rows = built_rows.slice(0, max_rows)

	return built_rows


func _make_empty_feature_row(col: Dictionary) -> Dictionary:
	var feature := str(col.get("feature", ""))
	var family := _get_column_family(col)

	return {
		"feature": feature,
		"label": str(col.get("label", feature)),
		"raw_label": str(col.get("raw_label", "")),
		"concept": str(col.get("concept", "")),
		"family": family,
		"years": {},
		"mean_abs_r": 0.0,
		"min_abs_r": 0.0,
		"max_abs_r": 0.0,
		"stable_count": 0,
	}


func _finalize_feature_row(row: Dictionary) -> void:
	var years_dict: Dictionary = row.get("years", {})

	var sum_r := 0.0
	var count := 0
	var stable_count := 0

	var min_r := INF
	var max_r := -INF

	for year in available_years:
		var cell: Dictionary = {}

		if years_dict.has(year):
			cell = years_dict[year]
		elif years_dict.has(str(year)):
			cell = years_dict[str(year)]

		if cell.is_empty():
			continue

		var abs_r := float(cell.get("abs_r", 0.0))
		var passes := bool(cell.get("passes_fdr", false))

		sum_r += abs_r
		count += 1

		min_r = min(min_r, abs_r)
		max_r = max(max_r, abs_r)

		if passes:
			stable_count += 1

	if count > 0:
		row["mean_abs_r"] = sum_r / float(count)
		row["min_abs_r"] = min_r
		row["max_abs_r"] = max_r
	else:
		row["mean_abs_r"] = 0.0
		row["min_abs_r"] = 0.0
		row["max_abs_r"] = 0.0

	row["stable_count"] = stable_count
	row["observed_year_count"] = count


func _sort_heatmap_rows(a: Dictionary, b: Dictionary) -> bool:
	var stable_a := int(a.get("stable_count", 0))
	var stable_b := int(b.get("stable_count", 0))

	if stable_a != stable_b:
		return stable_a > stable_b

	var mean_a := float(a.get("mean_abs_r", 0.0))
	var mean_b := float(b.get("mean_abs_r", 0.0))

	if not is_equal_approx(mean_a, mean_b):
		return mean_a > mean_b

	return str(a.get("feature", "")) < str(b.get("feature", ""))


func _extract_years(source_report: Dictionary) -> Array[int]:
	var years: Array[int] = []

	for key in source_report.keys():
		var key_str := str(key)

		if key_str == "report":
			continue

		if key_str.is_valid_int():
			years.append(int(key_str))

	years.sort()

	return years


func _get_year_data(year: int) -> Dictionary:
	var string_key := str(year)

	if report.has(string_key) and typeof(report[string_key]) == TYPE_DICTIONARY:
		return report[string_key]

	if report.has(year) and typeof(report[year]) == TYPE_DICTIONARY:
		return report[year]

	return {}


func _get_final_round(year_data: Dictionary) -> Dictionary:
	var rounds: Array = year_data.get("rounds", [])

	if rounds.is_empty():
		return {}

	for i in range(rounds.size() - 1, -1, -1):
		if typeof(rounds[i]) == TYPE_DICTIONARY:
			return rounds[i]

	return {}


func _get_round_columns(round_data: Dictionary) -> Array:
	var cols: Array = round_data.get("all_columns", [])

	if cols.is_empty():
		cols = round_data.get("top_columns", [])

	return cols


func _get_column_family(col: Dictionary) -> String:
	if col.has("group"):
		return str(col["group"])

	if col.has("family"):
		return str(col["family"])

	var feature := str(col.get("feature", ""))

	return _infer_family_from_feature(feature)


func _infer_family_from_feature(feature: String) -> String:
	if feature.contains("_"):
		return feature.split("_")[0]

	if feature.length() >= 4 and feature.begins_with("DP"):
		return feature.substr(0, 4)

	return "OTHER"


# ---------------------------------------------------------------------------
# Scaling / color
# ---------------------------------------------------------------------------

func _value_to_color(value: float, passes_fdr: bool) -> Color:
	var intensity := _value_to_intensity(value)

	var low := Color(0.14, 0.16, 0.20, 1.0)
	var high := PASS_CELL_BASE

	if not passes_fdr:
		high = FAIL_CELL_BASE

	var color := low.lerp(high, intensity)

	if not passes_fdr:
		color.a = 0.48

	return color


func _value_to_intensity(value: float) -> float:
	match scale_mode:
		ScaleMode.LINEAR_ABSOLUTE:
			return clamp(value, 0.0, 1.0)

		ScaleMode.LINEAR_ZOOMED:
			var min_v := _linear_display_min()
			return clamp(inverse_lerp(min_v, 1.0, value), 0.0, 1.0)

		ScaleMode.LOG_DISTANCE_TO_ONE:
			return _log_distance_t(value)

	return clamp(value, 0.0, 1.0)


func _linear_display_min() -> float:
	var min_r := 1.0

	for row in rows:
		var years_dict: Dictionary = row.get("years", {})

		for year in available_years:
			var cell: Dictionary = {}

			if years_dict.has(year):
				cell = years_dict[year]
			elif years_dict.has(str(year)):
				cell = years_dict[str(year)]

			if cell.is_empty():
				continue

			min_r = min(min_r, float(cell.get("abs_r", 0.0)))

	return max(0.0, min_r - zoom_padding_below_min)


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
	var min_r := 1.0

	for row in rows:
		var years_dict: Dictionary = row.get("years", {})

		for year in available_years:
			var cell: Dictionary = {}

			if years_dict.has(year):
				cell = years_dict[year]
			elif years_dict.has(str(year)):
				cell = years_dict[str(year)]

			if cell.is_empty():
				continue

			min_r = min(min_r, float(cell.get("abs_r", 0.0)))

	return clamp(min_r, 0.0, 1.0 - log_epsilon)


func _log_scale_max_source() -> float:
	var max_r := 0.0

	for row in rows:
		var years_dict: Dictionary = row.get("years", {})

		for year in available_years:
			var cell: Dictionary = {}

			if years_dict.has(year):
				cell = years_dict[year]
			elif years_dict.has(str(year)):
				cell = years_dict[str(year)]

			if cell.is_empty():
				continue

			max_r = max(max_r, float(cell.get("abs_r", 0.0)))

	return clamp(max_r, 0.0, 1.0 - log_epsilon)


func _log_distance_score(value: float) -> float:
	var safe_value :float= clamp(value, 0.0, 1.0 - log_epsilon)
	var distance_from_one :float= max(1.0 - safe_value, log_epsilon)

	return -log(distance_from_one) / log(10.0)


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------

func _update_minimum_size() -> void:
	var wanted_w := (
		left_padding
		+ label_width
		+ available_years.size() * cell_width
		+ summary_width
		+ summary_width
		+ right_padding
	)

	var wanted_h :float= (
		top_padding
		+ header_height
		+ row_height
		+ max(rows.size(), 1) * row_height
		+ bottom_padding
	)

	custom_minimum_size = Vector2(
		max(minimum_chart_width, wanted_w),
		max(minimum_empty_height, wanted_h)
	)


func _cells_start_x() -> float:
	return left_padding + label_width


func _mean_start_x() -> float:
	return _cells_start_x() + available_years.size() * cell_width + 12.0


func _stable_start_x() -> float:
	return _mean_start_x() + summary_width


func _family_color(family: String) -> Color:
	return FAMILY_COLORS.get(family, FAMILY_COLORS["OTHER"])


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
