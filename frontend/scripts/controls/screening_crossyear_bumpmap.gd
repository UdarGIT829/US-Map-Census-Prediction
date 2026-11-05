extends Control
class_name ScreeningCrossyearBumpmap

signal feature_selected(row: Dictionary)
signal feature_year_selected(row: Dictionary, year: int, point: Dictionary)

enum FeatureSelectionMode {
	TOP_ANY_YEAR,
	STABLE_ONLY,
	BIGGEST_MOVERS,
}

@export var top_n: int = 25
@export var max_visible_rank: int = 25

@export var selection_mode: FeatureSelectionMode = FeatureSelectionMode.TOP_ANY_YEAR

@export var pass_only: bool = true
@export var family_filter: String = "All"

@export var show_points: bool = true
@export var show_rank_grid: bool = true
@export var show_rank_axis_labels: bool = true

## Kept available for debugging / alternate view, but off by default.
@export var show_endpoint_labels: bool = false
@export var show_hover_details_bar: bool = false

@export var left_label_width: float = 300.0
@export var right_label_width: float = 300.0
@export var axis_label_font_size: int = 11

@export var top_padding: float = 56.0
@export var bottom_padding: float = 36.0
@export var left_padding: float = 20.0
@export var right_padding: float = 20.0

## Must fit inside the CrossYearResults tab (~1060 x 456) or the chart spills
## past the panel; the node is anchored full-rect and grows from here.
@export var minimum_chart_width: float = 1000.0
@export var minimum_chart_height: float = 420.0

@export var line_width: float = 2.0
@export var hovered_line_width: float = 4.0
@export var point_radius: float = 4.0
@export var hovered_point_radius: float = 6.0

@export var hover_distance: float = 9.0

var report: Dictionary = {}

var available_years: Array[int] = []
var rows: Array[Dictionary] = []

var hovered_feature: String = ""
var hovered_year: int = -1

var point_hitboxes: Array[Dictionary] = []
var line_hitboxes: Array[Dictionary] = []

var font: Font

const FAMILY_COLORS := {
	"DP02": Color("#6EA8FE"),
	"DP03": Color("#75D69C"),
	"DP04": Color("#F2C66D"),
	"DP05": Color("#D38CFF"),
	"OTHER": Color("#AAB2C0"),
}


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

func _ready() -> void:
	font = get_theme_default_font()

	mouse_filter = Control.MOUSE_FILTER_STOP
	size_flags_horizontal = Control.SIZE_EXPAND_FILL
	size_flags_vertical = Control.SIZE_EXPAND_FILL

	custom_minimum_size = Vector2(minimum_chart_width, minimum_chart_height)

	## Non-empty tooltip_text enables _get_tooltip(); the real text is per-feature.
	tooltip_text = " "


func _make_custom_tooltip(for_text: String) -> Object:
	return ChartTooltip.make(for_text)


## Full, un-elided label for the hovered line or point.
func _get_tooltip(at_position: Vector2) -> String:
	var hit := _point_hit_at(at_position)

	if hit.is_empty():
		hit = _line_hit_at(at_position)

	if hit.is_empty():
		return ""

	var row := _row_by_feature(str(hit.get("feature", "")))

	if row.is_empty():
		return ""

	var feature := str(row.get("feature", ""))
	var text := "%s\n%s  ·  mean rank %.1f  ·  best #%d, worst #%d" % [
		str(row.get("label", feature)),
		feature,
		float(row.get("mean_rank", 0.0)),
		int(row.get("best_rank", 0)),
		int(row.get("worst_rank", 0)),
	]

	var year := int(hit.get("year", -1))

	if year > 0:
		var point := _get_point_for_year(row.get("points", {}), year)
		if not point.is_empty():
			text += "\n%d: rank #%d  ·  |r| = %.4f" % [year, int(point.get("rank", 0)), float(point.get("abs_r", 0.0))]

	return text


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

func set_report(new_report: Dictionary) -> void:
	report = new_report.duplicate(true)

	available_years = _extract_years(report)
	rows = _build_bump_rows()

	_update_minimum_size()
	queue_redraw()


func clear_report() -> void:
	report.clear()
	available_years.clear()
	rows.clear()

	hovered_feature = ""
	hovered_year = -1

	point_hitboxes.clear()
	line_hitboxes.clear()

	custom_minimum_size = Vector2(minimum_chart_width, minimum_chart_height)
	queue_redraw()


func set_top_n(new_top_n: int) -> void:
	top_n = max(1, new_top_n)
	max_visible_rank = max(max_visible_rank, top_n)

	rows = _build_bump_rows()

	_update_minimum_size()
	queue_redraw()


func set_family_filter(new_family_filter: String) -> void:
	family_filter = new_family_filter

	rows = _build_bump_rows()

	_update_minimum_size()
	queue_redraw()


func set_pass_only(new_pass_only: bool) -> void:
	pass_only = new_pass_only

	rows = _build_bump_rows()

	_update_minimum_size()
	queue_redraw()


func set_selection_mode(new_selection_mode: FeatureSelectionMode) -> void:
	selection_mode = new_selection_mode

	rows = _build_bump_rows()

	_update_minimum_size()
	queue_redraw()


func set_max_visible_rank(new_max_visible_rank: int) -> void:
	max_visible_rank = max(1, new_max_visible_rank)

	rows = _build_bump_rows()

	_update_minimum_size()
	queue_redraw()


# ---------------------------------------------------------------------------
# Drawing
# ---------------------------------------------------------------------------

func _draw() -> void:
	if rows.is_empty() or available_years.is_empty():
		_draw_empty()
		return

	point_hitboxes.clear()
	line_hitboxes.clear()

	var plot_rect := _plot_rect()

	_draw_title()
	_draw_axes(plot_rect)

	_draw_lines(plot_rect)
	_draw_points(plot_rect)

	if show_rank_axis_labels:
		_draw_rank_axis_labels(plot_rect)

	if show_endpoint_labels:
		_draw_endpoint_labels(plot_rect)

	## Hover details now live in the tooltip (_get_tooltip); the bottom bar
	## duplicated it with a truncated label.
	if show_hover_details_bar:
		_draw_hover_details(plot_rect)


func _draw_empty() -> void:
	draw_string(
		font,
		Vector2(16, 32),
		"No cross-year bumpmap rows to display.",
		HORIZONTAL_ALIGNMENT_LEFT,
		-1,
		14,
		Color(0.85, 0.87, 0.92, 1.0)
	)


func _draw_title() -> void:
	draw_string(
		font,
		Vector2(left_padding, 24),
		"Final-round cross-year rank bumpmap",
		HORIZONTAL_ALIGNMENT_LEFT,
		500,
		16,
		Color(0.94, 0.95, 0.98, 1.0)
	)

	var subtitle := "Ranks are assigned within each year by abs_r descending. Missing years break the line."

	#draw_string(
		#font,
		#Vector2(left_padding, 43),
		#subtitle,
		#HORIZONTAL_ALIGNMENT_LEFT,
		#760,
		#12,
		#Color(0.72, 0.76, 0.84, 1.0)
	#)


func _draw_axes(plot_rect: Rect2) -> void:
	if show_rank_grid:
		_draw_rank_grid(plot_rect)

	for year_index in available_years.size():
		var year := available_years[year_index]
		var x := _year_index_to_x(year_index, plot_rect)

		draw_line(
			Vector2(x, plot_rect.position.y),
			Vector2(x, plot_rect.position.y + plot_rect.size.y),
			Color(1, 1, 1, 0.14),
			1.0
		)

		draw_string(
			font,
			Vector2(x - 42, plot_rect.position.y - 12),
			str(year),
			HORIZONTAL_ALIGNMENT_CENTER,
			84,
			13,
			Color(0.82, 0.86, 0.94, 1.0)
		)

	draw_line(
		plot_rect.position,
		Vector2(plot_rect.position.x, plot_rect.position.y + plot_rect.size.y),
		Color(1, 1, 1, 0.22),
		1.0
	)

	draw_line(
		Vector2(plot_rect.position.x, plot_rect.position.y),
		Vector2(plot_rect.position.x + plot_rect.size.x, plot_rect.position.y),
		Color(1, 1, 1, 0.22),
		1.0
	)


func _draw_rank_grid(plot_rect: Rect2) -> void:
	var grid_ranks := _rank_grid_values()

	for rank in grid_ranks:
		var y := _rank_to_y(rank, plot_rect)

		draw_line(
			Vector2(plot_rect.position.x, y),
			Vector2(plot_rect.position.x + plot_rect.size.x, y),
			Color(1, 1, 1, 0.08),
			1.0
		)

		draw_string(
			font,
			Vector2(plot_rect.position.x - 42, y + 4),
			"#%d" % rank,
			HORIZONTAL_ALIGNMENT_RIGHT,
			34,
			11,
			Color(0.70, 0.74, 0.82, 1.0)
		)


func _draw_lines(plot_rect: Rect2) -> void:
	var normal_rows: Array[Dictionary] = []
	var hovered_row: Dictionary = {}

	for row in rows:
		if str(row.get("feature", "")) == hovered_feature:
			hovered_row = row
		else:
			normal_rows.append(row)

	for row in normal_rows:
		_draw_feature_line(row, plot_rect, false)

	if not hovered_row.is_empty():
		_draw_feature_line(hovered_row, plot_rect, true)


func _draw_feature_line(row: Dictionary, plot_rect: Rect2, is_hovered: bool) -> void:
	var family := str(row.get("family", "OTHER"))
	var color := _family_color(family)

	var alpha := 0.48
	var width := line_width

	if hovered_feature != "":
		alpha = 0.12

	if is_hovered:
		alpha = 1.0
		width = hovered_line_width

	color.a = alpha

	var points_by_year: Dictionary = row.get("points", {})

	var previous_has_point := false
	var previous_pos := Vector2.ZERO
	var previous_year := -1

	for year_index in available_years.size():
		var year := available_years[year_index]
		var point := _get_point_for_year(points_by_year, year)

		if point.is_empty():
			previous_has_point = false
			previous_year = -1
			continue

		var rank := int(point.get("rank", 999999))

		if rank > max_visible_rank:
			previous_has_point = false
			previous_year = -1
			continue

		var pos := Vector2(
			_year_index_to_x(year_index, plot_rect),
			_rank_to_y(rank, plot_rect)
		)

		if previous_has_point:
			draw_line(previous_pos, pos, color, width, true)

			line_hitboxes.append({
				"feature": str(row.get("feature", "")),
				"year_a": previous_year,
				"year_b": year,
				"a": previous_pos,
				"b": pos,
			})

		previous_pos = pos
		previous_has_point = true
		previous_year = year


func _draw_points(plot_rect: Rect2) -> void:
	if not show_points:
		return

	var normal_rows: Array[Dictionary] = []
	var hovered_row: Dictionary = {}

	for row in rows:
		if str(row.get("feature", "")) == hovered_feature:
			hovered_row = row
		else:
			normal_rows.append(row)

	for row in normal_rows:
		_draw_feature_points(row, plot_rect, false)

	if not hovered_row.is_empty():
		_draw_feature_points(hovered_row, plot_rect, true)


func _draw_feature_points(row: Dictionary, plot_rect: Rect2, is_hovered: bool) -> void:
	var feature := str(row.get("feature", ""))
	var family := str(row.get("family", "OTHER"))
	var color := _family_color(family)

	var alpha := 0.64
	var radius := point_radius

	if hovered_feature != "":
		alpha = 0.18

	if is_hovered:
		alpha = 1.0
		radius = hovered_point_radius

	color.a = alpha

	var points_by_year: Dictionary = row.get("points", {})

	for year_index in available_years.size():
		var year := available_years[year_index]
		var point := _get_point_for_year(points_by_year, year)

		if point.is_empty():
			continue

		var rank := int(point.get("rank", 999999))

		if rank > max_visible_rank:
			continue

		var pos := Vector2(
			_year_index_to_x(year_index, plot_rect),
			_rank_to_y(rank, plot_rect)
		)

		draw_circle(pos, radius, color)

		if is_hovered and year == hovered_year:
			draw_circle(pos, radius + 3.0, Color(1, 1, 1, 0.36))

		point_hitboxes.append({
			"feature": feature,
			"year": year,
			"position": pos,
			"radius": radius + hover_distance,
		})


# ---------------------------------------------------------------------------
# Bumpmap rank-axis labels
# ---------------------------------------------------------------------------

func _draw_rank_axis_labels(plot_rect: Rect2) -> void:
	if available_years.is_empty():
		return

	var first_year := available_years[0]
	var last_year := available_years[available_years.size() - 1]

	_draw_year_rank_axis_labels(first_year, true, plot_rect)

	if last_year != first_year:
		_draw_year_rank_axis_labels(last_year, false, plot_rect)


func _draw_year_rank_axis_labels(year: int, is_left_axis: bool, plot_rect: Rect2) -> void:
	var rank_to_row := _rank_to_row_for_year(year)

	for rank in range(1, max_visible_rank + 1):
		if not rank_to_row.has(rank):
			continue

		var row: Dictionary = rank_to_row[rank]

		var feature := str(row.get("feature", ""))
		var label := str(row.get("label", feature))
		var family := str(row.get("family", "OTHER"))

		var y := _rank_to_y(rank, plot_rect)
		var color := _family_color(family)

		if hovered_feature != "":
			if hovered_feature == feature:
				color.a = 1.0
			else:
				color.a = 0.22
		else:
			color.a = 0.86

		## Family is already encoded by color, so spend the width on the label.
		var text := _fit(
			"#%d %s" % [rank, label],
			(left_label_width if is_left_axis else right_label_width) - 12,
			axis_label_font_size
		)

		if is_left_axis:
			draw_string(
				font,
				Vector2(left_padding, y + 4),
				text,
				HORIZONTAL_ALIGNMENT_LEFT,
				left_label_width - 12,
				axis_label_font_size,
				color
			)
		else:
			draw_string(
				font,
				Vector2(plot_rect.position.x + plot_rect.size.x + 12, y + 4),
				text,
				HORIZONTAL_ALIGNMENT_LEFT,
				right_label_width - 12,
				axis_label_font_size,
				color
			)


func _rank_to_row_for_year(year: int) -> Dictionary:
	var rank_to_row := {}

	for row in rows:
		var points_by_year: Dictionary = row.get("points", {})
		var point := _get_point_for_year(points_by_year, year)

		if point.is_empty():
			continue

		var rank := int(point.get("rank", 999999))

		if rank < 1 or rank > max_visible_rank:
			continue

		if not rank_to_row.has(rank):
			rank_to_row[rank] = row
			continue

		var existing_row: Dictionary = rank_to_row[rank]
		var existing_points: Dictionary = existing_row.get("points", {})
		var existing_point := _get_point_for_year(existing_points, year)

		var existing_abs_r := float(existing_point.get("abs_r", 0.0))
		var new_abs_r := float(point.get("abs_r", 0.0))

		if new_abs_r > existing_abs_r:
			rank_to_row[rank] = row

	return rank_to_row


# ---------------------------------------------------------------------------
# Optional endpoint labels
# ---------------------------------------------------------------------------

func _draw_endpoint_labels(plot_rect: Rect2) -> void:
	if not show_endpoint_labels:
		return

	for row in rows:
		var feature := str(row.get("feature", ""))

		if hovered_feature != "" and feature != hovered_feature:
			continue

		var points_by_year: Dictionary = row.get("points", {})
		var label := str(row.get("label", feature))
		var family := str(row.get("family", "OTHER"))
		var color := _family_color(family)

		if hovered_feature == feature:
			color.a = 1.0
		else:
			color.a = 0.82

		var first_info := _first_visible_point(points_by_year, plot_rect)
		var last_info := _last_visible_point(points_by_year, plot_rect)

		if not first_info.is_empty():
			var first_pos: Vector2 = first_info.get("position", Vector2.ZERO)
			var first_rank := int(first_info.get("rank", 0))

			draw_string(
				font,
				Vector2(left_padding, first_pos.y + 4),
				"#%d %s" % [first_rank, _shorten("[%s] %s" % [family, label], 35)],
				HORIZONTAL_ALIGNMENT_LEFT,
				left_label_width - 12,
				11,
				color
			)

		if not last_info.is_empty():
			var last_pos: Vector2 = last_info.get("position", Vector2.ZERO)
			var last_rank := int(last_info.get("rank", 0))
			var mean_rank := float(row.get("mean_rank", 0.0))

			draw_string(
				font,
				Vector2(plot_rect.position.x + plot_rect.size.x + 12, last_pos.y + 4),
				"#%d  μ %.1f" % [last_rank, mean_rank],
				HORIZONTAL_ALIGNMENT_LEFT,
				right_label_width - 12,
				11,
				color
			)


func _draw_hover_details(plot_rect: Rect2) -> void:
	if hovered_feature == "":
		return

	var row := _row_by_feature(hovered_feature)

	if row.is_empty():
		return

	var label := str(row.get("label", hovered_feature))
	var family := str(row.get("family", "OTHER"))
	var mean_rank := float(row.get("mean_rank", 0.0))
	var rank_delta := int(row.get("rank_delta", 0))
	var stable_count := int(row.get("stable_count", 0))

	var text := "%s | [%s] mean rank %.2f | Δ rank %d | stable %d/%d" % [
		_shorten(label, 54),
		family,
		mean_rank,
		rank_delta,
		stable_count,
		available_years.size(),
	]

	draw_rect(
		Rect2(plot_rect.position.x, size.y - 30, min(plot_rect.size.x, 720.0), 24),
		Color(0, 0, 0, 0.32)
	)

	draw_string(
		font,
		Vector2(plot_rect.position.x + 8, size.y - 13),
		text,
		HORIZONTAL_ALIGNMENT_LEFT,
		700,
		12,
		Color(0.94, 0.96, 1.0, 1.0)
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
	var hit_point := _point_hit_at(pos)

	if not hit_point.is_empty():
		var new_feature := str(hit_point.get("feature", ""))
		var new_year := int(hit_point.get("year", -1))

		if hovered_feature != new_feature or hovered_year != new_year:
			hovered_feature = new_feature
			hovered_year = new_year
			queue_redraw()

		return

	var hit_line := _line_hit_at(pos)

	if not hit_line.is_empty():
		var new_feature := str(hit_line.get("feature", ""))

		if hovered_feature != new_feature or hovered_year != -1:
			hovered_feature = new_feature
			hovered_year = -1
			queue_redraw()

		return

	if hovered_feature != "" or hovered_year != -1:
		hovered_feature = ""
		hovered_year = -1
		queue_redraw()


func _handle_click(pos: Vector2) -> void:
	var hit_point := _point_hit_at(pos)

	if not hit_point.is_empty():
		var feature := str(hit_point.get("feature", ""))
		var year := int(hit_point.get("year", -1))
		var row := _row_by_feature(feature)

		if not row.is_empty():
			var points_by_year: Dictionary = row.get("points", {})
			var point := _get_point_for_year(points_by_year, year)
			feature_year_selected.emit(row, year, point)
			return

	var hit_line := _line_hit_at(pos)

	if not hit_line.is_empty():
		var feature := str(hit_line.get("feature", ""))
		var row := _row_by_feature(feature)

		if not row.is_empty():
			feature_selected.emit(row)
			return


func _point_hit_at(pos: Vector2) -> Dictionary:
	for hit in point_hitboxes:
		var center: Vector2 = hit.get("position", Vector2.ZERO)
		var radius := float(hit.get("radius", hover_distance))

		if center.distance_to(pos) <= radius:
			return hit

	return {}


func _line_hit_at(pos: Vector2) -> Dictionary:
	var closest_hit := {}
	var closest_dist := INF

	for hit in line_hitboxes:
		var a: Vector2 = hit.get("a", Vector2.ZERO)
		var b: Vector2 = hit.get("b", Vector2.ZERO)

		var dist := _distance_to_segment(pos, a, b)

		if dist <= hover_distance and dist < closest_dist:
			closest_dist = dist
			closest_hit = hit

	return closest_hit


# ---------------------------------------------------------------------------
# Parsing / ranking / pivoting
# ---------------------------------------------------------------------------

func _build_bump_rows() -> Array[Dictionary]:
	var ranked_by_year := _rank_final_round_columns_by_year()
	var selected_features := _select_features(ranked_by_year)

	var by_feature := {}

	for year in available_years:
		var ranked_columns: Array = ranked_by_year.get(year, [])

		for ranked_col in ranked_columns:
			if typeof(ranked_col) != TYPE_DICTIONARY:
				continue

			var feature := str(ranked_col.get("feature", ""))

			if feature == "":
				continue

			if not selected_features.has(feature):
				continue

			if not by_feature.has(feature):
				by_feature[feature] = _make_empty_bump_row(ranked_col)

			var row: Dictionary = by_feature[feature]
			var points: Dictionary = row.get("points", {})

			points[year] = {
				"year": year,
				"rank": int(ranked_col.get("rank", 999999)),
				"abs_r": float(ranked_col.get("abs_r", 0.0)),
				"passes_fdr": bool(ranked_col.get("passes_fdr", false)),
				"round": int(ranked_col.get("round", 0)),
				"feature": feature,
				"label": str(ranked_col.get("label", feature)),
				"raw_label": str(ranked_col.get("raw_label", "")),
				"concept": str(ranked_col.get("concept", "")),
				"family": str(ranked_col.get("family", "OTHER")),
			}

			row["points"] = points
			by_feature[feature] = row

	var built_rows: Array[Dictionary] = []

	for feature in by_feature.keys():
		var row: Dictionary = by_feature[feature]
		_finalize_bump_row(row)
		built_rows.append(row)

	built_rows.sort_custom(_sort_bump_rows)

	return built_rows


func _rank_final_round_columns_by_year() -> Dictionary:
	var ranked_by_year := {}

	for year in available_years:
		var year_data := _get_year_data(year)
		var final_round := _get_final_round(year_data)

		if final_round.is_empty():
			ranked_by_year[year] = []
			continue

		var round_number := int(final_round.get("round", 0))
		var columns := _get_round_columns(final_round)

		var clean_columns: Array[Dictionary] = []

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

			var clean_col := {
				"feature": feature,
				"label": str(col.get("label", feature)),
				"raw_label": str(col.get("raw_label", "")),
				"concept": str(col.get("concept", "")),
				"family": family,
				"abs_r": float(col.get("abs_r", 0.0)),
				"passes_fdr": passes,
				"round": round_number,
			}

			clean_columns.append(clean_col)

		clean_columns.sort_custom(
			func(a: Dictionary, b: Dictionary) -> bool:
				var ar := float(a.get("abs_r", 0.0))
				var br := float(b.get("abs_r", 0.0))

				if not is_equal_approx(ar, br):
					return ar > br

				return str(a.get("feature", "")) < str(b.get("feature", ""))
		)

		for i in clean_columns.size():
			clean_columns[i]["rank"] = i + 1

		ranked_by_year[year] = clean_columns

	return ranked_by_year


func _select_features(ranked_by_year: Dictionary) -> Dictionary:
	match selection_mode:
		FeatureSelectionMode.TOP_ANY_YEAR:
			return _select_top_any_year(ranked_by_year)

		FeatureSelectionMode.STABLE_ONLY:
			return _select_stable_only(ranked_by_year)

		FeatureSelectionMode.BIGGEST_MOVERS:
			return _select_biggest_movers(ranked_by_year)

	return _select_top_any_year(ranked_by_year)


func _select_top_any_year(ranked_by_year: Dictionary) -> Dictionary:
	var selected := {}

	for year in available_years:
		var ranked_columns: Array = ranked_by_year.get(year, [])
		var limit: int = min(top_n, ranked_columns.size())

		for i in limit:
			var col: Dictionary = ranked_columns[i]
			var feature := str(col.get("feature", ""))

			if feature != "":
				selected[feature] = true

	return selected


func _select_stable_only(ranked_by_year: Dictionary) -> Dictionary:
	var counts := {}

	for year in available_years:
		var ranked_columns: Array = ranked_by_year.get(year, [])
		var limit: int = min(top_n, ranked_columns.size())

		for i in limit:
			var col: Dictionary = ranked_columns[i]
			var feature := str(col.get("feature", ""))

			if feature == "":
				continue

			counts[feature] = int(counts.get(feature, 0)) + 1

	var selected := {}

	for feature in counts.keys():
		if int(counts[feature]) >= available_years.size():
			selected[feature] = true

	return selected


func _select_biggest_movers(ranked_by_year: Dictionary) -> Dictionary:
	var rank_ranges := {}

	for year in available_years:
		var ranked_columns: Array = ranked_by_year.get(year, [])
		var limit: int = min(max_visible_rank, ranked_columns.size())

		for i in limit:
			var col: Dictionary = ranked_columns[i]
			var feature := str(col.get("feature", ""))

			if feature == "":
				continue

			var rank := int(col.get("rank", 999999))

			if not rank_ranges.has(feature):
				rank_ranges[feature] = {
					"min_rank": rank,
					"max_rank": rank,
					"count": 0,
				}

			var info: Dictionary = rank_ranges[feature]
			info["min_rank"] = min(int(info.get("min_rank", rank)), rank)
			info["max_rank"] = max(int(info.get("max_rank", rank)), rank)
			info["count"] = int(info.get("count", 0)) + 1
			rank_ranges[feature] = info

	var feature_rows: Array[Dictionary] = []

	for feature in rank_ranges.keys():
		var info: Dictionary = rank_ranges[feature]
		var count := int(info.get("count", 0))

		if count < 2:
			continue

		var delta := int(info.get("max_rank", 0)) - int(info.get("min_rank", 0))

		feature_rows.append({
			"feature": str(feature),
			"delta": delta,
			"count": count,
		})

	feature_rows.sort_custom(
		func(a: Dictionary, b: Dictionary) -> bool:
			var da := int(a.get("delta", 0))
			var db := int(b.get("delta", 0))

			if da != db:
				return da > db

			return str(a.get("feature", "")) < str(b.get("feature", ""))
	)

	var selected := {}
	var limit: int = min(top_n, feature_rows.size())

	for i in limit:
		selected[str(feature_rows[i].get("feature", ""))] = true

	return selected


func _make_empty_bump_row(col: Dictionary) -> Dictionary:
	var feature := str(col.get("feature", ""))
	var family := str(col.get("family", _infer_family_from_feature(feature)))

	return {
		"feature": feature,
		"label": str(col.get("label", feature)),
		"raw_label": str(col.get("raw_label", "")),
		"concept": str(col.get("concept", "")),
		"family": family,
		"points": {},
		"mean_rank": 0.0,
		"best_rank": 0,
		"worst_rank": 0,
		"rank_delta": 0,
		"mean_abs_r": 0.0,
		"stable_count": 0,
		"observed_year_count": 0,
	}


func _finalize_bump_row(row: Dictionary) -> void:
	var points: Dictionary = row.get("points", {})

	var rank_sum := 0.0
	var abs_r_sum := 0.0
	var count := 0

	var best_rank := 999999
	var worst_rank := -1
	var stable_count := 0

	for year in available_years:
		var point := _get_point_for_year(points, year)

		if point.is_empty():
			continue

		var rank := int(point.get("rank", 999999))
		var abs_r := float(point.get("abs_r", 0.0))
		var passes := bool(point.get("passes_fdr", false))

		rank_sum += rank
		abs_r_sum += abs_r
		count += 1

		best_rank = min(best_rank, rank)
		worst_rank = max(worst_rank, rank)

		if passes:
			stable_count += 1

	if count > 0:
		row["mean_rank"] = rank_sum / float(count)
		row["mean_abs_r"] = abs_r_sum / float(count)
		row["best_rank"] = best_rank
		row["worst_rank"] = worst_rank
		row["rank_delta"] = worst_rank - best_rank
	else:
		row["mean_rank"] = 0.0
		row["mean_abs_r"] = 0.0
		row["best_rank"] = 0
		row["worst_rank"] = 0
		row["rank_delta"] = 0

	row["stable_count"] = stable_count
	row["observed_year_count"] = count


func _sort_bump_rows(a: Dictionary, b: Dictionary) -> bool:
	match selection_mode:
		FeatureSelectionMode.BIGGEST_MOVERS:
			var delta_a := int(a.get("rank_delta", 0))
			var delta_b := int(b.get("rank_delta", 0))

			if delta_a != delta_b:
				return delta_a > delta_b

		_:
			var mean_a := float(a.get("mean_rank", 999999.0))
			var mean_b := float(b.get("mean_rank", 999999.0))

			if not is_equal_approx(mean_a, mean_b):
				return mean_a < mean_b

	var stable_a := int(a.get("stable_count", 0))
	var stable_b := int(b.get("stable_count", 0))

	if stable_a != stable_b:
		return stable_a > stable_b

	return str(a.get("feature", "")) < str(b.get("feature", ""))


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

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
# Geometry helpers
# ---------------------------------------------------------------------------

func _plot_rect() -> Rect2:
	var x := left_padding + left_label_width
	var y := top_padding
	var w := size.x - x - right_label_width - right_padding
	var h := size.y - top_padding - bottom_padding

	w = max(120.0, w)
	h = max(120.0, h)

	return Rect2(x, y, w, h)


func _year_index_to_x(year_index: int, plot_rect: Rect2) -> float:
	if available_years.size() <= 1:
		return plot_rect.position.x + plot_rect.size.x * 0.5

	var t := float(year_index) / float(available_years.size() - 1)

	return plot_rect.position.x + t * plot_rect.size.x


func _rank_to_y(rank: int, plot_rect: Rect2) -> float:
	var clamped_rank: int = clampi(rank, 1, max_visible_rank)
	var t := 0.0

	if max_visible_rank <= 1:
		t = 0.0
	else:
		t = inverse_lerp(1.0, float(max_visible_rank), float(clamped_rank))

	return plot_rect.position.y + t * plot_rect.size.y


func _distance_to_segment(p: Vector2, a: Vector2, b: Vector2) -> float:
	var ab := b - a
	var ab_len_sq := ab.length_squared()

	if ab_len_sq <= 0.000001:
		return p.distance_to(a)

	var t :float= clamp((p - a).dot(ab) / ab_len_sq, 0.0, 1.0)
	var closest :Vector2= a + ab * t

	return p.distance_to(closest)


func _rank_grid_values() -> Array[int]:
	var values: Array[int] = []

	if max_visible_rank <= 10:
		for i in range(1, max_visible_rank + 1):
			values.append(i)
		return values

	values.append(1)

	var step := 5

	if max_visible_rank > 50:
		step = 10

	var r := step

	while r < max_visible_rank:
		values.append(r)
		r += step

	if not values.has(max_visible_rank):
		values.append(max_visible_rank)

	return values


func _first_visible_point(points_by_year: Dictionary, plot_rect: Rect2) -> Dictionary:
	for year_index in available_years.size():
		var year := available_years[year_index]
		var point := _get_point_for_year(points_by_year, year)

		if point.is_empty():
			continue

		var rank := int(point.get("rank", 999999))

		if rank > max_visible_rank:
			continue

		return {
			"year": year,
			"rank": rank,
			"position": Vector2(
				_year_index_to_x(year_index, plot_rect),
				_rank_to_y(rank, plot_rect)
			),
		}

	return {}


func _last_visible_point(points_by_year: Dictionary, plot_rect: Rect2) -> Dictionary:
	for i in range(available_years.size() - 1, -1, -1):
		var year := available_years[i]
		var point := _get_point_for_year(points_by_year, year)

		if point.is_empty():
			continue

		var rank := int(point.get("rank", 999999))

		if rank > max_visible_rank:
			continue

		return {
			"year": year,
			"rank": rank,
			"position": Vector2(
				_year_index_to_x(i, plot_rect),
				_rank_to_y(rank, plot_rect)
			),
		}

	return {}


func _get_point_for_year(points_by_year: Dictionary, year: int) -> Dictionary:
	if points_by_year.has(year):
		return points_by_year[year]

	if points_by_year.has(str(year)):
		return points_by_year[str(year)]

	return {}


func _row_by_feature(feature: String) -> Dictionary:
	for row in rows:
		if str(row.get("feature", "")) == feature:
			return row

	return {}


func _update_minimum_size() -> void:
	custom_minimum_size = Vector2(
		minimum_chart_width,
		minimum_chart_height
	)


# ---------------------------------------------------------------------------
# Visual helpers
# ---------------------------------------------------------------------------

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
