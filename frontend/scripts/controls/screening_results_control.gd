extends Control
class_name ScreeningResultsControl

## Owns the full screening report JSON and coordinates result visualization tabs.
##
## Expected scene:
##
## ScreeningResultsControl
## └── TabContainer
##     └── SingleYearResults
##         └── VBoxContainer
##             ├── HeaderRow
##             │   ├── YearDropdown        OptionButton
##             │   ├── FamilyDropdown      OptionButton
##             │   ├── TopNSpinBox         SpinBox
##             │   └── PassOnlyCheckBox    CheckBox
##             ├── HSeparator
##             └── ScrollContainer
##                 └── ScreeningSingleBarChart



@onready var tab_container: TabContainer = $TabContainer


@export var year_dropdown: OptionButton

@export var family_dropdown: OptionButton

@export var top_n_spinbox: SpinBox

@export var pass_only_checkbox: CheckBox

@export var bar_chart: Control
@export var multi_year_heatmap: Control
@export var crossyear_bumpmap: Control

var report: Dictionary = {}

var available_years: Array[int] = []
var selected_year: int = -1
var selected_family: String = "All"

## For now, show the first screening round.
## Later this can become a RoundDropdown value.
var default_round_index: int = 0


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

func _ready() -> void:
	_connect_ui()

	top_n_spinbox.min_value = 1
	top_n_spinbox.max_value = 200
	top_n_spinbox.step = 1
	top_n_spinbox.value = 25

	pass_only_checkbox.button_pressed = true

	_populate_empty_state()


# ---------------------------------------------------------------------------
# Signal binding
# ---------------------------------------------------------------------------

func _on_results_received(payload) -> void:
	match typeof(payload):
		TYPE_DICTIONARY:
			set_report(payload)

		TYPE_STRING:
			load_report_from_json_text(payload)

		_:
			push_error(
				"ScreeningResultsControl: unsupported results_received payload type: %s"
				% typeof(payload)
			)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

func load_report_from_json_text(json_text: String) -> void:
	var parsed = JSON.parse_string(json_text)

	if typeof(parsed) != TYPE_DICTIONARY:
		push_error("ScreeningResultsControl: JSON text did not parse into a Dictionary.")
		return

	set_report(parsed)


func set_report(new_report: Dictionary) -> void:
	report = new_report

	available_years = _extract_years(report)

	if available_years.is_empty():
		push_warning("ScreeningResultsControl: report contains no year blocks.")
		_populate_empty_state()
		_refresh_multi_year_heatmap()
		
		return

	selected_year = available_years[0]

	_populate_year_dropdown()
	_populate_family_dropdown()
	_refresh_single_year_chart()
	_refresh_multi_year_heatmap()
	_refresh_crossyear_bumpmap()


func clear_report() -> void:
	report.clear()
	available_years.clear()

	selected_year = -1
	selected_family = "All"

	_populate_empty_state()
	_refresh_multi_year_heatmap()
	_refresh_crossyear_bumpmap()

func _refresh_multi_year_heatmap() -> void:
	if multi_year_heatmap == null:
		return

	if multi_year_heatmap.has_method("set_report"):
		multi_year_heatmap.call("set_report", report)
	else:
		push_error(
			"ScreeningResultsControl: ScreeningMultiYearHeatmap needs set_report(report)."
		)

func _refresh_crossyear_bumpmap() -> void:
	if crossyear_bumpmap == null:
		return

	if crossyear_bumpmap.has_method("set_report"):
		crossyear_bumpmap.call("set_report", report)
	else:
		push_error(
			"ScreeningResultsControl: ScreeningCrossyearBumpmap needs set_report(report)."
		)
# ---------------------------------------------------------------------------
# UI setup
# ---------------------------------------------------------------------------

func _connect_ui() -> void:
	year_dropdown.item_selected.connect(_on_year_selected)
	family_dropdown.item_selected.connect(_on_family_selected)
	top_n_spinbox.value_changed.connect(_on_top_n_changed)
	pass_only_checkbox.toggled.connect(_on_pass_only_toggled)

	if bar_chart.has_signal("feature_selected"):
		bar_chart.feature_selected.connect(_on_feature_selected)


func _populate_empty_state() -> void:
	year_dropdown.clear()
	year_dropdown.add_item("No years")

	family_dropdown.clear()
	family_dropdown.add_item("All")
	family_dropdown.set_item_metadata(0, "All")

	selected_year = -1
	selected_family = "All"

	if bar_chart.has_method("set_data"):
		bar_chart.call("set_data", [], 0.0)
		
	if crossyear_bumpmap != null and crossyear_bumpmap.has_method("clear_report"):
		crossyear_bumpmap.call("clear_report")


func _populate_year_dropdown() -> void:
	year_dropdown.clear()

	for year in available_years:
		year_dropdown.add_item(str(year))
		year_dropdown.set_item_metadata(year_dropdown.item_count - 1, year)

	var selected_idx := available_years.find(selected_year)

	if selected_idx < 0:
		selected_idx = 0
		selected_year = available_years[0]

	year_dropdown.select(selected_idx)


func _populate_family_dropdown() -> void:
	family_dropdown.clear()

	family_dropdown.add_item("All")
	family_dropdown.set_item_metadata(0, "All")

	var families := _extract_families_for_year(selected_year)

	for family in families:
		family_dropdown.add_item(family)
		family_dropdown.set_item_metadata(family_dropdown.item_count - 1, family)

	selected_family = "All"
	family_dropdown.select(0)


# ---------------------------------------------------------------------------
# Chart refresh
# ---------------------------------------------------------------------------

func _refresh_single_year_chart() -> void:
	if selected_year < 0:
		if bar_chart.has_method("set_data"):
			bar_chart.call("set_data", [], 0.0)
		return

	var year_data := _get_year_data(selected_year)

	if year_data.is_empty():
		push_warning(
			"ScreeningResultsControl: no data for selected year %s."
			% selected_year
		)

		if bar_chart.has_method("set_data"):
			bar_chart.call("set_data", [], 0.0)

		return

	var top_n := int(top_n_spinbox.value)
	var pass_only := pass_only_checkbox.button_pressed

	var rows := _build_single_year_bar_rows(
		year_data,
		default_round_index,
		top_n,
		pass_only,
		selected_family
	)

	var threshold := _get_year_threshold(year_data)

	if bar_chart.has_method("set_data"):
		bar_chart.call("set_data", rows, threshold)
	else:
		push_error(
			"ScreeningResultsControl: ScreeningSingleBarChart needs set_data(rows, threshold)."
		)


func _build_single_year_bar_rows(
	year_data: Dictionary,
	round_index: int,
	top_n: int,
	pass_only: bool,
	family_filter: String
) -> Array[Dictionary]:
	var rounds: Array = year_data.get("rounds", [])

	if rounds.is_empty():
		return []

	round_index = clampi(round_index, 0, rounds.size() - 1)

	var round_data: Dictionary = rounds[round_index]

	## Prefer all_columns because it gives pass/fail context.
	## Fall back to top_columns if all_columns is missing.
	var raw_columns: Array = round_data.get("all_columns", [])

	if raw_columns.is_empty():
		raw_columns = round_data.get("top_columns", [])

	var rows: Array[Dictionary] = []

	for raw_col in raw_columns:
		if typeof(raw_col) != TYPE_DICTIONARY:
			continue

		var col: Dictionary = raw_col

		var passes := bool(col.get("passes_fdr", false))

		if pass_only and not passes:
			continue

		var feature := str(col.get("feature", ""))
		var family := _get_column_family(col)

		if family_filter != "All" and family != family_filter:
			continue

		var row := {
			"feature": feature,
			"label": str(col.get("label", feature)),
			"family": family,
			"subfamily": str(col.get("subfamily", "")),
			"abs_r": float(col.get("abs_r", 0.0)),
			"passes_fdr": passes,
			"round": int(round_data.get("round", round_index + 1)),
		}

		rows.append(row)

	rows.sort_custom(
		func(a: Dictionary, b: Dictionary) -> bool:
			return float(a.get("abs_r", 0.0)) > float(b.get("abs_r", 0.0))
	)

	if top_n > 0 and rows.size() > top_n:
		rows = rows.slice(0, top_n)

	return rows


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

func _extract_years(source_report: Dictionary) -> Array[int]:
	var years: Array[int] = []

	for key in source_report.keys():
		var key_str := str(key)

		## Skip aggregate report block.
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

	## Defensive fallback in case some caller used integer year keys.
	if report.has(year) and typeof(report[year]) == TYPE_DICTIONARY:
		return report[year]

	return {}


func _get_year_threshold(year_data: Dictionary) -> float:
	var config: Dictionary = year_data.get("config", {})

	if config.has("min_abs_r"):
		return float(config["min_abs_r"])

	if config.has("computed_min_abs_r"):
		return float(config["computed_min_abs_r"])

	return 0.0


func _extract_families_for_year(year: int) -> Array[String]:
	var year_data := _get_year_data(year)
	var rounds: Array = year_data.get("rounds", [])

	var found := {}

	for raw_round in rounds:
		if typeof(raw_round) != TYPE_DICTIONARY:
			continue

		var round_data: Dictionary = raw_round

		var cols: Array = round_data.get("all_columns", [])

		if cols.is_empty():
			cols = round_data.get("top_columns", [])

		for raw_col in cols:
			if typeof(raw_col) != TYPE_DICTIONARY:
				continue

			var family := _get_column_family(raw_col)

			if family != "":
				found[family] = true

	var families: Array[String] = []

	for family in found.keys():
		families.append(str(family))

	families.sort()

	return families


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
# UI callbacks
# ---------------------------------------------------------------------------

func _on_year_selected(index: int) -> void:
	var meta = year_dropdown.get_item_metadata(index)

	if meta == null:
		return

	selected_year = int(meta)

	_populate_family_dropdown()
	_refresh_single_year_chart()


func _on_family_selected(index: int) -> void:
	var meta = family_dropdown.get_item_metadata(index)

	if meta == null:
		selected_family = "All"
	else:
		selected_family = str(meta)

	_refresh_single_year_chart()


func _on_top_n_changed(_value: float) -> void:
	_refresh_single_year_chart()


func _on_pass_only_toggled(_pressed: bool) -> void:
	_refresh_single_year_chart()


func _on_feature_selected(row: Dictionary) -> void:
	print(
		"Selected feature: %s | %s | abs_r=%.4f | passes=%s"
		% [
			str(row.get("feature", "")),
			str(row.get("label", "")),
			float(row.get("abs_r", 0.0)),
			str(row.get("passes_fdr", false)),
		]
	)
