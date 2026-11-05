# spreadsheet_preview.gd
extends Control

signal year_selected(year: String)
signal cancer_site_selected(code: String, name: String)
signal column_values_ready(column_name: String, unique_values: Array)

@onready var column_identifiers: HBoxContainer = $Identifiers
@onready var grid: Tree = $Tree
@onready var year_input: LineEdit = $Controls/YearContainer/YearInput
@onready var cancer_input: OptionButton = $Controls/GroupContainer/GroupInput

var _headers: Array = []
var _rows: Array = []

var _year_col: int = -1
var _code_col: int = -1
var _name_col: int = -1

# Filter state: column_name → selected value
var _active_filters: Dictionary = {}

const MAX_PREVIEW_ROWS = 200
const FILTER_COLORS: Array = [
	Color(0.2, 0.6, 1.0, 0.85),   # blue
	Color(0.2, 0.85, 0.5, 0.85),  # green
	Color(1.0, 0.75, 0.2, 0.85),  # amber
	Color(0.85, 0.3, 0.8, 0.85),  # purple
	Color(1.0, 0.4, 0.4, 0.85),   # red
]
func _populate_dropdowns() -> void:
	# Pull unique sorted years
	var years: Array = []
	var cancer_map: Dictionary = {}  # code → name

	for row in _rows:
		if _year_col >= 0:
			var y = str(row.get(_headers[_year_col], "")).strip_edges()
			if y != "" and y not in years:
				years.append(y)
		if _code_col >= 0 and _name_col >= 0:
			var code = str(row.get(_headers[_code_col], "")).strip_edges()
			var name = str(row.get(_headers[_name_col], "")).strip_edges()
			if code != "":
				cancer_map[code] = name

	years.sort()

	# Stuff them into the LineEdits as hints for now
	# (swap for OptionButton if you want a true dropdown)
	year_input.placeholder_text = "e.g. " + ", ".join(years.slice(0, 5)) + "..."

	# Store for validation
	year_input.set_meta("valid_years", years)
	cancer_input.set_meta("valid_codes", cancer_map)

func load_data(csv_dict: Dictionary) -> void:
	_rows = csv_dict.get("rows", [])
	if _rows.is_empty():
		return
	_headers = _rows[0].keys()
	_find_key_columns()
	_populate_grid()
	_populate_dropdowns()
	_rebuild_identifiers()
	grid.size_flags_vertical = Control.SIZE_EXPAND_FILL
	grid.size_flags_horizontal = Control.SIZE_EXPAND_FILL

func _find_key_columns() -> void:
	for i in _headers.size():
		var h: String = _headers[i].to_lower()
		if "year" in h:               _year_col = i
		if "cancer sites code" in h:  _code_col = i
		if h == "cancer sites":       _name_col = i

# ── Column title clicked → emit unique values for that column ─────────────────
func _on_column_title_clicked(column: int, _v: int) -> void:
	if column >= _headers.size():
		return
	var col_name: String = _headers[column]
	var seen: Dictionary = {}
	var unique: Array = []
	for row in _rows:
		var val = str(row.get(col_name, "")).strip_edges()
		if val != "" and val not in seen:
			seen[val] = true
			unique.append(val)
	unique.sort()
	emit_signal("column_values_ready", col_name, unique)

# ── Set or clear a filter for a column, then refresh ─────────────────────────
func set_filter(col_name: String, value: String) -> void:
	if value == "":
		_active_filters.erase(col_name)
	else:
		_active_filters[col_name] = value
	_populate_grid()
	_rebuild_identifiers()

func clear_all_filters() -> void:
	_active_filters.clear()
	_populate_grid()
	_rebuild_identifiers()

# ── Populate grid, respecting active filters ──────────────────────────────────
func _populate_grid() -> void:
	grid.clear()
	grid.hide_root = true
	grid.set_column_titles_visible(true)
	grid.columns = _headers.size()

	for i in _headers.size():
		grid.set_column_title(i, _headers[i])
		grid.set_column_custom_minimum_width(i, 120)
		grid.set_column_expand(i, true)

	var root: TreeItem = grid.create_item()
	var shown: int = 0

	for row in _rows:
		if shown >= MAX_PREVIEW_ROWS:
			break
		if not _row_passes_filters(row):
			continue
		var row_item: TreeItem = grid.create_item(root)
		for j in _headers.size():
			var val = str(row.get(_headers[j], ""))
			row_item.set_text(j, val)
			# Tint filtered columns so user can see what's active
			if _headers[j] in _active_filters:
				var color_idx = _active_filters.keys().find(_headers[j]) % FILTER_COLORS.size()
				row_item.set_custom_bg_color(j, Color(FILTER_COLORS[color_idx], 0.15))
		shown += 1

func _row_passes_filters(row: Dictionary) -> bool:
	for col_name in _active_filters:
		var row_val = str(row.get(col_name, "")).strip_edges()
		if row_val != _active_filters[col_name]:
			return false
	return true

# ── Rebuild the Identifiers HBox to show active filter chips ─────────────────
func _rebuild_identifiers() -> void:
	for child in column_identifiers.get_children():
		child.queue_free()

	if _active_filters.is_empty():
		var lbl: Label = Label.new()
		lbl.text = "Click a column header to filter"
		lbl.modulate = Color(1, 1, 1, 0.4)
		column_identifiers.add_child(lbl)
		return

	var color_idx: int = 0
	for col_name in _active_filters:
		var value = _active_filters[col_name]
		var color = FILTER_COLORS[color_idx % FILTER_COLORS.size()]

		# Chip container
		var chip: PanelContainer = PanelContainer.new()
		var style: StyleBoxFlat = StyleBoxFlat.new()
		style.bg_color = Color(color, 0.25)
		style.border_color = color
		style.set_border_width_all(1)
		style.set_corner_radius_all(4)
		chip.add_theme_stylebox_override("panel", style)

		var hbox: HBoxContainer = HBoxContainer.new()
		chip.add_child(hbox)

		var lbl: Label = Label.new()
		lbl.text = "%s = %s" % [col_name, value]
		lbl.modulate = color
		hbox.add_child(lbl)

		# ✕ clear button
		var btn: Button = Button.new()
		btn.text = "✕"
		btn.flat = true
		btn.modulate = color
		var captured_col = col_name  # capture for lambda
		btn.pressed.connect(func(): set_filter(captured_col, ""))
		hbox.add_child(btn)

		column_identifiers.add_child(chip)
		color_idx += 1
