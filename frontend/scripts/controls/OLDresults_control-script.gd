extends Control

@export var annotated_results   : Array
@export var family_results      : Array
@export var pairwise_results      : Array

@onready var L_Tree : Tree = $BoxContainer/HSplitContainer/HSplitContainer/VBoxContainer/Tree
@onready var R_Tree : Tree = $BoxContainer/HSplitContainer/HSplitContainer/VBoxContainer2/Tree2

# I will link the signal in the inspector
@onready var export_button : Button = $ExportButton
@onready var exportDialogueAnnotated : FileDialog = $FileDialogAnnotated
@onready var exportDialogueFamily : FileDialog = $FileDialogFamily
@onready var exportDialoguePairwise : FileDialog = $FileDialogPairwise

@onready var resultsSelector : VBoxContainer = $BoxContainer/HSplitContainer/ResultsSelector

# Column layout for the right tree
const FEATURE_COLUMNS = [
	{ "key": "measure",   "title": "Measure",    "min_width": 80,  "visible": false },
	{ "key": "category",  "title": "Category",   "min_width": 160, "visible": false  },
	{ "key": "total",     "title": "Population",  "min_width": 140, "visible": true  },
	{ "key": "group",     "title": "Group",      "min_width": 120, "visible": true  },
	{ "key": "subgroup",  "title": "Subgroup",   "min_width": 140, "visible": true  },
	{ "key": "detail",    "title": "Detail",     "min_width": 120, "visible": true  },
	{ "key": "score",     "title": "Score",      "min_width": 80,  "visible": true  },
	{ "key": "count",     "title": "# Recipes",  "min_width": 80,  "visible": true  },
]

# Maps visible column index → FEATURE_COLUMNS index
var _col_index_map: Array = []

func parse_dp_label(raw_label: String) -> Dictionary:
	var parts: Array = raw_label.split("!!")
	return {
		"measure":  parts[0] if parts.size() > 0 else "",
		"category": parts[1] if parts.size() > 1 else "",
		"total":    parts[2] if parts.size() > 2 else "",
		"group":    parts[3] if parts.size() > 3 else "",
		"subgroup": parts[4] if parts.size() > 4 else "",
		"detail":   parts[5] if parts.size() > 5 else "",
		"raw":      raw_label,
	}

func _setup_feature_tree_columns() -> void:
	_col_index_map.clear()
	var visible_cols: Array = []
	for i in FEATURE_COLUMNS.size():
		if FEATURE_COLUMNS[i]["visible"]:
			_col_index_map.append(i)
			visible_cols.append(FEATURE_COLUMNS[i])

	R_Tree.columns = visible_cols.size()
	R_Tree.set_column_titles_visible(true)

	for i in visible_cols.size():
		var col_def = visible_cols[i]
		R_Tree.set_column_title(i, col_def["title"])
		R_Tree.set_column_custom_minimum_width(i, col_def["min_width"])
		# Only the label columns expand, score/count stay fixed
		var is_fixed = col_def["key"] in ["score", "count", "measure"]
		R_Tree.set_column_expand(i, not is_fixed)

func _set_feature_item_columns(item: TreeItem, parsed: Dictionary, score: float, count: int) -> void:
	var data = parsed.duplicate()
	data["score"] = "%.4f" % score
	data["count"]  = str(count)

	for vis_i in _col_index_map.size():
		var col_def = FEATURE_COLUMNS[_col_index_map[vis_i]]
		item.set_text(vis_i, data.get(col_def["key"], ""))

# Toggle a column on/off by its key, then rebuild
func toggle_column(key: String, visible: bool) -> void:
	for col_def in FEATURE_COLUMNS:
		if col_def["key"] == key:
			col_def["visible"] = visible
			break
	_populate_feature_tree()  # repopulate with new column layout

func _push_results(analysis_uid:StringName, annotated_results:Array, family_results:Array, pairwise_scores:Array):
	resultsSelector.add_results(analysis_uid,annotated_results,family_results,pairwise_scores)
	
	# Automatically select this results key:
	resultsSelector.select_last_results()
	# This will trigger signals that will automatically call _do_update_Trees
	return

func _do_update_Trees() -> void:
	# Every selection calls this and it sets internal variables to the selected results
	var thisResults:	Array= resultsSelector.get_results()
	var p_annotated:	Array = thisResults[0]
	var p_family: 		Array = thisResults[1]
	var p_pairscores:	Array = thisResults[2]

	if not p_annotated or not p_family:
		push_error("Update called but no results loaded")
		return

	annotated_results = p_annotated
	family_results    = p_family
	pairwise_results = p_pairscores

	_populate_family_tree()
	_populate_feature_tree()


# ── LEFT TREE: Top Family → Subfamily ─────────────────────────────────────────
func _populate_family_tree() -> void:
	L_Tree.clear()
	L_Tree.set_column_titles_visible(true)
	L_Tree.set_column_expand(0, true)
	L_Tree.columns = 2
	L_Tree.set_column_title(0, "Family")
	L_Tree.set_column_title(1, "Avg Score")

	var root: TreeItem = L_Tree.create_item()
	root.set_text(0, "Families")
	L_Tree.hide_root = true

	# Group by top_family
	var top_families: Dictionary = {}
	for row in family_results:
		var tf: String = row.get("top_family", "Unknown")
		if tf not in top_families:
			top_families[tf] = []
		top_families[tf].append(row)

	# Sort top families by their best subfamily score
	var sorted_tops: Array = top_families.keys()
	sorted_tops.sort_custom(func(a, b):
		var best_a = top_families[a].map(func(r): return r["mean_score"]).max()
		var best_b = top_families[b].map(func(r): return r["mean_score"]).max()
		return best_a > best_b
	)

	for tf in sorted_tops:
		var subfams: Array = top_families[tf]
		var top_score: float = subfams.map(func(r): return r["mean_score"]).max()

		var tf_item: TreeItem = L_Tree.create_item(root)
		tf_item.set_text(0, tf)
		tf_item.set_text(1, "%.4f" % top_score)

		# Sort subfamilies by score descending
		subfams.sort_custom(func(a, b): return a["mean_score"] > b["mean_score"])

		for row in subfams:
			var sf_item: TreeItem = L_Tree.create_item(tf_item)
			sf_item.set_text(0, "  " + row.get("subfamily", "Unknown"))
			sf_item.set_text(1, "%.4f" % row["mean_score"])
			sf_item.set_metadata(0, row)  # store full row for later use


# ── RIGHT TREE: Top Family → Subfamily → Feature Label ────────────────────────
func _populate_feature_tree() -> void:
	R_Tree.clear()
	_setup_feature_tree_columns()

	var root: TreeItem = R_Tree.create_item()
	R_Tree.hide_root = true

	# Build nested dict: top_family → subfamily → [features]
	var tree_data: Dictionary = {}
	for row in annotated_results:
		var tf: String = row.get("top_family", "Unknown")
		var sf: String = row.get("subfamily", "Unknown")
		if tf not in tree_data:
			tree_data[tf] = {}
		if sf not in tree_data[tf]:
			tree_data[tf][sf] = []
		tree_data[tf][sf].append(row)

	# Sort top families by their highest scoring feature
	var sorted_tops: Array = tree_data.keys()
	sorted_tops.sort_custom(func(a, b):
		return _best_score_in_family(tree_data[a]) > _best_score_in_family(tree_data[b])
	)

	for tf in sorted_tops:
		# Family header row — only populate score column, leave label cols blank
		var tf_item: TreeItem = R_Tree.create_item(root)
		_set_feature_item_columns(tf_item, {"measure":"","category":tf,"total":"","group":"","subgroup":"","detail":"","raw":tf}, _best_score_in_family(tree_data[tf]), 0)

		var sorted_subs: Array = tree_data[tf].keys()
		sorted_subs.sort_custom(func(a, b):
			var best_a = tree_data[tf][a].map(func(r): return r["mean_score"]).max()
			var best_b = tree_data[tf][b].map(func(r): return r["mean_score"]).max()
			return best_a > best_b
		)

		for sf in sorted_subs:
			var features: Array = tree_data[tf][sf]
			features.sort_custom(func(a, b): return a["mean_score"] > b["mean_score"])

			# Subfamily header row
			var sf_item: TreeItem = R_Tree.create_item(tf_item)
			_set_feature_item_columns(sf_item, {"measure":"","category":"","total":sf,"group":"","subgroup":"","detail":"","raw":sf}, features[0]["mean_score"], 0)

			for row in features:
				# Leaf — parse the full !! label into columns
				var parsed = parse_dp_label(row.get("label", row.get("feature", "")))
				var f_item: TreeItem = R_Tree.create_item(sf_item)
				_set_feature_item_columns(f_item, parsed, row["mean_score"], row.get("count", 0))
				f_item.set_metadata(0, row)

func _best_score_in_family(subfam_dict: Dictionary) -> float:
	var best: float = -INF
	for sf in subfam_dict.values():
		for row in sf:
			if row["mean_score"] > best:
				best = row["mean_score"]
	return best
	
func _on_export_pressed_annotated() -> void:
	var results_idx :int= resultsSelector.current_idx
	var current_fname :String= resultsSelector.resultsButtonsMapping[results_idx]
	var full_fname :String= current_fname + ".json"
	exportDialogueAnnotated.set_current_file(full_fname)

	exportDialogueAnnotated.filters = ["*.csv ; CSV Files", "*.json ; JSON Files"]
	exportDialogueAnnotated.file_mode = FileDialog.FILE_MODE_SAVE_FILE
	
	exportDialogueAnnotated.popup_centered()
	
func _on_export_pressed_family() -> void:
	var results_idx :int= resultsSelector.current_idx
	var current_fname :String= resultsSelector.resultsButtonsMapping[results_idx]
	var full_fname :String= current_fname + ".json"
	exportDialogueFamily.set_current_file(full_fname)

	exportDialogueFamily.filters = ["*.csv ; CSV Files", "*.json ; JSON Files"]
	exportDialogueFamily.file_mode = FileDialog.FILE_MODE_SAVE_FILE
	
	exportDialogueFamily.popup_centered()

func _on_export_pressed_pairwise() -> void:
	var results_idx :int= resultsSelector.current_idx
	var current_fname :String= resultsSelector.resultsButtonsMapping[results_idx]
	var full_fname :String= current_fname + ".json"
	exportDialoguePairwise.set_current_file(full_fname)

	exportDialoguePairwise.filters = ["*.csv ; CSV Files", "*.json ; JSON Files"]
	exportDialoguePairwise.file_mode = FileDialog.FILE_MODE_SAVE_FILE
	
	exportDialoguePairwise.popup_centered()

	
	

func _on_file_selected_annotated(path: String) -> void:
	if path.ends_with(".json"):
		_export_annotated_json(path)
	else:
		_export_csv(path)

func _on_file_selected_family(path: String) -> void:
	if path.ends_with(".json"):
		_export_families_json(path)
	else:
		_export_csv(path)

func _on_file_selected_pairwise(path: String) -> void:
	if path.ends_with(".json"):
		_export_pairwise_json(path)
	else:
		_export_csv(path)

func _export_csv(path: String) -> void:
	var file = FileAccess.open(path, FileAccess.WRITE)
	if not file:
		push_error("Could not open file for writing: " + path)
		return

	# Header row
	file.store_line("top_family,subfamily,label,score,count")

	for row in annotated_results:
		var parsed = parse_dp_label(row.get("label", row.get("feature", "")))
		var line = ",".join([
			_csv_escape(row.get("top_family", "")),
			_csv_escape(row.get("subfamily", "")),
			_csv_escape(parsed.get("raw", "")),
			"%.4f" % row["mean_score"],
			str(row.get("count", 0))
		])
		file.store_line(line)

	file.close()

func _export_annotated_json(path: String) -> void:
	var file = FileAccess.open(path, FileAccess.WRITE)
	if not file:
		push_error("Could not open file for writing: " + path)
		return

	file.store_string(JSON.stringify(annotated_results, "\t"))
	file.close()

func _export_families_json(path: String) -> void:
	var file = FileAccess.open(path, FileAccess.WRITE)
	if not file:
		push_error("Could not open file for writing: " + path)
		return

	file.store_string(JSON.stringify(family_results, "\t"))
	file.close()

func _export_pairwise_json(path: String) -> void:
	var file = FileAccess.open(path, FileAccess.WRITE)
	if not file:
		push_error("Could not open file for writing: " + path)
		return

	file.store_string(JSON.stringify(pairwise_results, "\t"))
	file.close()

# Wraps a field in quotes if it contains commas, quotes, or newlines
func _csv_escape(value: String) -> String:
	if "," in value or "\"" in value or "\n" in value:
		return "\"" + value.replace("\"", "\"\"") + "\""
	return value
