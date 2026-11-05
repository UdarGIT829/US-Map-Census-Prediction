class_name TrainingResultsTab
extends Control

signal parsed_report_ready(parsed_report: Dictionary)
signal selected_year_changed(year: String, year_data: Dictionary)

@export var year_option_path: NodePath
@export var summary_label_path: NodePath
@export var sunburst_chart_path: NodePath
@export var pairwise_heatmap_path: NodePath

@export var default_top_feature_limit: int = 50
@export var default_pair_feature_limit: int = 70

@export var _raw_report: Dictionary = {}
@export var _parsed_report: Dictionary = {}
@export var _selected_year: String = ""

@onready var year_option: OptionButton = get_node_or_null(year_option_path)
@onready var summary_label: Label = get_node_or_null(summary_label_path)
@onready var sunburst_chart: Control = get_node_or_null(sunburst_chart_path)
@onready var pairwise_heatmap: Control = get_node_or_null(pairwise_heatmap_path)


const HEADER_GAP := 12.0
const RESULTS_TABS_PATH := "BoxContainer/TabContainer"


func _ready() -> void:
	if year_option != null:
		year_option.item_selected.connect(_on_year_option_selected)

	## The year dropdown + summary sit in the tab bar, right after the last tab.
	## Tab widths change with the theme, so position them from the real tab bar
	## instead of fixed offsets (which overlapped "Pairwise Scores" in Arcade).
	resized.connect(_layout_header)
	ThemeManager.theme_changed.connect(func(_on): _layout_header_next_frame())
	if year_option != null:
		year_option.resized.connect(_layout_header)
	_layout_header_next_frame()


func _layout_header_next_frame() -> void:
	## Tab sizes update after the theme propagates; measure once that's done.
	await get_tree().process_frame
	_layout_header()


func _layout_header() -> void:
	var tabs := get_node_or_null(RESULTS_TABS_PATH) as TabContainer
	if tabs == null or year_option == null:
		return

	var bar := tabs.get_tab_bar()
	if bar.tab_count == 0:
		return

	var last := bar.get_tab_rect(bar.tab_count - 1)
	var bar_origin := bar.global_position - global_position
	var bar_height := last.size.y

	var x := bar_origin.x + last.end.x + HEADER_GAP
	var dropdown_size := year_option.get_combined_minimum_size()
	year_option.position = Vector2(x, bar_origin.y + (bar_height - dropdown_size.y) * 0.5)
	year_option.size = dropdown_size

	if summary_label != null:
		var label_x := x + dropdown_size.x + HEADER_GAP
		var label_h := summary_label.get_combined_minimum_size().y
		summary_label.position = Vector2(label_x, bar_origin.y + (bar_height - label_h) * 0.5)
		summary_label.size = Vector2(maxf(0.0, size.x - label_x - 16.0), label_h)


# Public entry point.
# Connect your API/results controller signal to this.
func receive_training_report(report: Dictionary) -> void:
	print("Here")
	print("Payload keys: ", report.keys())

	if report.is_empty():
		push_warning("TrainingResultsTab received an empty report.")
		return

	_raw_report = _get_sampler_report(report)

	print("Sampler report keys: ", _raw_report.keys())

	if not _raw_report.has("results"):
		push_warning("Sampler report has no results key.")
		return

	var results: Dictionary = _raw_report.get("results", {})
	print("Results keys: ", results.keys())

	if not results.has("per_year"):
		push_warning("Sampler report results has no per_year key.")
		return

	print("per_year keys: ", results.get("per_year", {}).keys())

	_parsed_report = _parse_report_for_visuals(_raw_report)

	print("Parsed years: ", _parsed_report.get("years", []))
	print("Parsed per_year keys: ", _parsed_report.get("per_year", {}).keys())

	_populate_year_selector()
	_select_default_year()

	parsed_report_ready.emit(_parsed_report)


# Optional alias if your signal is literally named results_received.
func _on_results_received(report: Dictionary) -> void:
	receive_training_report(report)

func _get_sampler_report(report_payload: Dictionary) -> Dictionary:
	# Already the actual sampler report.
	if report_payload.has("results") and typeof(report_payload.get("results")) == TYPE_DICTIONARY:
		var results: Dictionary = report_payload.get("results", {})
		if results.has("per_year"):
			return report_payload

	# Wrapped training response from your API.
	if report_payload.has("sampler_report") and typeof(report_payload["sampler_report"]) == TYPE_DICTIONARY:
		return report_payload["sampler_report"]

	# Fallback, so warnings/debug still show something useful.
	return report_payload

func _populate_year_selector() -> void:
	if year_option == null:
		return

	year_option.clear()

	var years: Array = _parsed_report.get("years", [])
	for year in years:
		year_option.add_item(str(year))


func _select_default_year() -> void:
	var years: Array = _parsed_report.get("years", [])
	if years.is_empty():
		push_warning("Parsed report contains no years.")
		return

	# Default to the last year because your earlier visual approach used last round/year.
	_selected_year = str(years[years.size() - 1])

	if year_option != null:
		for i in range(year_option.item_count):
			if year_option.get_item_text(i) == _selected_year:
				year_option.select(i)
				break

	_apply_selected_year(_selected_year)


func _on_year_option_selected(index: int) -> void:
	if year_option == null:
		return

	var year := year_option.get_item_text(index)
	_apply_selected_year(year)


func _apply_selected_year(year: String) -> void:
	if not _parsed_report.has("per_year"):
		return

	var per_year: Dictionary = _parsed_report["per_year"]
	if not per_year.has(year):
		push_warning("Selected year not found in parsed report: %s" % year)
		return

	_selected_year = year
	var year_data: Dictionary = per_year[year]

	_update_summary_label(year, year_data)
	_send_year_data_to_charts(year, year_data)

	selected_year_changed.emit(year, year_data)


func _update_summary_label(year: String, year_data: Dictionary) -> void:
	if summary_label == null:
		return

	var summary: Dictionary = year_data.get("summary", {})
	var score_r2: Dictionary = summary.get("score_r2", {})
	print("PLACE")
	print(year)
	print(year_data)
	print(summary)
	print(score_r2)

	var valid_recipes := int(summary.get("valid_recipes", 0))
	var errored_recipes := int(summary.get("errored_recipes", 0))
	var screened_feature_count := int(summary.get("screened_feature_count", 0))

	var mean_r2 :float= score_r2.get("mean", 0.0)
	var max_r2 :float= score_r2.get("max", 0.0)
	var min_r2 :float= score_r2.get("min", 0.0)

	## Compact so it fits beside the tabs; the year is already in the dropdown.
	summary_label.text = (
		"%d features  ·  %d recipes  ·  R² mean %.3f (min %.3f, max %.3f)"
		% [
			screened_feature_count,
			valid_recipes,
			mean_r2,
			min_r2,
			max_r2
		]
	)

	if errored_recipes > 0:
		summary_label.text += "  ·  %d errors" % errored_recipes


func _send_year_data_to_charts(year: String, year_data: Dictionary) -> void:
	if sunburst_chart != null:
		if sunburst_chart.has_method("set_year_data"):
			sunburst_chart.call("set_year_data", year_data)
		elif sunburst_chart.has_method("set_tree"):
			sunburst_chart.call("set_tree", year_data.get("feature_tree", {}))

	if pairwise_heatmap != null:
		if pairwise_heatmap.has_method("set_year_data"):
			pairwise_heatmap.call("set_year_data", year_data)
		elif pairwise_heatmap.has_method("set_heatmap_data"):
			pairwise_heatmap.call(
				"set_heatmap_data",
				year_data.get("features", {}),
				year_data.get("pairs", {}),
				year_data.get("pair_feature_order", []),
				year_data.get("family_blocks", []),
				year_data.get("topic_blocks", []),
				year_data.get("score_ranges", {})
			)


# ==============================================================================
# Main parser
# ==============================================================================

func _parse_report_for_visuals(report: Dictionary) -> Dictionary:
	var parsed := {
		"metadata": _parse_metadata(report),
		"years": [],
		"per_year": {}
	}

	var per_year: Dictionary = report.get("results", {}).get("per_year", {})

	var years: Array = per_year.keys()
	years.sort_custom(func(a, b): return int(a) < int(b))

	for year_key in years:
		var year := str(year_key)
		parsed["years"].append(year)
		parsed["per_year"][year] = _parse_year(report, year)

	return parsed


func _parse_metadata(report: Dictionary) -> Dictionary:
	var dataset_manifest: Dictionary = report.get("dataset_manifest", {})
	var settings: Dictionary = report.get("settings", {})

	return {
		"status": str(report.get("status", "")),
		"runner": str(report.get("runner", "")),
		"artifact_type": str(report.get("artifact_type", "")),
		"research_framing": report.get("research_framing", {}),
		"dataset_manifest": dataset_manifest,
		"settings": settings,
		"pool_name": str(dataset_manifest.get("pool_name", "")),
		"n_features_in_pool": int(dataset_manifest.get("n_features_in_pool", 0)),
		"years_built": dataset_manifest.get("years_built", []),
		"mode": str(settings.get("mode", ""))
	}


func _parse_year(report: Dictionary, year: String) -> Dictionary:
	var year_data: Dictionary = report.get("results", {}).get("per_year", {}).get(year, {})

	var features := _parse_features(year_data)
	var pairs := _parse_pairs(year_data)

	_enrich_features_from_pairs(features, pairs)
	_compute_feature_pair_summaries(features, pairs)

	var feature_order := _build_feature_order(features, default_top_feature_limit)
	var pair_feature_order := _build_pair_feature_order(features, pairs, default_pair_feature_limit)

	var feature_tree := _build_feature_tree(features, feature_order)

	var family_blocks := _build_axis_blocks(pair_feature_order, features, "group")
	var topic_blocks := _build_topic_blocks(pair_feature_order, features)
	var subfamily_blocks := _build_subfamily_blocks(pair_feature_order, features)

	var score_ranges := _compute_score_ranges(features, pairs)

	return {
		"year": year,
		"summary": year_data.get("summary", {}),

		# Main normalized feature data.
		"features": features,
		"feature_order": feature_order,
		"feature_tree": feature_tree,

		# Main normalized pair data.
		"pairs": pairs,
		"pair_list": pairs.values(),
		"pair_feature_order": pair_feature_order,

		# Axis grouping data for the pairwise heatmap.
		"family_blocks": family_blocks,
		"topic_blocks": topic_blocks,
		"subfamily_blocks": subfamily_blocks,

		# Coloring/scaling metadata.
		"score_ranges": score_ranges,

		# Raw summaries preserved in case another graph wants them later.
		"top_columns_raw": year_data.get("top_columns", []),
		"top_families_raw": year_data.get("top_families", []),
		"top_pairs_raw": year_data.get("top_pairs", []),
		"top_recipes_raw": year_data.get("top_recipes", [])
	}


# ==============================================================================
# Feature parsing
# ==============================================================================

func _parse_features(year_data: Dictionary) -> Dictionary:
	var features := {}
	var top_columns: Array = year_data.get("top_columns", [])

	for row_variant in top_columns:
		if typeof(row_variant) != TYPE_DICTIONARY:
			continue

		var row: Dictionary = row_variant
		var feature := str(row.get("feature", ""))
		if feature == "":
			continue

		var group := str(row.get("group", "Unknown"))
		var label := str(row.get("label", feature))
		var raw_label := str(row.get("raw_label", label))
		var path := _make_feature_path(group, raw_label)

		features[feature] = {
			"feature": feature,
			"group": group,
			"label": label,
			"raw_label": raw_label,
			"path": path,

			# Convenience fields for sorting and grouping.
			"topic": _safe_path_part(path, 1),
			"subfamily": _safe_path_part(path, 2),
			"leaf_label": _safe_path_part(path, path.size() - 1),

			# Feature-level performance.
			"mean_score_r2": float(row.get("mean_score_r2", 0.0)),
			"count": int(row.get("count", 1)),

			# Metadata from screening/report.
			"concept": str(row.get("concept", "")),
			"source_reasons": row.get("source_reasons", []),
			"years_seen": row.get("years_seen", []),

			# Pair-derived metrics. Filled later.
			"pair_lift_mean": 0.0,
			"pair_lift_weighted": 0.0,
			"pair_count": 0,
			"max_pair_lift": 0.0
		}

	return features


func _make_feature_path(group: String, raw_label: String) -> Array:
	var path: Array = [group]

	for part_variant in raw_label.split("!!"):
		var clean := str(part_variant).strip_edges()
		if clean == "":
			continue
		if clean == "Estimate":
			continue

		path.append(clean)

	return path


func _safe_path_part(path: Array, index: int) -> String:
	if index < 0:
		return ""
	if index >= path.size():
		return ""
	return str(path[index])


# ==============================================================================
# Pair parsing
# ==============================================================================

func _parse_pairs(year_data: Dictionary) -> Dictionary:
	var pairs := {}
	var top_pairs: Array = year_data.get("top_pairs", [])

	for row_variant in top_pairs:
		if typeof(row_variant) != TYPE_DICTIONARY:
			continue

		var row: Dictionary = row_variant

		var a := str(row.get("feature_a", ""))
		var b := str(row.get("feature_b", ""))

		if a == "" or b == "":
			continue

		var key := _pair_key(a, b)

		pairs[key] = {
			"key": key,
			"feature_a": a,
			"feature_b": b,

			"feature_a_label": str(row.get("feature_a_label", a)),
			"feature_b_label": str(row.get("feature_b_label", b)),
			"feature_a_group": str(row.get("feature_a_group", "")),
			"feature_b_group": str(row.get("feature_b_group", "")),

			"mean_score_r2": float(row.get("mean_score_r2", 0.0)),
			"baseline_score_r2": float(row.get("baseline_score_r2", 0.0)),
			"lift": float(row.get("lift", 0.0)),
			"co_occurrence_count": int(row.get("co_occurrence_count", 0))
		}

	return pairs


func _pair_key(a: String, b: String) -> String:
	if a < b:
		return a + "||" + b
	return b + "||" + a


func _enrich_features_from_pairs(features: Dictionary, pairs: Dictionary) -> void:
	for pair_variant in pairs.values():
		var pair: Dictionary = pair_variant
		_ensure_pair_feature(features, pair, "feature_a", "feature_a_label", "feature_a_group")
		_ensure_pair_feature(features, pair, "feature_b", "feature_b_label", "feature_b_group")


func _ensure_pair_feature(
	features: Dictionary,
	pair: Dictionary,
	feature_key: String,
	label_key: String,
	group_key: String
) -> void:
	var feature := str(pair.get(feature_key, ""))
	if feature == "":
		return

	if features.has(feature):
		return

	var group := str(pair.get(group_key, "Unknown"))
	if group == "":
		group = "Unknown"

	var label := str(pair.get(label_key, feature))
	var path: Array = [group, label]

	features[feature] = {
		"feature": feature,
		"group": group,
		"label": label,
		"raw_label": label,
		"path": path,

		"topic": _safe_path_part(path, 1),
		"subfamily": "",
		"leaf_label": label,

		# These are unknown because this feature was seen only through top_pairs.
		"mean_score_r2": 0.0,
		"count": 1,

		"concept": "",
		"source_reasons": [],
		"years_seen": [],

		"pair_lift_mean": 0.0,
		"pair_lift_weighted": 0.0,
		"pair_count": 0,
		"max_pair_lift": 0.0
	}


func _compute_feature_pair_summaries(features: Dictionary, pairs: Dictionary) -> void:
	var lift_sums := {}
	var lift_weighted_sums := {}
	var weight_sums := {}
	var counts := {}
	var max_lifts := {}

	for feature in features.keys():
		lift_sums[feature] = 0.0
		lift_weighted_sums[feature] = 0.0
		weight_sums[feature] = 0.0
		counts[feature] = 0
		max_lifts[feature] = -INF

	for pair_variant in pairs.values():
		var pair: Dictionary = pair_variant

		var a := str(pair.get("feature_a", ""))
		var b := str(pair.get("feature_b", ""))
		var lift := float(pair.get("lift", 0.0))
		var weight := float(max(1, int(pair.get("co_occurrence_count", 1))))

		for feature in [a, b]:
			if not features.has(feature):
				continue

			lift_sums[feature] += lift
			lift_weighted_sums[feature] += lift * weight
			weight_sums[feature] += weight
			counts[feature] += 1
			max_lifts[feature] = max(float(max_lifts[feature]), lift)

	for feature in features.keys():
		var n := int(counts[feature])
		var weight_sum := float(weight_sums[feature])

		if n <= 0:
			continue

		features[feature]["pair_lift_mean"] = float(lift_sums[feature]) / float(n)
		features[feature]["pair_lift_weighted"] = float(lift_weighted_sums[feature]) / max(weight_sum, 0.000001)
		features[feature]["pair_count"] = n
		features[feature]["max_pair_lift"] = float(max_lifts[feature])


# ==============================================================================
# Feature ordering
# ==============================================================================

func _build_feature_order(features: Dictionary, limit: int) -> Array:
	var out: Array = features.keys()

	out.sort_custom(func(a, b):
		var ma: Dictionary = features.get(a, {})
		var mb: Dictionary = features.get(b, {})

		var ka := _feature_sort_key(ma, str(a))
		var kb := _feature_sort_key(mb, str(b))

		if ka == kb:
			return float(ma.get("mean_score_r2", 0.0)) > float(mb.get("mean_score_r2", 0.0))

		return ka < kb
	)

	if limit > 0 and out.size() > limit:
		out = out.slice(0, limit)

	return out


func _build_pair_feature_order(features: Dictionary, pairs: Dictionary, limit: int) -> Array:
	var ids := {}

	for pair_variant in pairs.values():
		var pair: Dictionary = pair_variant
		ids[str(pair.get("feature_a", ""))] = true
		ids[str(pair.get("feature_b", ""))] = true

	var out: Array = ids.keys()
	out.erase("")

	out.sort_custom(func(a, b):
		var ma: Dictionary = features.get(a, {})
		var mb: Dictionary = features.get(b, {})

		var ka := _feature_sort_key(ma, str(a))
		var kb := _feature_sort_key(mb, str(b))

		if ka == kb:
			return str(a) < str(b)

		return ka < kb
	)

	if limit > 0 and out.size() > limit:
		# This keeps the chart readable, but it may remove some pair cells.
		# Later we can make this smarter by ranking by pair degree / max lift.
		out = out.slice(0, limit)

	return out


func _feature_sort_key(meta: Dictionary, fallback_feature: String) -> String:
	var group := str(meta.get("group", "ZZZ")).to_lower()
	var path: Array = meta.get("path", [])

	var path_text := ""
	for part in path:
		path_text += str(part).to_lower() + "///"

	return group + "///" + path_text + fallback_feature.to_lower()


# ==============================================================================
# Feature tree for sunburst
# ==============================================================================

func _build_feature_tree(features: Dictionary, feature_order: Array) -> Dictionary:
	var root := _make_tree_node("All Features", "root", 0)

	for feature_variant in feature_order:
		var feature := str(feature_variant)
		if not features.has(feature):
			continue

		var meta: Dictionary = features[feature]
		_insert_feature_into_tree(root, meta)

	_finalize_tree_node(root)
	_cleanup_tree_node(root)

	return root


func _make_tree_node(name: String, node_type: String, depth: int) -> Dictionary:
	return {
		"name": name,
		"type": node_type,
		"depth": depth,
		"children": [],
		"_child_index": {},

		"feature": "",
		"label": "",
		"path": [],

		"weight": 0.0,
		"score": 0.0,
		"score_weight": 0.0,
		"score_sum": 0.0,

		"pair_lift_weighted": 0.0,
		"pair_lift_sum": 0.0,
		"pair_lift_weight": 0.0,

		# Filled by the chart, not by the parser.
		"angle_start": 0.0,
		"angle_end": 0.0,
		"radius_inner": 0.0,
		"radius_outer": 0.0
	}


func _insert_feature_into_tree(root: Dictionary, meta: Dictionary) -> void:
	var node := root
	var path: Array = meta.get("path", [])

	if path.is_empty():
		path = [str(meta.get("group", "Unknown")), str(meta.get("label", meta.get("feature", "")))]

	for i in range(path.size()):
		var part := str(path[i])
		if part == "":
			continue

		var child_index: Dictionary = node["_child_index"]

		if not child_index.has(part):
			var node_type := _depth_to_node_type(i + 1)
			var child := _make_tree_node(part, node_type, i + 1)
			child_index[part] = child
			node["children"].append(child)

		node = child_index[part]

	node["type"] = "feature_leaf"
	node["feature"] = str(meta.get("feature", ""))
	node["label"] = str(meta.get("label", ""))
	node["path"] = path

	var weight := float(max(1, int(meta.get("count", 1))))
	var score := float(meta.get("mean_score_r2", 0.0))
	var pair_lift := float(meta.get("pair_lift_weighted", 0.0))

	node["weight"] += weight
	node["score_sum"] += score * weight
	node["score_weight"] += weight

	node["pair_lift_sum"] += pair_lift * weight
	node["pair_lift_weight"] += weight


func _depth_to_node_type(depth: int) -> String:
	match depth:
		1:
			return "family"
		2:
			return "topic"
		3:
			return "subfamily"
		_:
			return "category"


func _finalize_tree_node(node: Dictionary) -> void:
	for child_variant in node["children"]:
		var child: Dictionary = child_variant
		_finalize_tree_node(child)

	if node["children"].is_empty():
		node["score"] = float(node["score_sum"]) / max(float(node["score_weight"]), 0.000001)
		node["pair_lift_weighted"] = float(node["pair_lift_sum"]) / max(float(node["pair_lift_weight"]), 0.000001)
		node["weight"] = max(float(node["weight"]), 1.0)
		return

	var total_weight := 0.0
	var score_sum := 0.0
	var lift_sum := 0.0

	for child_variant in node["children"]:
		var child: Dictionary = child_variant

		var weight := float(child.get("weight", 0.0))
		total_weight += weight
		score_sum += float(child.get("score", 0.0)) * weight
		lift_sum += float(child.get("pair_lift_weighted", 0.0)) * weight

	node["weight"] = max(total_weight, 1.0)
	node["score"] = score_sum / max(total_weight, 0.000001)
	node["pair_lift_weighted"] = lift_sum / max(total_weight, 0.000001)

	node["children"].sort_custom(func(a, b):
		return float(a.get("weight", 0.0)) > float(b.get("weight", 0.0))
	)


func _cleanup_tree_node(node: Dictionary) -> void:
	node.erase("_child_index")

	for child_variant in node.get("children", []):
		var child: Dictionary = child_variant
		_cleanup_tree_node(child)


# ==============================================================================
# Heatmap axis blocks
# ==============================================================================

func _build_axis_blocks(feature_order: Array, features: Dictionary, field_name: String) -> Array:
	var blocks: Array = []

	if feature_order.is_empty():
		return blocks

	var current := ""
	var start := 0

	for i in range(feature_order.size()):
		var feature := str(feature_order[i])
		var meta: Dictionary = features.get(feature, {})
		var value := str(meta.get(field_name, "Unknown"))

		if i == 0:
			current = value
			start = 0
		elif value != current:
			blocks.append({
				"name": current,
				"start": start,
				"end": i - 1,
				"count": i - start
			})

			current = value
			start = i

	blocks.append({
		"name": current,
		"start": start,
		"end": feature_order.size() - 1,
		"count": feature_order.size() - start
	})

	return blocks


func _build_topic_blocks(feature_order: Array, features: Dictionary) -> Array:
	var blocks: Array = []

	if feature_order.is_empty():
		return blocks

	var current_key := ""
	var current_name := ""
	var start := 0

	for i in range(feature_order.size()):
		var feature := str(feature_order[i])
		var meta: Dictionary = features.get(feature, {})

		var group := str(meta.get("group", "Unknown"))
		var topic := str(meta.get("topic", meta.get("label", feature)))
		var key := group + "||" + topic

		if i == 0:
			current_key = key
			current_name = topic
			start = 0
		elif key != current_key:
			blocks.append({
				"name": current_name,
				"key": current_key,
				"start": start,
				"end": i - 1,
				"count": i - start
			})

			current_key = key
			current_name = topic
			start = i

	blocks.append({
		"name": current_name,
		"key": current_key,
		"start": start,
		"end": feature_order.size() - 1,
		"count": feature_order.size() - start
	})

	return blocks


func _build_subfamily_blocks(feature_order: Array, features: Dictionary) -> Array:
	var blocks: Array = []

	if feature_order.is_empty():
		return blocks

	var current_key := ""
	var current_name := ""
	var start := 0

	for i in range(feature_order.size()):
		var feature := str(feature_order[i])
		var meta: Dictionary = features.get(feature, {})

		var group := str(meta.get("group", "Unknown"))
		var topic := str(meta.get("topic", ""))
		var subfamily := str(meta.get("subfamily", meta.get("label", feature)))

		if subfamily == "":
			subfamily = str(meta.get("label", feature))

		var key := group + "||" + topic + "||" + subfamily

		if i == 0:
			current_key = key
			current_name = subfamily
			start = 0
		elif key != current_key:
			blocks.append({
				"name": current_name,
				"key": current_key,
				"start": start,
				"end": i - 1,
				"count": i - start
			})

			current_key = key
			current_name = subfamily
			start = i

	blocks.append({
		"name": current_name,
		"key": current_key,
		"start": start,
		"end": feature_order.size() - 1,
		"count": feature_order.size() - start
	})

	return blocks


# ==============================================================================
# Score ranges
# ==============================================================================

func _compute_score_ranges(features: Dictionary, pairs: Dictionary) -> Dictionary:
	var feature_scores: Array = []
	var feature_counts: Array = []
	var feature_pair_lifts: Array = []

	for meta_variant in features.values():
		var meta: Dictionary = meta_variant

		var score := float(meta.get("mean_score_r2", 0.0))
		if score > 0.0:
			feature_scores.append(score)

		feature_counts.append(float(meta.get("count", 0)))

		var pair_lift := float(meta.get("pair_lift_weighted", 0.0))
		if pair_lift != 0.0:
			feature_pair_lifts.append(pair_lift)

	var pair_lifts: Array = []
	var pair_mean_scores: Array = []
	var pair_baselines: Array = []
	var pair_counts: Array = []

	for pair_variant in pairs.values():
		var pair: Dictionary = pair_variant

		pair_lifts.append(float(pair.get("lift", 0.0)))
		pair_mean_scores.append(float(pair.get("mean_score_r2", 0.0)))
		pair_baselines.append(float(pair.get("baseline_score_r2", 0.0)))
		pair_counts.append(float(pair.get("co_occurrence_count", 0)))

	return {
		"feature_mean_score_r2": _range_from_values(feature_scores),
		"feature_count": _range_from_values(feature_counts),
		"feature_pair_lift_weighted": _range_from_values(feature_pair_lifts, true),

		"pair_lift": _range_from_values(pair_lifts, true),
		"pair_mean_score_r2": _range_from_values(pair_mean_scores),
		"pair_baseline_score_r2": _range_from_values(pair_baselines),
		"co_occurrence_count": _range_from_values(pair_counts)
	}


func _range_from_values(values: Array, include_abs_max: bool = false) -> Dictionary:
	if values.is_empty():
		var empty_range := {
			"min": 0.0,
			"max": 1.0
		}

		if include_abs_max:
			empty_range["abs_max"] = 1.0

		return empty_range

	var min_v := INF
	var max_v := -INF
	var abs_max := 0.0

	for value_variant in values:
		var value := float(value_variant)
		min_v = min(min_v, value)
		max_v = max(max_v, value)
		abs_max = max(abs_max, abs(value))

	var result := {
		"min": min_v,
		"max": max_v
	}

	if include_abs_max:
		result["abs_max"] = max(abs_max, 0.000001)

	return result


# ==============================================================================
# Debug helpers
# ==============================================================================

func get_parsed_report() -> Dictionary:
	return _parsed_report


func get_selected_year_data() -> Dictionary:
	if _selected_year == "":
		return {}

	return _parsed_report.get("per_year", {}).get(_selected_year, {})


func get_selected_year() -> String:
	return _selected_year


func print_selected_year_debug_summary() -> void:
	var year_data := get_selected_year_data()
	if year_data.is_empty():
		print("No selected year data.")
		return

	print("--- TrainingResultsTab Debug ---")
	print("Year: ", _selected_year)
	print("Features: ", year_data.get("features", {}).size())
	print("Pairs: ", year_data.get("pairs", {}).size())
	print("Feature tree children: ", year_data.get("feature_tree", {}).get("children", []).size())
	print("Pair axis features: ", year_data.get("pair_feature_order", []).size())
	print("Family blocks: ", year_data.get("family_blocks", []).size())
	print("Topic blocks: ", year_data.get("topic_blocks", []).size())
	print("Subfamily blocks: ", year_data.get("subfamily_blocks", []).size())
