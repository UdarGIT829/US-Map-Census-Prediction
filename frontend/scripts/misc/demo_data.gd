extends RefCounted
class_name DemoData

## Precomputed results for DEMO MODE (the web build has no backend).
##
## Files live in res://demo/ and are real server responses captured from the
## Python APIs, so they go through exactly the same UI code as live results:
##   template.json                    - /template, limited to the bundled runs
##   screening_<key>.json             - screening /run response
##   training_<key>.json              - ML /run response
## <key> is "<loader>_<site or metric slug>", e.g. cancer_lung_and_bronchus.

const DEMO_DIR := "res://demo/"


## Demo files ship gzipped (<name>.json.gz) because GitHub Pages doesn't
## compress the .pck; a plain .json is still accepted if present.
static func load_json(path: String) -> Dictionary:
	var text := ""

	if FileAccess.file_exists(path + ".gz"):
		var packed := FileAccess.get_file_as_bytes(path + ".gz")
		text = packed.decompress_dynamic(-1, FileAccess.COMPRESSION_GZIP).get_string_from_utf8()
	elif FileAccess.file_exists(path):
		text = FileAccess.get_file_as_string(path)
	else:
		push_warning("DemoData: missing demo file " + path)
		return {}

	var parsed = JSON.parse_string(text)

	if typeof(parsed) != TYPE_DICTIONARY:
		push_error("DemoData: could not parse " + path)
		return {}

	return parsed


static func template() -> Dictionary:
	return load_json(DEMO_DIR + "template.json")


static func run_key(loader: String, site: String, metric: String, tmpl: Dictionary) -> String:
	if loader == "cancer":
		return "cancer_" + _slug(site)

	## No metric picked: fall back to the loader's first (recommended) metric.
	if metric == "":
		var metrics: Array = tmpl.get("loaders", {}).get(loader, {}).get("metrics", [])
		metric = str(metrics[0]) if not metrics.is_empty() else ""

	return "%s_%s" % [loader, _slug(metric)]


static func has_run(kind: String, key: String) -> bool:
	var path := "%s%s_%s.json" % [DEMO_DIR, kind, key]
	return FileAccess.file_exists(path + ".gz") or FileAccess.file_exists(path)


static func load_run(kind: String, key: String, years: Array) -> Dictionary:
	var report := load_json("%s%s_%s.json" % [DEMO_DIR, kind, key])

	if report.is_empty() or years.is_empty():
		return report

	if kind == "screening":
		_keep_years(report, years)
	else:
		var per_year = report.get("sampler_report", {}).get("results", {}).get("per_year", null)
		if per_year is Dictionary:
			_keep_years(per_year, years)

	return report


## Bundled runs cover every demo year; honor the UI's year selection by
## dropping the year blocks that weren't picked.
static func _keep_years(blocks: Dictionary, years: Array) -> void:
	var wanted := {}
	for y in years:
		wanted[str(y)] = true

	for key in blocks.keys():
		var k := str(key)
		if k.is_valid_int() and not wanted.has(k):
			blocks.erase(key)


static func _slug(text: String) -> String:
	return text.to_lower().replace(" ", "_").replace(",", "")
