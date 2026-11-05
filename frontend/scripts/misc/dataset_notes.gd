extends RefCounted
class_name DatasetNotes

## Builds the Dataset tab's explanation cards from res://content/dataset_notes.json.
##
## The JSON is keyed by loader, then by metric / site / year. Only problem
## years need an entry; consecutive years without one collapse into a single
## "2016–2019: No problems." line. 'acs_years' notes (Census-side changes)
## apply to every dataset and are appended to that year's line.
##
## Card ids are prefixed with the loader, so switching datasets swaps the whole
## set while picking a metric/site or moving the years only adds/updates cards.

const NOTES_PATH := "res://content/dataset_notes.json"

const ACCENT_SOURCE  := Color("#6EA8FE")
const ACCENT_MEANING := Color("#75D69C")
const ACCENT_RESULTS := Color("#F2C66D")
const ACCENT_METRIC  := Color("#D38CFF")
const ACCENT_SITE    := Color("#FF8FB1")
const ACCENT_YEARS   := Color("#8FD3D8")
const ACCENT_NEUTRAL := Color("#AAB2C0")

const COLOR_OK      := "#75d69c"
const COLOR_PROBLEM := "#f2c66d"

static var _notes: Dictionary = {}


static func _load() -> Dictionary:
	if _notes.is_empty():
		var parsed = JSON.parse_string(FileAccess.get_file_as_string(NOTES_PATH))
		if typeof(parsed) == TYPE_DICTIONARY:
			_notes = parsed
		else:
			push_warning("DatasetNotes: could not load " + NOTES_PATH)
	return _notes


## Cards for the current selection: [{id, title, body (BBCode), accent}].
## `years` empty = all available years.
static func build_cards(loader: String, years: Array, available_years: Array, metric: String, site: String) -> Array:
	var notes := _load()
	var cards: Array = []

	if loader == "":
		cards.append(_card("intro", "Explore a dataset", str(notes.get("intro", "")), ACCENT_NEUTRAL))
		return cards

	var info: Dictionary = notes.get("loaders", {}).get(loader, {})

	if info.is_empty():
		cards.append(_card(loader + ".none", loader.capitalize(), "No notes for this dataset yet.", ACCENT_NEUTRAL))
		return cards

	var title := str(info.get("title", loader.capitalize()))

	_append(cards, loader + ".source", title + " · Source", info.get("source", ""), ACCENT_SOURCE)
	_append(cards, loader + ".meaning", "What it measures", info.get("meaning", ""), ACCENT_MEANING)
	_append(cards, loader + ".results", "Overall results", info.get("results", ""), ACCENT_RESULTS)

	if metric != "":
		_append(cards, loader + ".metric", "Metric: " + metric, info.get("metrics", {}).get(metric, ""), ACCENT_METRIC)

	if loader == "cancer" and site != "":
		var site_note = info.get("sites", {}).get(site, info.get("site_default", ""))
		_append(cards, loader + ".site", "Site: " + site, site_note, ACCENT_SITE)

	var shown_years: Array = years if not years.is_empty() else available_years
	var year_lines := _year_lines(info.get("years", {}), notes.get("acs_years", {}), shown_years)

	if not year_lines.is_empty():
		_append(cards, loader + ".years", "Years " + _span_text(shown_years), "\n".join(year_lines), ACCENT_YEARS)

	return cards


## Plain-text rendering of the cards (used by the headless test).
static func build_text(loader: String, years: Array, available_years: Array, metric: String, site: String) -> String:
	var parts := PackedStringArray()
	for c in build_cards(loader, years, available_years, metric, site):
		parts.append("[%s] %s\n%s" % [c["id"], c["title"], c["body"]])
	return "\n\n".join(parts)


static func _card(id: String, title: String, body: String, accent: Color) -> Dictionary:
	return {"id": id, "title": title, "body": body, "accent": accent}


## Skip empty fields so missing JSON entries don't produce blank cards.
static func _append(cards: Array, id: String, title: String, body, accent: Color) -> void:
	var text := str(body)
	if text != "":
		cards.append(_card(id, title, text, accent))


static func _span_text(years: Array) -> String:
	if years.is_empty():
		return ""

	var lo := int(years[0])
	var hi := int(years[0])
	for y in years:
		lo = mini(lo, int(y))
		hi = maxi(hi, int(y))

	return str(lo) if lo == hi else "%d–%d" % [lo, hi]


## Problem years get their own line; runs of consecutive fine years collapse.
static func _year_lines(year_notes: Dictionary, acs_notes: Dictionary, years: Array) -> PackedStringArray:
	var sorted_years: Array[int] = []
	for y in years:
		sorted_years.append(int(y))
	sorted_years.sort()

	var lines := PackedStringArray()
	var run_start := -1
	var run_end := -1

	for y in sorted_years:
		var parts := PackedStringArray()
		for source in [year_notes, acs_notes]:
			var n := str(source.get(str(y), ""))
			if n != "":
				parts.append(n)
		var note := " ".join(parts)

		if note == "":
			if run_start != -1 and y == run_end + 1:
				run_end = y
			else:
				_flush_run(lines, run_start, run_end)
				run_start = y
				run_end = y
			continue

		_flush_run(lines, run_start, run_end)
		run_start = -1
		run_end = -1
		lines.append("[color=%s][b]%d:[/b][/color] %s" % [COLOR_PROBLEM, y, note])

	_flush_run(lines, run_start, run_end)
	return lines


static func _flush_run(lines: PackedStringArray, run_start: int, run_end: int) -> void:
	if run_start == -1:
		return

	var span := str(run_start) if run_start == run_end else "%d–%d" % [run_start, run_end]
	lines.append("[color=%s][b]%s:[/b][/color] No problems." % [COLOR_OK, span])
