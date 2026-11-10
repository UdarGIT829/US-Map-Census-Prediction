extends Control

## 127.0.0.1, not 0.0.0.0: the servers *bind* 0.0.0.0, but a client can't
## connect to 0.0.0.0 on Windows.
const API_BASE     = "http://127.0.0.1:11650"
const HEALTH_URL   = API_BASE + "/health"
const TEMPLATE_URL = API_BASE + "/template"
const RUN_URL      = API_BASE + "/run"

# Phase 2 CatBoost recipe-sampling API
const ML_API_BASE  = "http://127.0.0.1:11651"
const ML_RUN_URL   = ML_API_BASE + "/run"
const ML_HEALTH_URL = ML_API_BASE + "/health"

const HEALTH_POLL_INTERVAL = 3.0  # seconds between health polls

## DEMO MODE: Screen/Train load precomputed results from res://demo/ instead of
## calling the servers. Forced on (and locked) in web builds, which have no
## backend. The checkbox lives in the main scene's top-right corner.
const DEMO_CHECKBOX_PATH := "DemoModeCheckBox"
const DEMO_FAKE_DELAY := 0.6  # seconds; a brief "working" beat so the click registers

# ── Node refs ─────────────────────────────────────────────────────────────────
@onready var FileDialogControl: FileDialog    = $FileDialog
@onready var http_req:          HTTPRequest   = $HTTPRequest
@onready var http_health:       HTTPRequest   = $HTTPRequestHealth
@onready var http_template:     HTTPRequest   = $HTTPRequestTemplate
@onready var SpreadsheetPreview: VBoxContainer = $"Spreadsheet-Preview"
@onready var ScreeningButton: Button               = $"HBoxContainer/CenterContainer2/Screening_Button"
@onready var TrainingButton: Button                = $"HBoxContainer/CenterContainer4/Training_Button"
@onready var GroupInput: OptionButton          = $"Spreadsheet-Preview/Controls/GroupContainer/GroupInput"
@onready var ExplanationCardsNode: ExplanationCards = get_node_or_null("ExplanationCards")

# ── Path A OptionButton node paths — set these in the Inspector ──────────────
@export var loader_option_button_path:  NodePath
@export var years_item_list_path:       NodePath
@export var metrics_option_button_path: NodePath
@export var sites_option_button_path:   NodePath
@export var crops_option_button_path:   NodePath

@export var sites_vbox: VBoxContainer
@export var crops_vbox: VBoxContainer

# ── Signals ───────────────────────────────────────────────────────────────────
signal results_received(report: Dictionary)
signal training_results_recieved(report: Dictionary)
signal health_updated(status: Dictionary)
signal template_received(template: Dictionary)

# ── File state (Path B) ───────────────────────────────────────────────────────
var csv_dict:      Dictionary = {}
var _file_loaded:  bool       = false
var _file_path:    String     = ""
var _preview_instance         = null

# ── Path selector ─────────────────────────────────────────────────────────────
var _use_loader: bool = true   # true = Path A (loader), false = Path B (user data)

# ── Path A params — wire controls to these vars ───────────────────────────────
var _loader:           String      = ""          # nothing selected until the user picks one
var _years:            Array[int]  = []          # empty = all available years
var _site:             String      = "All Invasive Cancer Sites Combined"
var _metric:           String      = ""          # empty = loader default
var _crop:             String      = "corn"
var _window:           int         = 1

# ── Path B params — wire controls to these vars ───────────────────────────────
var _fips_col:         String      = ""
var _target_col:       String      = ""
var _year_col:         String      = ""          # empty = use _single_year
var _single_year:      int         = 0           # used when _year_col is empty

# ── Shared screening params — wire controls to these vars ─────────────────────
var _threshold_mode:      String = "mean"
var _r_floor_fdr:         float  = 0.05
var _r_floor_convergence: float  = 0.10
var _stale_rounds:        int    = 3
var _top_n:               int    = 5
var _rounds:              int    = 999

# ── Server state ──────────────────────────────────────────────────────────────
var _server_processing: bool  = false
var _request_in_flight: bool  = false
var _health_timer:      float = 0.0
var _template:          Dictionary = {}

var demo_mode:          bool     = false
var _demo_checkbox:     CheckBox = null

# ── Phase 1 / Phase 2 result state ────────────────────────────────────────────
var _last_screening_report: Dictionary = {}
var _screened_signature: String = ""   # selection that _last_screening_report came from
var _pending_signature: String = ""    # selection captured when Screen was clicked
var _last_training_report: Dictionary = {}
var _active_request_kind: String = "" # "screening" or "training"

# ── Path A dropdown values — populated from template ─────────────────────────
var available_loaders:   Array[String] = []   # ["cancer", "opioid", "crop", "ufo"]
var available_years:     Array[int]    = []   # years for the currently selected loader
var available_sites:     Array[String] = []   # cancer only
var available_metrics:   Array[String] = []   # loader-specific
var available_crops:     Array[String] = []   # crop only
var available_windows:   Array[int]    = []   # [1] or [1,2,3,4,5] depending on loader


# ── Lifecycle ─────────────────────────────────────────────────────────────────

func _ready() -> void:
	http_health.request_completed.connect(_on_health_completed)
	http_template.request_completed.connect(_on_template_completed)
	http_req.request_completed.connect(_on_run_completed)

	_update_explanation()
	_setup_demo_checkbox()

	_fetch_health()
	_fetch_template()
	_update_screening_button()
	_update_training_button()


func _process(delta: float) -> void:
	# Poll health on interval (no server to poll in demo mode)
	if not demo_mode:
		_health_timer += delta
		if _health_timer >= HEALTH_POLL_INTERVAL:
			_health_timer = 0.0
			_fetch_health()

	# Keep buttons in sync
	_update_screening_button()
	_update_training_button()


func _update_screening_button() -> void:
	var missing := _missing_for_screening()
	var busy := _request_in_flight or _server_processing

	ScreeningButton.disabled = busy or missing != ""
	ScreeningButton.tooltip_text = missing if missing != "" else ("Working…" if busy else "Run statistical screening for this selection.")


func _update_training_button() -> void:
	if TrainingButton == null:
		return

	var missing := _missing_for_training()
	var busy := _request_in_flight or _server_processing

	TrainingButton.disabled = busy or missing != ""
	TrainingButton.tooltip_text = missing if missing != "" else ("Working…" if busy else "Run the CatBoost recipe sampler on the screened features.")


func _is_request_ready() -> bool:
	return _missing_for_screening() == ""


## "" when Screen can run; otherwise what the user still has to choose
## (shown as the disabled button's tooltip).
func _missing_for_screening() -> String:
	if not _use_loader:
		if _file_loaded and _fips_col != "" and _target_col != "" and (_year_col != "" or _single_year > 0):
			return ""
		return "Load a file and choose its FIPS, target and year columns."

	if _loader == "":
		return "Choose a loader."
	if available_years.is_empty():
		return "This loader has no available years."
	if _metric == "":
		return "Choose a metric."
	if _loader == "cancer" and _site == "":
		return "Choose a cancer site."

	if demo_mode and not DemoData.has_run("screening", DemoData.run_key(_loader, _site, _metric, _template)):
		return "No precomputed results for this selection in the demo."

	return ""


## Training must follow a screening of the *current* selection; otherwise it
## would pair one dataset's ML results with another's screening.
func _missing_for_training() -> String:
	if _last_screening_report.is_empty():
		return "Run Screen Dataset first."
	if _screened_signature != _selection_signature():
		return "The selection changed since the last screening. Run Screen Dataset again."
	return ""


func _selection_signature() -> String:
	return "%s|%s|%s|%s" % [_loader, _site, _metric, str(_years)]


# ── Health ────────────────────────────────────────────────────────────────────

func _fetch_health() -> void:
	if demo_mode:
		_server_processing = false
		health_updated.emit({"status": "ok", "demo": true})
		return

	var err = http_health.request(HEALTH_URL)
	if err != OK:
		push_error("Health request failed: " + str(err))


func _on_health_completed(_result, response_code, _headers, body) -> void:
	# A live response that lands after switching to demo mode is stale.
	if demo_mode:
		return

	if response_code != 200:
		push_error("Health check returned: " + str(response_code))
		return

	var json = JSON.new()
	if json.parse(body.get_string_from_utf8()) != OK:
		push_error("Health JSON parse failed")
		return

	var data: Dictionary = json.get_data()
	_server_processing = bool(data.get("processing", false))
	health_updated.emit(data)


# ── Template ──────────────────────────────────────────────────────────────────

func _fetch_template() -> void:
	if demo_mode:
		_apply_template(DemoData.template())
		return

	var err = http_template.request(TEMPLATE_URL)
	if err != OK:
		push_error("Template request failed: " + str(err))


func _on_template_completed(_result, response_code, _headers, body) -> void:
	# A live response that lands after switching to demo mode is stale.
	if demo_mode:
		return

	if response_code != 200:
		push_error("Template request returned: " + str(response_code))
		return

	var json = JSON.new()
	if json.parse(body.get_string_from_utf8()) != OK:
		push_error("Template JSON parse failed")
		return

	_apply_template(json.get_data())


func _apply_template(tmpl: Dictionary) -> void:
	_template = tmpl
	_loader = ""
	_years = []
	_populate_loader_options(_template)
	template_received.emit(_template)
	_update_explanation()
	if OS.has_feature("web") and not _web_example_shown:
		_web_example_shown = true
		_show_web_example.call_deferred()


# ── Web example ───────────────────────────────────────────────────────────────

## The web demo opens already showing results rather than an empty form:
## lung cancer is picked in the Dataset tab, both stages run on the bundled
## results, and the Screening Results tab is brought to the front. Every
## control stays live, so visitors can pick another dataset straight away.
const WEB_EXAMPLE_LOADER := "cancer"
const WEB_EXAMPLE_SITE := "Lung and Bronchus"
var _web_example_shown := false


func _show_web_example() -> void:
	if not _select_option(loader_option_button_path, WEB_EXAMPLE_LOADER, _on_loader_option_selected):
		return
	_select_option(sites_option_button_path, WEB_EXAMPLE_SITE, _on_sites_option_selected)
	if _missing_for_screening() != "":
		return
	_pending_signature = _selection_signature()
	await _run_demo("screening")
	if _missing_for_training() == "":
		await _run_demo("training")
	var tabs := get_parent().get_parent() as TabContainer   # Dataset tab -> TabContainer
	if tabs != null and tabs.get_tab_count() > 1:
		tabs.current_tab = 1


## Selects the item whose metadata is value and runs its handler. False if absent.
func _select_option(path: NodePath, value: String, handler: Callable) -> bool:
	var btn: OptionButton = get_node_or_null(path)
	if btn == null:
		return false
	for i in btn.item_count:
		if str(btn.get_item_metadata(i)) == value:
			btn.select(i)
			handler.call(i)
			return true
	return false


# ── Dataset explanation cards ─────────────────────────────────────────────────

## Rebuild the cards for the current loader / years / metric / site.
func _update_explanation() -> void:
	if ExplanationCardsNode == null:
		return

	ExplanationCardsNode.set_cards(
		DatasetNotes.build_cards(_loader, _years, available_years, _metric, _site)
	)


# ── Demo mode ─────────────────────────────────────────────────────────────────

func _setup_demo_checkbox() -> void:
	var scene_root := get_tree().current_scene
	if scene_root != null:
		_demo_checkbox = scene_root.get_node_or_null(DEMO_CHECKBOX_PATH)

	if OS.has_feature("web"):
		# No backend exists on the web build: demo mode is the only mode.
		demo_mode = true
		if _demo_checkbox != null:
			_demo_checkbox.button_pressed = true
			_demo_checkbox.disabled = true
			_demo_checkbox.tooltip_text = "Web demo: showing precomputed results for lung cancer, opioid overdose and UFO sighting data.\nRun the Python servers locally to analyze other datasets."
		return

	if _demo_checkbox == null:
		return

	demo_mode = _demo_checkbox.button_pressed
	_demo_checkbox.toggled.connect(_on_demo_mode_toggled)


func _on_demo_mode_toggled(on: bool) -> void:
	if _request_in_flight:
		# Don't switch sources mid-request; snap the checkbox back.
		_demo_checkbox.set_pressed_no_signal(demo_mode)
		return

	demo_mode = on
	_last_screening_report = {}
	_last_training_report = {}
	_health_timer = 0.0

	_fetch_health()
	_fetch_template()


func _run_demo(kind: String) -> void:
	if _request_in_flight:
		return

	var key := DemoData.run_key(_loader, _site, _metric, _template)

	if not DemoData.has_run(kind, key):
		push_warning("DEMO MODE: no precomputed %s results for '%s'." % [kind, key])
		return

	_active_request_kind = kind
	_request_in_flight = true
	_update_screening_button()
	_update_training_button()

	await get_tree().create_timer(DEMO_FAKE_DELAY).timeout

	var report := DemoData.load_run(kind, key, _years)

	_active_request_kind = ""
	_request_in_flight = false

	if report.is_empty():
		push_error("DEMO MODE: failed to load %s results for '%s'." % [kind, key])
		return

	_handle_report(kind, report)


# ── Path A option population ─────────────────────────────────────────────────

func _fill_item_list(path: NodePath, items: Array) -> void:
	if path.is_empty():
		return

	## Years use a two-handle timeline (YearRangeSlider).
	var slider := get_node_or_null(path) as YearRangeSlider
	if slider == null:
		push_error("YearRangeSlider not found at path: " + str(path))
		return

	slider.set_years(items)
	if not slider.range_changed.is_connected(_on_year_range_changed):
		slider.range_changed.connect(_on_year_range_changed)


func _connect_option_button(path: NodePath, callable: Callable) -> void:
	if path.is_empty():
		return

	var btn: OptionButton = get_node_or_null(path)
	if btn == null:
		return

	if not btn.item_selected.is_connected(callable):
		btn.item_selected.connect(callable)


func _fill_option_button(path: NodePath, items: Array, connect_signal: bool = false) -> void:
	if path.is_empty():
		return

	var btn: OptionButton = get_node_or_null(path)
	if btn == null:
		push_error("OptionButton not found at path: " + str(path))
		return

	btn.clear()
	btn.add_item("-", 0)          # always first — represents "none selected"
	btn.set_item_metadata(0, "")

	for i in items.size():
		var label = str(items[i])
		btn.add_item(label, i + 1)
		btn.set_item_metadata(i + 1, items[i])

	btn.select(0)

	if connect_signal and not btn.item_selected.is_connected(_on_loader_option_selected):
		btn.item_selected.connect(_on_loader_option_selected)


func _populate_loader_options(tmpl: Dictionary) -> void:
	var loaders_dict: Dictionary = tmpl.get("loaders", {})

	available_loaders.clear()

	for key in loaders_dict.keys():
		if not loaders_dict[key].has("error"):
			available_loaders.append(key)

	available_loaders.sort()

	_fill_option_button(loader_option_button_path, available_loaders, true)

	# Clear dependent dropdowns until a loader is chosen
	_fill_item_list(years_item_list_path, [])
	_fill_option_button(metrics_option_button_path, [])
	_fill_option_button(sites_option_button_path,   [])
	_fill_option_button(crops_option_button_path,   [])


func _on_loader_option_selected(index: int) -> void:
	if index == 0:
		# "-" selected — clear everything
		_loader = ""
		_fill_item_list(years_item_list_path, [])
		_fill_option_button(metrics_option_button_path, [])
		_fill_option_button(sites_option_button_path,   [])
		_fill_option_button(crops_option_button_path,   [])
		sites_vbox.hide()
		crops_vbox.hide()
		available_years.clear()
		_update_explanation()
		return

	var btn: OptionButton = get_node(loader_option_button_path)
	_loader = str(btn.get_item_metadata(index))

	refresh_loader_options(_loader)

	# Hide/show specific loader parameter controls
	if available_loaders[index - 1] == "cancer":
		sites_vbox.show()
	else:
		sites_vbox.hide()

	if available_loaders[index - 1] == "crop":
		crops_vbox.show()
	else:
		crops_vbox.hide()

	_update_explanation()


func refresh_loader_options(loader: String) -> void:
	if _template.is_empty() or not _template.has("loaders"):
		return

	var info: Dictionary = _template["loaders"].get(loader, {})
	if info.is_empty() or info.has("error"):
		return

	# Years
	_years = []
	available_years.clear()

	for yr in info.get("acs_years", []):
		available_years.append(int(yr))

	_fill_item_list(years_item_list_path, available_years)

	# Metrics
	_metric = ""
	available_metrics.clear()

	for m in info.get("metrics", []):
		available_metrics.append(str(m))

	_fill_option_button(metrics_option_button_path, available_metrics)
	_connect_option_button(metrics_option_button_path, _on_metrics_option_selected)

	## Only one choice: pick it so the user doesn't need an extra click.
	if available_metrics.size() == 1:
		var metric_btn: OptionButton = get_node_or_null(metrics_option_button_path)
		if metric_btn != null:
			metric_btn.select(1)   # index 0 is the "-" placeholder
		_metric = available_metrics[0]

	# Sites (cancer only)
	_site = ""
	available_sites.clear()

	if loader == "cancer":
		for s in info.get("sites", []):
			available_sites.append(str(s))

	_fill_option_button(sites_option_button_path, available_sites)
	_connect_option_button(sites_option_button_path, _on_sites_option_selected)

	## Only one choice: pick it so the user doesn't need an extra click.
	if available_sites.size() == 1:
		var site_btn: OptionButton = get_node_or_null(sites_option_button_path)
		if site_btn != null:
			site_btn.select(1)   # index 0 is the "-" placeholder
		_site = available_sites[0]

	# Crops (crop only)
	_crop = ""
	available_crops.clear()

	if loader == "crop":
		for c in info.get("crops", []):
			available_crops.append(str(c))

	_fill_option_button(crops_option_button_path, available_crops)
	_connect_option_button(crops_option_button_path, _on_crops_option_selected)

	# Windows — silently fixed at 1
	available_windows = [1]
	_window = 1


# ── Path A selection handlers ────────────────────────────────────────────────

func _on_year_range_changed(years: Array) -> void:
	## The full range is the same as "all years", which _years expresses as [].
	_years.clear()
	if years.size() != available_years.size():
		for y in years:
			_years.append(int(y))

	_update_explanation()


func _on_metrics_option_selected(index: int) -> void:
	if metrics_option_button_path.is_empty():
		return

	var btn: OptionButton = get_node_or_null(metrics_option_button_path)
	if btn == null:
		return

	_metric = "" if index == 0 else str(btn.get_item_metadata(index))
	_update_explanation()


func _on_sites_option_selected(index: int) -> void:
	if sites_option_button_path.is_empty():
		return

	var btn: OptionButton = get_node_or_null(sites_option_button_path)
	if btn == null:
		return

	_site = "" if index == 0 else str(btn.get_item_metadata(index))
	_update_explanation()


func _on_crops_option_selected(index: int) -> void:
	if crops_option_button_path.is_empty():
		return

	var btn: OptionButton = get_node_or_null(crops_option_button_path)
	if btn == null:
		return

	_crop = "" if index == 0 else str(btn.get_item_metadata(index))


# ── File upload (Path B) ──────────────────────────────────────────────────────

func _on_button_pressed() -> void:
	FileDialogControl.access = FileDialog.ACCESS_FILESYSTEM
	FileDialogControl.filters = ["*.csv,*.json ; CSV and JSON Files"]
	FileDialogControl.show()



func _on_column_values_ready(column_name: String, unique_values: Array) -> void:
	GroupInput.clear()
	GroupInput.add_item("All (%s)" % column_name, 0)
	GroupInput.set_item_metadata(0, {"col": column_name, "val": ""})

	for i in unique_values.size():
		GroupInput.add_item(str(unique_values[i]), i + 1)
		GroupInput.set_item_metadata(i + 1, {"col": column_name, "val": str(unique_values[i])})

	GroupInput.disabled = false


var _chosen_year:   String = "2022"
var _chosen_cancer: String = ""


func _on_year_chosen(year: String) -> void:
	_chosen_year = year


func _on_cancer_chosen(item_index: int) -> void:
	_chosen_cancer = GroupInput.get_item_text(item_index)
	var meta: Dictionary = GroupInput.get_item_metadata(item_index)
	_preview_instance.set_filter(meta["col"], meta["val"])


# ── Run ───────────────────────────────────────────────────────────────────────

func _on_screening_button_pressed() -> void:
	if not _is_request_ready():
		return

	_pending_signature = _selection_signature()

	if demo_mode:
		_run_demo("screening")
		return

	if _use_loader:
		send_loader_request()
	else:
		send_user_data_request()

	# Force an immediate health check so UI shows processing right away
	_fetch_health()


func _on_train_button_pressed() -> void:
	if _last_screening_report.is_empty():
		push_error("No screening report available. Run screening before training.")
		return

	if demo_mode:
		_run_demo("training")
		return

	var payload: Dictionary = {
		# Send the actual Phase 1 report object from Godot.
		# Godot does not need to know the server-side screening report filepath.
		"screening_report": _last_screening_report,

		# Output naming on the Phase 2 server.
		"run_name": _build_training_run_name(),

		# Pool construction:
		# Phase 1 filters the ACS feature universe. This builds the candidate pool
		# that the recipe sampler is allowed to draw from.
		"pool_name": "screened_hybrid_pool",
		"include_stable_fdr": true,
		"include_stable_fallback": true,
		"include_final_round_top_n": 20,
		"include_any_round_fdr": false,
		"include_any_round_above_threshold": false,
		"min_years_seen": 1,

		# Dataset rebuild:
		# If _years is empty, Phase 2 will use all successful years from screening.
		"strict_features": false,
		"write_csv": true,

		# CatBoost recipe sampling:
		# Use per_year first because it is easiest to visualize/debug in Godot.
		"mode": "per_year",
		"n_recipes": 200,
		"min_families": 1,
		"max_families": 5,
		"min_features": 8,
		"max_features": 30,
		"coverage_rounds": 5,
		"random_state": 42,
		"test_size": 0.25,

		# CatBoost params
		"iterations": 300,
		"depth": 3,
		"learning_rate": 0.03,

		# Aggregation / display params
		"min_rows": 10,
		"pair_min_count": 5,
		"top_n": 50,

		# For Godot development, return the full report.
		# Later you can set this false and load/report only summaries.
		"return_full_outputs": true
	}

	if _years.size() > 0:
		payload["years"] = _years

	_send_run(payload, ML_RUN_URL, "training")

	# Force an immediate health check so UI shows processing right away
	_fetch_health()


func _build_shared_params() -> Dictionary:
	return {
		"threshold_mode":      _threshold_mode,
		"r_floor_fdr":         _r_floor_fdr,
		"r_floor_convergence": _r_floor_convergence,
		"stale_rounds":        _stale_rounds,
		"top_n":               _top_n,
		"rounds":              _rounds,
	}


func _build_training_run_name() -> String:
	var parts := PackedStringArray()

	if _loader != "":
		parts.append(_loader)
	else:
		parts.append("user_data")

	if _loader == "cancer":
		var site_slug := _site.to_lower().replace(" ", "_").replace(",", "")
		if site_slug != "":
			parts.append(site_slug)
	elif _loader == "crop":
		if _crop != "":
			parts.append(_crop)
	elif _metric != "":
		parts.append(_metric)

	parts.append("catboost_recipe_sampling")

	return "_".join(parts)


func send_loader_request() -> void:
	var payload: Dictionary = _build_shared_params()
	payload["loader"] = _loader
	payload["site"]   = _site
	payload["metric"] = _metric
	payload["crop"]   = _crop
	payload["window"] = _window

	if _years.size() > 0:
		payload["years"] = _years

	_send_run(payload, RUN_URL, "screening")


func send_user_data_request() -> void:
	if not _file_loaded or csv_dict.is_empty():
		push_error("No file loaded for user data request.")
		return

	var user_data: Dictionary = {
		"rows":       csv_dict.get("rows", []),
		"fips_col":   _fips_col,
		"target_col": _target_col,
	}

	if _year_col != "":
		user_data["year_col"] = _year_col
	else:
		user_data["single_year"] = _single_year

	var payload: Dictionary = _build_shared_params()
	payload["user_data"] = user_data

	_send_run(payload, RUN_URL, "screening")


func _send_run(payload: Dictionary, url: String = RUN_URL, request_kind: String = "screening") -> void:
	if _request_in_flight:
		push_error("A request is already in flight.")
		return

	_active_request_kind = request_kind
	_request_in_flight = true
	_update_screening_button()
	_update_training_button()

	var body    = JSON.stringify(payload)
	var headers = ["Content-Type: application/json"]
	var err     = http_req.request(url, headers, HTTPClient.METHOD_POST, body)

	if err != OK:
		push_error("%s request failed: %s" % [request_kind.capitalize(), str(err)])
		_active_request_kind = ""
		_request_in_flight = false
		_update_screening_button()
		_update_training_button()


# ── Response ──────────────────────────────────────────────────────────────────

func _on_run_completed(_result, response_code, _headers, body) -> void:
	# Force a health update so processing=false is reflected immediately
	_fetch_health()
	print("HERE")

	var request_kind := _active_request_kind
	if request_kind == "":
		request_kind = "run"

	_active_request_kind = ""
	_request_in_flight = false
	_update_screening_button()
	_update_training_button()

	if response_code != 200:
		var msg = body.get_string_from_utf8()
		push_error("%s API error %d: %s" % [request_kind.capitalize(), response_code, msg])
		return

	var json = JSON.new()
	if json.parse(body.get_string_from_utf8()) != OK:
		push_error("%s response JSON parse failed" % request_kind.capitalize())
		return

	_handle_report(request_kind, json.get_data())


## Shared by live HTTP responses and DEMO MODE's bundled results.
func _handle_report(request_kind: String, report: Dictionary) -> void:
	if request_kind == "training":
		_last_training_report = report

		training_results_recieved.emit(report)
	else:
		_last_screening_report = report
		_screened_signature = _pending_signature

		# Main Phase 1 screening report.
		results_received.emit(report)


# ── Convenience getters for downstream UI ─────────────────────────────────────

func get_last_screening_report() -> Dictionary:
	return _last_screening_report


func get_last_training_report() -> Dictionary:
	return _last_training_report


func get_training_sampler_report() -> Dictionary:
	return _last_training_report.get("sampler_report", {})


func get_training_per_year_results() -> Dictionary:
	var sampler_report: Dictionary = get_training_sampler_report()
	var results: Dictionary = sampler_report.get("results", {})
	return results.get("per_year", {})


func get_training_year_result(year: int) -> Dictionary:
	var per_year: Dictionary = get_training_per_year_results()
	return per_year.get(str(year), {})


# ── File parsing ──────────────────────────────────────────────────────────────

func load_file_to_dict(path: String) -> Dictionary:
	var file = FileAccess.open(path, FileAccess.READ)
	if file == null:
		push_error("Failed to open file: " + path)
		return {}

	var content = file.get_as_text()
	file.close()

	match path.get_extension().to_lower():
		"json":
			return parse_json_to_dict(content)
		"csv":
			return parse_csv_to_dict(content)
		_:
			push_error("Unsupported file type: " + path.get_extension())
			return {}


func parse_json_to_dict(content: String) -> Dictionary:
	var json = JSON.new()
	if json.parse(content) != OK:
		push_error("JSON parse error: " + json.get_error_message())
		return {}

	var result = json.get_data()

	if result is Dictionary:
		return result

	if result is Array:
		var dict: Dictionary = {}
		for i in result.size():
			dict[i] = result[i]
		return dict

	return {}


func parse_csv_to_dict(content: String) -> Dictionary:
	var lines = content.split("\n", false)
	if lines.size() < 2:
		push_error("CSV has no data rows")
		return {}

	var headers = lines[0].split(",")
	for i in headers.size():
		headers[i] = headers[i].strip_edges()

	var rows: Array = []

	for i in range(1, lines.size()):
		var values = lines[i].split(",")
		var row: Dictionary = {}

		for j in headers.size():
			var raw = values[j].strip_edges() if j < values.size() else ""
			row[headers[j]] = _coerce_value(raw)

		rows.append(row)

	return {"rows": rows}


func _coerce_value(raw: String):
	if raw.is_valid_int():
		return raw.to_int()

	if raw.is_valid_float():
		return raw.to_float()

	return raw
