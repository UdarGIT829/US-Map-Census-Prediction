extends Control
class_name ExplanationCards

## Flexbox-style wrap of explanation cards with springy layout animation.
##
## Cards live in an HFlowContainer, so Godot does the wrapping. To animate,
## each card is a PanelContainer inside a plain "slot" Control that the flow
## container positions. When the set of cards changes we (FLIP technique):
##   1. record where every card is on screen,
##   2. let the container re-flow,
##   3. offset each card back to its old spot inside its slot and spring it to 0.
## New cards pop in; removed cards fade out in an overlay while the rest close
## the gap.
##
## Feed it with set_cards([{id, title, body (BBCode), accent: Color}, ...]).

@export var min_card_width: float = 300.0
@export var gap: float = 12.0
@export var move_duration: float = 0.7
@export var appear_duration: float = 0.45
@export var stagger: float = 0.05

## Text auto-sizes between these so every card fits without a scrollbar.
@export var max_font_size: int = 14
@export var min_font_size: int = 11

const SCROLLBAR_ROOM := 14.0
const TITLE_GAP := 6.0          # VBox separation between title and body
const HEIGHT_SLACK := 4.0       # per card; bold glyphs / line spacing rounding

var _scroll: ScrollContainer
var _flow: HFlowContainer
var _overlay: Control

var _slots: Dictionary = {}   # id -> slot Control (its "card" meta is the PanelContainer)
var _card_width: float = 300.0
var _generation: int = 0
var _font_size: int = 14
var _last_specs: Array = []
var _bbcode_re := RegEx.create_from_string("\\[/?[a-z_]+(=[^\\]]*)?\\]")


func _ready() -> void:
	clip_contents = true

	_scroll = ScrollContainer.new()
	_scroll.set_anchors_and_offsets_preset(Control.PRESET_FULL_RECT)
	_scroll.horizontal_scroll_mode = ScrollContainer.SCROLL_MODE_DISABLED
	add_child(_scroll)

	_flow = HFlowContainer.new()
	_flow.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_flow.add_theme_constant_override("h_separation", int(gap))
	_flow.add_theme_constant_override("v_separation", int(gap))
	_scroll.add_child(_flow)

	## Removed cards fade out here, above the layout, so the flow can re-close.
	_overlay = Control.new()
	_overlay.set_anchors_and_offsets_preset(Control.PRESET_FULL_RECT)
	_overlay.mouse_filter = Control.MOUSE_FILTER_IGNORE
	add_child(_overlay)

	resized.connect(_update_card_width)
	_update_card_width()

	ThemeManager.theme_changed.connect(_on_theme_changed)


## Card content margins per theme: {left, right, top, bottom}. The single
## source for both the card styles and the font-size estimate, so the estimate
## always matches the real layout (a mismatch made cards spill in Rooftop).
func _card_margins() -> Dictionary:
	if ThemeManager.current == "rooftop":
		return {"left": 20, "right": 14, "top": 12, "bottom": 12}   # includes the 7px red bar
	return {"left": 16, "right": 14, "top": 12, "bottom": 12}


func _pad_x() -> float:
	var m := _card_margins()
	return float(m["left"] + m["right"])


func _pad_y() -> float:
	var m := _card_margins()
	return float(m["top"] + m["bottom"])


func _on_theme_changed(_theme_name: String) -> void:
	## Padding and fonts differ per theme, so redo sizing as well as styling.
	_choose_font_size(_last_specs)
	for slot in _slots.values():
		_style_card(_card_of(slot))
		_apply_font(slot)
		_apply_width(slot)


# ── Public API ───────────────────────────────────────────────────────────────

func set_cards(specs: Array) -> void:
	_generation += 1
	var generation := _generation

	_last_specs = specs
	_choose_font_size(specs)

	## 1. Where is everything on screen right now (including mid-animation)?
	var old_positions := {}
	for id in _slots:
		old_positions[id] = _card_of(_slots[id]).global_position

	var wanted := {}
	for spec in specs:
		wanted[str(spec.get("id", ""))] = true

	for id in _slots.keys():
		if not wanted.has(id):
			_remove_card(id)

	## 2. Add / update / order. New cards pop in right away (staggered).
	var new_count := 0
	for i in specs.size():
		var spec: Dictionary = specs[i]
		var id := str(spec.get("id", ""))

		if _slots.has(id):
			_update_card(_slots[id], spec)
			_apply_font(_slots[id])
		else:
			var slot := _make_card(spec)
			_slots[id] = slot
			_flow.add_child(slot)
			_animate_in(_card_of(slot), new_count * stagger)
			new_count += 1

		_flow.move_child(_slots[id], i)

	## 3. Let the flow container re-layout, then spring survivors into place.
	await get_tree().process_frame
	await get_tree().process_frame

	if generation != _generation:
		return   # a newer update already took over

	for id in old_positions:
		if not _slots.has(id):
			continue

		var slot: Control = _slots[id]
		var card := _card_of(slot)
		var delta: Vector2 = old_positions[id] - slot.global_position

		if delta.length() < 0.5:
			continue

		_kill_tween(card, "move_tween")
		card.position = delta

		var tween := card.create_tween()
		tween.tween_property(card, "position", Vector2.ZERO, move_duration) \
			.set_trans(Tween.TRANS_SPRING).set_ease(Tween.EASE_OUT)
		card.set_meta("move_tween", tween)


# ── Card construction ────────────────────────────────────────────────────────

func _make_card(spec: Dictionary) -> Control:
	var slot := Control.new()
	slot.mouse_filter = Control.MOUSE_FILTER_PASS

	var card := PanelContainer.new()
	card.mouse_filter = Control.MOUSE_FILTER_PASS

	var box := VBoxContainer.new()
	box.add_theme_constant_override("separation", 6)
	card.add_child(box)

	var title := Label.new()
	## Never let a long (e.g. uppercase italic) title force the card wider
	## than its column; trim with "…" instead.
	title.clip_text = true
	title.text_overrun_behavior = TextServer.OVERRUN_TRIM_ELLIPSIS
	box.add_child(title)

	var body := RichTextLabel.new()
	body.bbcode_enabled = true
	body.fit_content = true
	body.scroll_active = false
	body.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	body.selection_enabled = true
	box.add_child(body)

	slot.add_child(card)
	slot.set_meta("card", card)
	card.set_meta("title", title)
	card.set_meta("body", body)
	_apply_font(slot)

	## The card isn't in a container, so keep the slot sized to the card.
	card.minimum_size_changed.connect(_sync_slot.bind(slot))

	_update_card(slot, spec)
	_apply_width(slot)
	return slot


func _update_card(slot: Control, spec: Dictionary) -> void:
	var card := _card_of(slot)
	card.set_meta("accent", spec.get("accent", Color(0.67, 0.70, 0.75)))
	_style_card(card)

	var title: Label = card.get_meta("title")
	title.text = str(spec.get("title", ""))

	var body: RichTextLabel = card.get_meta("body")
	var text := str(spec.get("body", ""))
	if body.text != text:
		body.text = text


## Default: dark card with a colored accent bar on the left.
## Arcade:  a white-bordered navy "description box" with the colored title.
## Rooftop: a flat white card, thick red bar, dark text, uppercase title.
func _style_card(card: PanelContainer) -> void:
	var accent: Color = card.get_meta("accent", Color(0.67, 0.70, 0.75))
	var theme_name: String = ThemeManager.current

	var m := _card_margins()
	var title: Label = card.get_meta("title")
	var body: RichTextLabel = card.get_meta("body")
	var title_color := accent.lightened(0.15)
	var body_color := Color(0.80, 0.83, 0.89)
	title.uppercase = false

	var style: StyleBox

	if theme_name == "rooftop":
		## Near-black static-grain card with a thick red bar on the left.
		style = RooftopTheme.grain_box(RooftopTheme.RED, 7, m["left"] - 7, m["right"], m["top"])
		title_color = RooftopTheme.RED_HOVER
		body_color = Color(0.84, 0.86, 0.90)
		title.uppercase = true
	else:
		var flat := StyleBoxFlat.new()
		flat.content_margin_left = m["left"]
		flat.content_margin_right = m["right"]
		flat.content_margin_top = m["top"]
		flat.content_margin_bottom = m["bottom"]

		if theme_name == "arcade":
			flat.bg_color = Color(0.015, 0.02, 0.06, 0.86)
			flat.border_color = Color(0.90, 0.92, 0.97, 0.9)
			flat.set_border_width_all(2)
			flat.set_corner_radius_all(4)
			flat.shadow_color = Color(accent, 0.25)
			flat.shadow_size = 6
			title_color = accent.lightened(0.25)
		else:
			flat.bg_color = Color(0.105, 0.118, 0.141, 0.96)
			flat.border_color = accent
			flat.border_width_left = 3
			flat.set_corner_radius_all(6)

		style = flat

	card.add_theme_stylebox_override("panel", style)
	title.add_theme_color_override("font_color", title_color)
	body.add_theme_color_override("default_color", body_color)


func _remove_card(id: String) -> void:
	var slot: Control = _slots[id]
	_slots.erase(id)

	var card := _card_of(slot)
	_kill_tween(card, "move_tween")
	_kill_tween(card, "appear_tween")

	## Hand the card to the overlay at the same screen position, then fade it.
	var screen_pos := card.global_position
	slot.remove_child(card)
	_overlay.add_child(card)
	card.global_position = screen_pos
	slot.queue_free()

	card.pivot_offset = card.size * 0.5
	var tween := card.create_tween().set_parallel(true)
	tween.tween_property(card, "modulate:a", 0.0, 0.18)
	tween.tween_property(card, "scale", Vector2(0.92, 0.92), 0.18) \
		.set_trans(Tween.TRANS_QUAD).set_ease(Tween.EASE_IN)
	tween.chain().tween_callback(card.queue_free)


func _animate_in(card: Control, delay: float) -> void:
	card.modulate.a = 0.0
	card.scale = Vector2(0.88, 0.88)

	var tween := card.create_tween().set_parallel(true)
	tween.tween_property(card, "modulate:a", 1.0, appear_duration * 0.6).set_delay(delay)
	tween.tween_property(card, "scale", Vector2.ONE, appear_duration).set_delay(delay) \
		.set_trans(Tween.TRANS_BACK).set_ease(Tween.EASE_OUT)
	card.set_meta("appear_tween", tween)


# ── Sizing ───────────────────────────────────────────────────────────────────

## Flexbox-ish: as many columns of >= min_card_width as fit, stretched evenly.
func _update_card_width() -> void:
	if _scroll == null:
		return

	var available := maxf(min_card_width, _scroll.size.x - SCROLLBAR_ROOM)
	var columns := maxi(1, int((available + gap) / (min_card_width + gap)))
	_card_width = floorf((available - gap * (columns - 1)) / columns)

	## A new width changes how text wraps, so re-pick the font size too.
	_choose_font_size(_last_specs)

	for slot in _slots.values():
		_apply_font(slot)
		_apply_width(slot)


## Pick the largest font size whose laid-out cards fit the visible height.
## Heights are estimated by measuring each card's wrapped text with the real
## font, then stacking rows the way HFlowContainer does (row height = tallest
## card in the row). Done before layout, so there's no visible shrink.
func _choose_font_size(specs: Array) -> void:
	if _scroll == null or specs.is_empty():
		return

	var available_h := _scroll.size.y
	if available_h <= 0.0:
		return

	var columns := maxi(1, int((_scroll.size.x - SCROLLBAR_ROOM + gap) / (_card_width + gap)))

	for font_size in range(max_font_size, min_font_size - 1, -1):
		if _estimate_height(specs, font_size, columns) <= available_h:
			_font_size = font_size
			return

	_font_size = min_font_size   # still too tall: fall back to scrolling


func _estimate_height(specs: Array, font_size: int, columns: int) -> float:
	var font := get_theme_default_font()
	var text_width := _card_width - _pad_x()
	var total := 0.0
	var row_height := 0.0

	for i in specs.size():
		var spec: Dictionary = specs[i]
		var body := _bbcode_re.sub(str(spec.get("body", "")), "", true)

		var body_h := font.get_multiline_string_size(
			body, HORIZONTAL_ALIGNMENT_LEFT, text_width, font_size
		).y
		var card_h := _pad_y() + font.get_height(font_size) + TITLE_GAP + body_h + HEIGHT_SLACK

		row_height = maxf(row_height, card_h)

		var row_done := (i + 1) % columns == 0 or i == specs.size() - 1
		if row_done:
			total += row_height + (gap if total > 0.0 else 0.0)
			row_height = 0.0

	return total


func _apply_font(slot: Control) -> void:
	var card := _card_of(slot)

	var title: Label = card.get_meta("title")
	title.add_theme_font_size_override("font_size", _font_size)

	var body: RichTextLabel = card.get_meta("body")
	body.add_theme_font_size_override("normal_font_size", _font_size)
	body.add_theme_font_size_override("bold_font_size", _font_size)


func _apply_width(slot: Control) -> void:
	var card := _card_of(slot)
	var body: RichTextLabel = card.get_meta("body")

	## Fixing the text width lets the RichTextLabel compute its wrapped height.
	body.custom_minimum_size.x = _card_width - _pad_x()
	card.custom_minimum_size.x = _card_width
	_sync_slot(slot)


func _sync_slot(slot: Control) -> void:
	if not is_instance_valid(slot) or not slot.has_meta("card"):
		return

	var card := _card_of(slot)
	if card.get_parent() != slot:
		return   # card was handed to the overlay to fade out

	var min_size := card.get_combined_minimum_size()
	min_size.x = _card_width

	card.size = min_size
	card.pivot_offset = min_size * 0.5
	slot.custom_minimum_size = min_size


# ── Helpers ──────────────────────────────────────────────────────────────────

func _card_of(slot: Control) -> PanelContainer:
	return slot.get_meta("card")


func _kill_tween(node: Node, key: String) -> void:
	if node.has_meta(key):
		var tween: Tween = node.get_meta(key)
		if tween != null and tween.is_valid():
			tween.kill()
		node.remove_meta(key)
