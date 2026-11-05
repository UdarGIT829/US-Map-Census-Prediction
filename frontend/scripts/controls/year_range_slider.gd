extends Control
class_name YearRangeSlider

## Horizontal year timeline with two handles that select a contiguous range.
##
## Replaces the multi-select ItemList on the Dataset tab, which grew taller with
## every year and pushed the layout down. Emits the selected years (inclusive)
## whenever a handle moves.

signal range_changed(years: Array)

@export var side_margin: float = 16.0
@export var track_y: float = 16.0
@export var label_y: float = 42.0
@export var px_per_year: float = 40.0
@export var min_width: float = 220.0

@export var track_color := Color(1, 1, 1, 0.16)
@export var range_color := Color("#6EA8FE")
@export var label_color := Color(0.92, 0.94, 0.98, 1.0)
@export var muted_label_color := Color(0.55, 0.59, 0.67, 1.0)

const HANDLE_RADIUS := 7.0
const HANDLE_HIT_RADIUS := 14.0
const LABEL_FONT_SIZE := 11

var _years: Array[int] = []
var _lo: int = 0
var _hi: int = 0
var _dragging: int = -1   # 0 = start handle, 1 = end handle
var _hovered: int = -1


var _default_range_color: Color


func _ready() -> void:
	mouse_filter = Control.MOUSE_FILTER_STOP
	mouse_default_cursor_shape = Control.CURSOR_POINTING_HAND
	custom_minimum_size = Vector2(min_width, 52)

	_default_range_color = range_color
	ThemeManager.theme_changed.connect(_on_theme_changed)


func _on_theme_changed(theme_name: String) -> void:
	match theme_name:
		"arcade":
			range_color = ArcadeTheme.GOLD
		"rooftop":
			range_color = RooftopTheme.RED
		_:
			range_color = _default_range_color
	queue_redraw()


## Replace the available years; the whole range starts selected.
func set_years(years: Array) -> void:
	_years.clear()
	for y in years:
		_years.append(int(y))
	_years.sort()

	_lo = 0
	_hi = max(0, _years.size() - 1)
	_dragging = -1
	_hovered = -1

	custom_minimum_size = Vector2(max(min_width, side_margin * 2.0 + px_per_year * _years.size()), 52)
	tooltip_text = _range_text()
	queue_redraw()


func get_selected_years() -> Array[int]:
	if _years.is_empty():
		return []
	return _years.slice(_lo, _hi + 1)


# ── Drawing ──────────────────────────────────────────────────────────────────

func _draw() -> void:
	var font := get_theme_default_font()

	if _years.is_empty():
		draw_line(Vector2(side_margin, track_y), Vector2(size.x - side_margin, track_y), track_color, 3.0)
		draw_string(font, Vector2(side_margin, label_y), "Pick a loader", HORIZONTAL_ALIGNMENT_LEFT, -1, LABEL_FONT_SIZE, muted_label_color)
		return

	var x0 := _x_for(0)
	var x1 := _x_for(_years.size() - 1)

	draw_line(Vector2(x0, track_y), Vector2(x1, track_y), track_color, 3.0, true)
	draw_line(Vector2(_x_for(_lo), track_y), Vector2(_x_for(_hi), track_y), range_color, 4.0, true)

	var label_step := _label_step(font)

	for i in _years.size():
		var x := _x_for(i)
		var in_range := i >= _lo and i <= _hi

		draw_line(Vector2(x, track_y - 5), Vector2(x, track_y + 5), range_color if in_range else track_color, 1.5)

		## Always label the ends and both handles; thin out the rest if crowded.
		if i % label_step == 0 or i == _lo or i == _hi or i == _years.size() - 1:
			var text := str(_years[i])
			var w := font.get_string_size(text, HORIZONTAL_ALIGNMENT_LEFT, -1, LABEL_FONT_SIZE).x
			draw_string(
				font,
				Vector2(x - w * 0.5, label_y),
				text,
				HORIZONTAL_ALIGNMENT_LEFT,
				-1,
				LABEL_FONT_SIZE,
				label_color if in_range else muted_label_color
			)

	for h in [0, 1]:
		var idx := _lo if h == 0 else _hi
		var r := HANDLE_RADIUS + (2.0 if h == _hovered or h == _dragging else 0.0)
		var center := Vector2(_x_for(idx), track_y)
		draw_circle(center, r, Color(0.96, 0.97, 1.0))
		draw_arc(center, r, 0.0, TAU, 24, range_color, 2.0, true)


func _label_step(font: Font) -> int:
	var label_w := font.get_string_size("2000", HORIZONTAL_ALIGNMENT_LEFT, -1, LABEL_FONT_SIZE).x + 6.0
	var spacing := (_x_for(1) - _x_for(0)) if _years.size() > 1 else INF
	return max(1, int(ceil(label_w / spacing)))


func _x_for(index: int) -> float:
	if _years.size() <= 1:
		return size.x * 0.5
	var t := float(index) / float(_years.size() - 1)
	return side_margin + t * (size.x - side_margin * 2.0)


func _index_at(x: float) -> int:
	if _years.size() <= 1:
		return 0
	var t := (x - side_margin) / maxf(1.0, size.x - side_margin * 2.0)
	return clampi(roundi(t * (_years.size() - 1)), 0, _years.size() - 1)


func _range_text() -> String:
	if _years.is_empty():
		return ""
	if _lo == _hi:
		return str(_years[_lo])
	return "%d–%d" % [_years[_lo], _years[_hi]]


# ── Input ────────────────────────────────────────────────────────────────────

func _gui_input(event: InputEvent) -> void:
	if _years.is_empty():
		return

	if event is InputEventMouseButton and event.button_index == MOUSE_BUTTON_LEFT:
		if event.pressed:
			_dragging = _pick_handle(event.position.x)
			_move_handle(_dragging, _index_at(event.position.x))
		else:
			_dragging = -1
			queue_redraw()
		accept_event()

	elif event is InputEventMouseMotion:
		if _dragging != -1:
			_move_handle(_dragging, _index_at(event.position.x))
			accept_event()
		else:
			var hovered := _handle_near(event.position)
			if hovered != _hovered:
				_hovered = hovered
				queue_redraw()


func _notification(what: int) -> void:
	if what == NOTIFICATION_MOUSE_EXIT and _hovered != -1:
		_hovered = -1
		queue_redraw()
	elif what == NOTIFICATION_RESIZED:
		queue_redraw()


## Clicking anywhere on the track moves whichever handle is closer.
func _pick_handle(x: float) -> int:
	var d_lo := absf(x - _x_for(_lo))
	var d_hi := absf(x - _x_for(_hi))

	if is_equal_approx(d_lo, d_hi):
		## Handles stacked on one year: clicking right of them grabs the end.
		return 1 if x > _x_for(_hi) else 0

	return 0 if d_lo < d_hi else 1


func _handle_near(pos: Vector2) -> int:
	for h in [1, 0]:
		var idx := _lo if h == 0 else _hi
		if pos.distance_to(Vector2(_x_for(idx), track_y)) <= HANDLE_HIT_RADIUS:
			return h
	return -1


func _move_handle(handle: int, index: int) -> void:
	var lo := _lo
	var hi := _hi

	if handle == 0:
		lo = clampi(index, 0, _hi)
	else:
		hi = clampi(index, _lo, _years.size() - 1)

	if lo == _lo and hi == _hi:
		return

	_lo = lo
	_hi = hi
	tooltip_text = _range_text()
	queue_redraw()
	range_changed.emit(get_selected_years())
