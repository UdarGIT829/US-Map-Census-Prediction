extends Node

## Autoload "ThemeManager": switches between the visual themes.
##
##   "default"  Godot's stock look + round checkboxes
##   "arcade"   dark navy, glowing grid shader, slanted gold bars (ArcadeTheme)
##   "rooftop"  3D white city on a river (RooftopCity3D), dark static surfaces, red blocks (RooftopTheme)
##
## Applies the Theme to the scene root, swaps the animated shader backdrop,
## gives the main tab panel its "MainFrame" style and restyles the pieces that
## sit outside the panel (window title, DEMO MODE checkbox). Custom-drawn
## pieces (explanation cards, chart tooltips, year slider, ML header) listen to
## `theme_changed` and restyle themselves.

signal theme_changed(theme_name: String)

const THEMES := ["default", "arcade", "rooftop"]
const LABELS := {"default": "Default", "arcade": "Arcade", "rooftop": "Rooftop"}

const SHADERS := {
	"arcade": preload("res://shaders/arcade_background.gdshader"),
}

const MAIN_TABS_PATH := "Main/TabContainer"
const TITLE_PATH := "RichTextLabel"
const DEMO_CHECKBOX_PATH := "DemoModeCheckBox"
const THEME_TOGGLE_PATH := "ThemeToggle"

const NOISE_SHADER := preload("res://shaders/static_noise.gdshader")
const NOISE_SIZE := 1024   # big enough that large cards map 1 texel = 1 screen px

var current: String = "rooftop"   # the project's default look

## Live animated static (GrainStyleBox samples regions of it). Rendered only
## while a theme that uses it is active.
var noise_texture: Texture2D

## Canvas units -> screen pixels (from the window's canvas_items stretch).
var ui_scale: float = 1.0

var _themes: Dictionary = {}       # name -> Theme (built lazily)
var _materials: Dictionary = {}    # name -> ShaderMaterial
var _background: ColorRect
var _city_3d: RooftopCity3D      # rooftop's 3D backdrop (built on first use)
var _noise_viewport: SubViewport
var _noise_material: ShaderMaterial

## Static clock in seconds, wrapped so the shader never sees a huge float.
var _noise_clock: float = 0.0
const NOISE_CLOCK_WRAP := 600.0


func _ready() -> void:
	_create_noise_source()

	get_tree().root.size_changed.connect(_update_ui_scale)
	_update_ui_scale.call_deferred()

	## Optional start theme from the command line (handy for screenshots):
	##   godot --path . ++ --theme=rooftop
	for arg in OS.get_cmdline_user_args():
		if arg.begins_with("--theme="):
			var requested := arg.trim_prefix("--theme=")
			if THEMES.has(requested):
				current = requested

	## The default look also needs a (small) theme, for the round checkboxes.
	## Deferred so the main scene exists.
	set_theme_by_name.call_deferred(current)


## Grain size depends on this, so redraw themed controls when it changes.
func _update_ui_scale() -> void:
	var scale := get_tree().root.get_final_transform().get_scale().x
	if scale <= 0.0 or is_equal_approx(scale, ui_scale):
		return
	ui_scale = scale

	var root := get_tree().current_scene
	if root != null and current == "rooftop":
		_redraw_all(root)


func _redraw_all(node: Node) -> void:
	if node is CanvasItem:
		(node as CanvasItem).queue_redraw()
	for child in node.get_children():
		_redraw_all(child)


## Off-screen viewport that renders the static shader into a texture.
func _create_noise_source() -> void:
	_noise_viewport = SubViewport.new()
	_noise_viewport.size = Vector2i(NOISE_SIZE, NOISE_SIZE)
	_noise_viewport.transparent_bg = false
	_noise_viewport.disable_3d = true
	_noise_viewport.render_target_update_mode = SubViewport.UPDATE_DISABLED

	var rect := ColorRect.new()
	rect.size = Vector2(NOISE_SIZE, NOISE_SIZE)
	var mat := ShaderMaterial.new()
	mat.shader = NOISE_SHADER
	mat.set_shader_parameter("resolution", float(NOISE_SIZE))
	rect.material = mat
	_noise_material = mat

	_noise_viewport.add_child(rect)
	add_child(_noise_viewport)
	noise_texture = _noise_viewport.get_texture()


func _process(delta: float) -> void:
	if _noise_viewport.render_target_update_mode == SubViewport.UPDATE_DISABLED:
		return
	_noise_clock = fmod(_noise_clock + delta, NOISE_CLOCK_WRAP)
	_noise_material.set_shader_parameter("clock", _noise_clock)


func is_theme(theme_name: String) -> bool:
	return current == theme_name


func set_theme_by_name(theme_name: String) -> void:
	if not THEMES.has(theme_name):
		push_warning("ThemeManager: unknown theme " + theme_name)
		return

	var root := get_tree().current_scene as Control
	if root == null:
		return

	current = theme_name
	root.theme = _get_theme(theme_name)

	## Only spend frames on the static while a theme uses it.
	_noise_viewport.render_target_update_mode = (
		SubViewport.UPDATE_ALWAYS if theme_name == "rooftop" else SubViewport.UPDATE_DISABLED
	)

	if SHADERS.has(theme_name):
		_ensure_background(root)
		_background.material = _get_material(theme_name)
		_update_aspect()
		_background.show()
	elif _background != null:
		_background.hide()

	## Rooftop's backdrop is a real 3D scene rather than a shader.
	if theme_name == "rooftop":
		if _city_3d == null or not is_instance_valid(_city_3d):
			_city_3d = RooftopCity3D.new()
			_city_3d.name = "RooftopCity3D"
			root.add_child(_city_3d)
			root.move_child(_city_3d, 0)   # behind everything else
		_city_3d.show()
	elif _city_3d != null and is_instance_valid(_city_3d):
		_city_3d.hide()   # its SubViewport stops rendering while hidden

	_style_extras(root)
	theme_changed.emit(theme_name)


func _get_theme(theme_name: String) -> Theme:
	if not _themes.has(theme_name):
		match theme_name:
			"arcade":
				_themes[theme_name] = ArcadeTheme.build()
			"rooftop":
				_themes[theme_name] = RooftopTheme.build()
			_:
				_themes[theme_name] = ArcadeTheme.build_default()
	return _themes[theme_name]


func _get_material(theme_name: String) -> ShaderMaterial:
	if not _materials.has(theme_name):
		var mat := ShaderMaterial.new()
		mat.shader = SHADERS[theme_name]
		_materials[theme_name] = mat
	return _materials[theme_name]


func _ensure_background(root: Control) -> void:
	if _background != null and is_instance_valid(_background):
		return

	_background = ColorRect.new()
	_background.name = "ThemeBackground"
	_background.set_anchors_and_offsets_preset(Control.PRESET_FULL_RECT)
	_background.mouse_filter = Control.MOUSE_FILTER_IGNORE

	root.add_child(_background)
	root.move_child(_background, 0)   # behind everything else

	_background.resized.connect(_update_aspect)


## Keeps shader geometry (grid cells, buildings) undistorted at any window shape.
func _update_aspect() -> void:
	if _background == null or _background.size.y <= 0.0:
		return
	var mat := _background.material as ShaderMaterial
	if mat != null:
		mat.set_shader_parameter("aspect", Vector2(_background.size.x / _background.size.y, 1.0))


## Pieces outside the themed panel that need per-theme touches.
func _style_extras(root: Control) -> void:
	var tabs := root.get_node_or_null(MAIN_TABS_PATH) as TabContainer
	if tabs != null:
		tabs.theme_type_variation = &"" if current == "default" else &"MainFrame"

	var title := root.get_node_or_null(TITLE_PATH) as RichTextLabel
	if title != null:
		_clear_title(title)
		match current:
			"arcade":
				title.add_theme_font_override("normal_font", ArcadeTheme.heavy_font())
				title.add_theme_font_size_override("normal_font_size", 22)
				title.add_theme_color_override("default_color", Color("#c9cdd8"))
				title.add_theme_color_override("font_shadow_color", Color(0, 0, 0, 0.8))
				title.add_theme_constant_override("shadow_offset_x", 3)
				title.add_theme_constant_override("shadow_offset_y", 3)
			"rooftop":
				## Sits over the bright white sky, so it needs dark text.
				title.add_theme_font_override("normal_font", RooftopTheme.italic_font())
				title.add_theme_font_size_override("normal_font_size", 22)
				title.add_theme_color_override("default_color", RooftopTheme.INK)

	## The two checkboxes sit over the bright sky, so they need dark text.
	for path in [DEMO_CHECKBOX_PATH, THEME_TOGGLE_PATH]:
		var box := root.get_node_or_null(path) as CheckBox
		if box == null:
			continue
		for item in ["font_color", "font_hover_color", "font_pressed_color", "font_hover_pressed_color", "font_disabled_color"]:
			if current == "rooftop":
				box.add_theme_color_override(item, RooftopTheme.INK)
			else:
				box.remove_theme_color_override(item)


func _clear_title(title: RichTextLabel) -> void:
	title.remove_theme_font_override("normal_font")
	title.remove_theme_font_size_override("normal_font_size")
	title.remove_theme_color_override("default_color")
	title.remove_theme_color_override("font_shadow_color")
	title.remove_theme_constant_override("shadow_offset_x")
	title.remove_theme_constant_override("shadow_offset_y")
