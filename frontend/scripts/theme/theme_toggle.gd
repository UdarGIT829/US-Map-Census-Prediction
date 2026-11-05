extends CheckBox

## "ROOFTOP THEME" checkbox under DEMO MODE: on = the Rooftop theme (the
## project's default look), off = Godot's stock look. Forwards to ThemeManager.

func _ready() -> void:
	button_pressed = ThemeManager.current == "rooftop"
	toggled.connect(_on_toggled)
	ThemeManager.theme_changed.connect(_on_theme_changed)


func _on_toggled(on: bool) -> void:
	ThemeManager.set_theme_by_name("rooftop" if on else "default")


## Keeps the box in sync if the theme is changed elsewhere (e.g. --theme=).
func _on_theme_changed(theme_name: String) -> void:
	set_pressed_no_signal(theme_name == "rooftop")
