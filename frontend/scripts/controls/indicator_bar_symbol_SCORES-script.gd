extends Control

@export var topx_value: float:
	set(value):
		topx_value = value
		_updateTooltip()
		
@onready var tooltipContainer: CenterContainer= $TooltipContainer
@onready var TOPX_value_label: RichTextLabel= $"TooltipContainer/HBoxContainer/VBoxContainer2/AVG-Value"

func _updateTooltip() -> void:
	TOPX_value_label.text = str(topx_value)

func _on_mouse_entered() -> void:
	tooltipContainer.show()


func _on_mouse_exited() -> void:
	tooltipContainer.hide()
