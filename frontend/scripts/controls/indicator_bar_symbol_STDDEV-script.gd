extends Control

@export var average_value: float:
	set(value):
		average_value = value
		_updateTooltip()
@export var stddev_value: float:
	set(value):
		stddev_value = value
		_updateTooltip()
		
@onready var tooltipContainer: CenterContainer= $TooltipContainer
@onready var AVG_value_label: RichTextLabel= $"TooltipContainer/HBoxContainer/VBoxContainer2/AVG-Value"
@onready var STDDEV_value_label: RichTextLabel= $"TooltipContainer/HBoxContainer/VBoxContainer2/STDDEV-Value"

func _updateTooltip() -> void:
	AVG_value_label.text = str(average_value)
	STDDEV_value_label.text = str(stddev_value)

func _on_mouse_entered() -> void:
	tooltipContainer.show()


func _on_mouse_exited() -> void:
	tooltipContainer.hide()
