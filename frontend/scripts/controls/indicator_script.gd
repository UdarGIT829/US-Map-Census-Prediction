extends HBoxContainer

@export var top_results_size:int
@export var standard_deviation_value: float:
	set(value):
		standard_deviation_value = value
		update_stddev()
@export var average_value: float:
	set(value):
		average_value = value
		update_average()
@export var q1_value: float
@export var q3_value: float

@onready var indicator_STDDEV :Control= $"Indicator-BarSymbol-STDDEV"

func update_stddev():
	indicator_STDDEV.stddev_value = standard_deviation_value
	return
	
func update_average():
	indicator_STDDEV.average_value = average_value
	return
