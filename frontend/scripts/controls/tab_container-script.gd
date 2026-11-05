extends TabContainer

@export var name_to_idx :Dictionary= {}

func _ready() -> void:
	var counter = 0
	for _child in get_children():
		name_to_idx[_child.name] = counter
		counter += 1
		
	# Block Results on Start
	#print("Blocking results tab on start")
	#block_tab(&"Results")

func block_tab(tab_name:StringName) -> void:
	set_tab_disabled(name_to_idx.get(tab_name), true)
	
func unblock_tab(tab_name:StringName) -> void:
	set_tab_disabled(name_to_idx.get(tab_name), false)
