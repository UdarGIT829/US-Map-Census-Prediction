extends VBoxContainer

@export var resultsDict :Dictionary= {}
@export var resultsButtonsMapping :Array[StringName]= []
# Shape is {
#	key:StringName : [ button_index:StringName ,  ]
#		}
#
@export var selected_results :Array
@export var current_idx :int = 0
signal results_selected

@onready var resultsList:ItemList= $ResultsList

func add_results(results_uid:StringName, annotated_results: Array, family_results:Array, pairwise_scores:Array):	
	# Update Selector Bar
	#resultsList.max_columns += 1
	resultsList.add_item(results_uid, null, true)
	resultsButtonsMapping.append(results_uid)

	# Update internal dict
	resultsDict[results_uid] = [annotated_results, family_results, pairwise_scores]
	


func get_uniqueness_id()->String:
	return str(len(resultsDict))

func select_results_idx(index:int):
	print("SELECTED %s" % str(index-1))
	current_idx = index-1
	var thisKey := resultsButtonsMapping[current_idx]
	selected_results = resultsDict[thisKey]
	results_selected.emit()
	return

func select_last_results():
	current_idx = resultsButtonsMapping.size() - 1
	print("SELECTED %s" % str(current_idx))
	var thisKey := resultsButtonsMapping[current_idx]
	selected_results = resultsDict[thisKey]
	results_selected.emit()
	return

func get_results() -> Array:
	return selected_results
