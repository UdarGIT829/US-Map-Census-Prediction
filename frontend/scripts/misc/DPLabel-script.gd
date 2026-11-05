# Parsed structure for a DP label
class_name DPLabel

const DELIMITER = "!!"

static func parse(raw_label: String) -> Dictionary:
	var parts: Array = raw_label.split(DELIMITER)
	return {
		"measure":   parts[0] if parts.size() > 0 else "",   # "Estimate"
		"category":  parts[1] if parts.size() > 1 else "",   # "PLACE OF BIRTH"
		"total":     parts[2] if parts.size() > 2 else "",   # "Total population"
		"group":     parts[3] if parts.size() > 3 else "",   # "Native"
		"subgroup":  parts[4] if parts.size() > 4 else "",   # "Born in United States"
		"detail":    parts[5] if parts.size() > 5 else "",   # "Different state"
		"raw":       raw_label,
	}
