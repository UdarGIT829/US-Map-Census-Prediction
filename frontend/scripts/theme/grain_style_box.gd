extends StyleBox
class_name GrainStyleBox

## Near-black surface filled with animated static, plus an optional solid bar
## on its left edge. Used by the Rooftop theme.
##
## The static comes from ThemeManager.noise_texture (a live, shader-driven
## texture). Each surface samples its own region of it, picked by hashing the
## canvas item it draws into, so separate controls never show identical static
## even when they share this StyleBox through the theme. Everything drawn into
## one canvas item (e.g. a whole open dropdown list) shares one continuous
## region.
##
## The edge bar is drawn as its own rectangle, never baked into the noise, so
## texture filtering can't bleed its color into the static.

@export var edge_color: Color = Color(0, 0, 0, 0)
@export var edge_width: float = 0.0
@export var brightness: float = 1.0   # >1 lightens the static (hover states)

const FALLBACK := Color(0.105, 0.110, 0.122, 0.97)


func _draw(to_canvas_item: RID, rect: Rect2) -> void:
	var tex: Texture2D = ThemeManager.noise_texture

	if tex == null:
		RenderingServer.canvas_item_add_rect(to_canvas_item, rect, FALLBACK)
	else:
		## One noise texel per *screen* pixel: the UI is drawn in canvas units
		## that the window stretch scales, so convert using that scale. Keeps
		## the grain fine, square and the same size at any window size.
		var tex_size := tex.get_size()
		var screen_size := rect.size * ThemeManager.ui_scale
		var src_size := Vector2(minf(screen_size.x, tex_size.x), minf(screen_size.y, tex_size.y))

		## Stable per-surface offset from the canvas item's id.
		var h := _hash(to_canvas_item.get_id())
		var offset := Vector2(
			floorf(h.x * maxf(0.0, tex_size.x - src_size.x)),
			floorf(h.y * maxf(0.0, tex_size.y - src_size.y))
		)

		RenderingServer.canvas_item_add_texture_rect_region(
			to_canvas_item, rect, tex.get_rid(), Rect2(offset, src_size),
			Color(brightness, brightness, brightness, 0.97)
		)

	if edge_width > 0.0 and edge_color.a > 0.0:
		RenderingServer.canvas_item_add_rect(
			to_canvas_item,
			Rect2(rect.position, Vector2(edge_width, rect.size.y)),
			edge_color
		)


## Two well-mixed values in [0, 1) from an integer id.
static func _hash(id: int) -> Vector2:
	var a := (id * 2654435761 + 12345) & 0x7fffffff
	var b := (id * 40503 + 2246822519) & 0x7fffffff
	a = (a ^ (a >> 15)) * 2246822519 & 0x7fffffff
	b = (b ^ (b >> 13)) * 3266489917 & 0x7fffffff
	return Vector2(float(a % 100003) / 100003.0, float(b % 100019) / 100019.0)
