extends RefCounted
class_name CircleCheckIcons

## Round checkbox icons drawn in code: an anti-aliased ring, plus a filled dot
## when checked. Used by both the default and Arcade themes.

const SIZE := 20
const RING_WIDTH := 2.0
const DOT_INSET := 5.0


## Sets CheckBox's checked/unchecked (and disabled) icons on `theme`.
static func apply(theme: Theme, ring: Color, dot: Color, disabled: Color) -> void:
	theme.set_icon("unchecked", "CheckBox", make(ring, dot, false))
	theme.set_icon("checked", "CheckBox", make(ring, dot, true))
	theme.set_icon("unchecked_disabled", "CheckBox", make(disabled, disabled, false))
	theme.set_icon("checked_disabled", "CheckBox", make(disabled, disabled, true))


static func make(ring: Color, dot: Color, filled: bool) -> ImageTexture:
	var img := Image.create(SIZE, SIZE, false, Image.FORMAT_RGBA8)
	var center := Vector2(SIZE, SIZE) * 0.5
	var r_outer := SIZE * 0.5 - 1.0
	var r_inner := r_outer - RING_WIDTH
	var r_dot := r_outer - DOT_INSET
	var well := Color(0, 0, 0, 0.35)   # faint dark fill inside the ring

	for y in SIZE:
		for x in SIZE:
			var d := Vector2(x + 0.5, y + 0.5).distance_to(center)

			## Coverage of each shape at this pixel (1px soft edge = anti-aliasing).
			var inside_outer := clampf(r_outer + 0.5 - d, 0.0, 1.0)
			var inside_inner := clampf(r_inner + 0.5 - d, 0.0, 1.0)
			var ring_cov := inside_outer - inside_inner
			var dot_cov := clampf(r_dot + 0.5 - d, 0.0, 1.0) if filled else 0.0

			var c := Color(well.r, well.g, well.b, well.a * inside_inner)
			c = _over(c, Color(dot.r, dot.g, dot.b, dot.a * dot_cov))
			c = _over(c, Color(ring.r, ring.g, ring.b, ring.a * ring_cov))
			img.set_pixel(x, y, c)

	return ImageTexture.create_from_image(img)


## Standard "source over destination" alpha compositing.
static func _over(dst: Color, src: Color) -> Color:
	var a := src.a + dst.a * (1.0 - src.a)
	if a <= 0.0:
		return Color(0, 0, 0, 0)
	var rgb := (Vector3(src.r, src.g, src.b) * src.a + Vector3(dst.r, dst.g, dst.b) * dst.a * (1.0 - src.a)) / a
	return Color(rgb.x, rgb.y, rgb.z, a)
