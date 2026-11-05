extends SubViewportContainer
class_name RooftopCity3D

## Rooftop-theme backdrop: a real 3D, overexposed white coastal city seen from
## out over the water, with the camera panning slowly left along an endless,
## natural-looking coastline. Built entirely in code, no .tscn.
##
## - Coastline: z = coast(x), a sum of harmonics whose frequencies are whole
##   multiples of 1/TILE_LENGTH, so it repeats exactly every tile and the seam
##   is invisible. Random per launch:
##     * shape     - a few strong low harmonics (bays, headlands)
##     * jaggedness - many higher harmonics with a random amplitude and falloff
##     * elongation - x is warped by a periodic function first, so some
##                    stretches of coast are drawn out and others bunched up
## - Land: thin columns from the coast inland, with rotated quay blocks along
##   the waterline so the edge follows the curve smoothly.
## - Buildings: one MultiMesh of unit boxes laid out by distance inland; low
##   near the water, taller and with occasional towers further back. White
##   albedo; the blue on shaded faces comes from a strong cool ambient light.
## - Water: the GL Compatibility renderer has no SSR, so the reflection is the
##   city mirrored below the water line, seen through a translucent pale-blue
##   plane. Works on the web build too.
## - Haze: depth fog the colour of the sky washes the far city out to white.
## - Endless pan: the tile is drawn four times side by side. The camera moves
##   through one tile and wraps back by exactly TILE_LENGTH, where the view is
##   identical, so the loop never jumps.

const TILE_LENGTH := 1200.0     # metres of coast per repeating tile
const CITY_DEPTH := 420.0       # how far the city extends inland
const LAND_STEP := 4.0          # width of each land column
const QUAY_STEP := 6.0          # length of each waterline quay block
const LAND_HEIGHT := 1.4
const WATER_Y := 0.0
const CAMERA_OFFSHORE := 170.0  # camera distance from the mean coastline

@export var pan_speed := 7.0       # metres per second, moving left (-x)
@export var sway_degrees := 3.0    # gentle look left/right
@export var sway_period := 46.0    # seconds per sway cycle
@export var random_seed := 0       # 0 = new coastline every launch

var _viewport: SubViewport
var _camera: Camera3D
var _pan_t := 0.0    # wraps every tile: seamless by construction
var _sway_t := 0.0   # wraps every 17 sway periods (whole cycles of both sways)

## Coastline parameters (see _coast_z).
var _harmonics: Array[Vector3] = []   # (k, amplitude, phase)
var _warps: Array[Vector3] = []       # (m, amplitude, phase)


func _ready() -> void:
	stretch = true
	mouse_filter = Control.MOUSE_FILTER_IGNORE
	set_anchors_and_offsets_preset(Control.PRESET_FULL_RECT)

	var rng := RandomNumberGenerator.new()
	if random_seed == 0:
		rng.randomize()
	else:
		rng.seed = random_seed
	_make_coastline(rng)

	_viewport = SubViewport.new()
	_viewport.own_world_3d = true
	_viewport.msaa_3d = Viewport.MSAA_2X
	_viewport.render_target_update_mode = SubViewport.UPDATE_WHEN_PARENT_VISIBLE
	add_child(_viewport)

	var world := Node3D.new()
	_viewport.add_child(world)
	_build_environment(world)
	_build_city(world, rng)
	_build_water(world)

	_camera = Camera3D.new()
	_camera.fov = 58.0
	_camera.near = 0.5
	_camera.far = 1600.0
	world.add_child(_camera)
	_update_camera()


func _process(delta: float) -> void:
	if not is_visible_in_tree():
		return
	## Each clock wraps at a point where its motion is identical, so the
	## floats never grow and nothing jumps.
	_pan_t = fmod(_pan_t + delta, TILE_LENGTH / pan_speed)
	_sway_t = fmod(_sway_t + delta, sway_period * 17.0)
	_update_camera()


func _update_camera() -> void:
	var x := -_pan_t * pan_speed
	var yaw := sin(_sway_t * TAU / sway_period) * deg_to_rad(sway_degrees)
	var bob := sin(_sway_t * TAU / (sway_period * 1.7)) * 1.5
	_camera.position = Vector3(x, 36.0 + bob, CAMERA_OFFSHORE)
	_camera.rotation = Vector3(deg_to_rad(-6.5), yaw, 0.0)


# ── Coastline ──────────────────────────────────────────────────────────────

func _make_coastline(rng: RandomNumberGenerator) -> void:
	## Shape: a few big, slow features (bays and headlands).
	for k in range(1, 5):
		_harmonics.append(Vector3(k, rng.randf_range(14.0, 48.0) / sqrt(k), rng.randf() * TAU))

	## Jaggedness: random overall roughness and random falloff, so some
	## coastlines are smooth and others ragged.
	var rough := rng.randf_range(6.0, 16.0)
	var falloff := rng.randf_range(0.9, 1.5)
	for k in range(5, 48):
		var amp := rough * rng.randf() * pow(float(k) / 5.0, -falloff)
		_harmonics.append(Vector3(k, amp, rng.randf() * TAU))

	## Elongation: warp x with whole-number frequencies (stays periodic). The
	## amplitudes keep the warp monotonic: sum of (2*pi*m*A / L) < 1.
	var budget := 0.85
	for m: int in [1, 2, 3]:
		var share := rng.randf_range(0.15, 0.45) * budget
		budget -= share
		var amp := share * TILE_LENGTH / (TAU * m)
		_warps.append(Vector3(m, amp, rng.randf() * TAU))


## Shoreline depth (z) at x. Land is at z < coast(x); the camera is at z > 0.
func _coast_z(x: float) -> float:
	var u := x
	for w in _warps:
		u += w.y * sin(TAU * w.x * x / TILE_LENGTH + w.z)
	var z := 0.0
	for h in _harmonics:
		z += h.y * sin(TAU * h.x * u / TILE_LENGTH + h.z)
	return z


# ── Scene ──────────────────────────────────────────────────────────────────

func _build_environment(world: Node3D) -> void:
	var sky_mat := ProceduralSkyMaterial.new()
	sky_mat.sky_top_color = Color(0.84, 0.91, 0.97)
	sky_mat.sky_horizon_color = Color(0.97, 0.985, 1.0)
	sky_mat.sky_curve = 0.12
	sky_mat.ground_horizon_color = Color(0.97, 0.985, 1.0)
	sky_mat.ground_bottom_color = Color(0.80, 0.88, 0.96)
	sky_mat.sun_angle_max = 0.0   # no visible sun disc

	var sky := Sky.new()
	sky.sky_material = sky_mat

	var env := Environment.new()
	env.background_mode = Environment.BG_SKY
	env.sky = sky
	env.ambient_light_source = Environment.AMBIENT_SOURCE_COLOR
	env.ambient_light_color = Color(0.36, 0.56, 0.90)
	env.ambient_light_energy = 0.9
	env.fog_enabled = true
	env.fog_light_color = Color(0.95, 0.975, 1.0)
	env.fog_density = 0.0026
	env.fog_sky_affect = 0.0

	var we := WorldEnvironment.new()
	we.environment = env
	world.add_child(we)

	## White key light from high and to the right, so the camera-facing and
	## left faces fall into the blue ambient like the reference.
	var sun := DirectionalLight3D.new()
	sun.light_color = Color(1.0, 0.99, 0.97)
	sun.light_energy = 1.25
	sun.rotation_degrees = Vector3(-50.0, 130.0, 0.0)
	world.add_child(sun)


func _box(center: Vector3, size: Vector3, yaw: float = 0.0) -> Transform3D:
	return Transform3D(Basis(Vector3.UP, yaw) * Basis.from_scale(size), center)


## One tile of coast: land, quays and buildings, as transformed unit boxes.
func _generate_tile(rng: RandomNumberGenerator) -> Array[Transform3D]:
	var boxes: Array[Transform3D] = []

	## Land: columns from the shoreline inland.
	var x := 0.0
	while x < TILE_LENGTH:
		var cx := x + LAND_STEP * 0.5
		var shore := _coast_z(cx)
		var depth := CITY_DEPTH + 60.0
		boxes.append(_box(
			Vector3(cx, LAND_HEIGHT * 0.5, shore - depth * 0.5),
			Vector3(LAND_STEP + 0.05, LAND_HEIGHT, depth)))
		x += LAND_STEP

	## Quays: short blocks laid along the curve, rotated to its tangent, so
	## the waterline reads smooth instead of stepped.
	x = 0.0
	while x < TILE_LENGTH:
		var x0 := x
		var x1 := x + QUAY_STEP
		var a := Vector2(x0, _coast_z(x0))
		var b := Vector2(x1, _coast_z(x1))
		var mid := (a + b) * 0.5
		var seg := b - a
		var yaw := -atan2(seg.y, seg.x)
		var h := LAND_HEIGHT + 0.9
		boxes.append(_box(Vector3(mid.x, h * 0.5, mid.y - 2.5), Vector3(seg.length() + 0.6, h, 6.0), yaw))
		x = x1

	## Buildings: jittered cells laid out by distance inland from the shore.
	var bx := 0.0
	while bx < TILE_LENGTH:
		var cell_x := rng.randf_range(18.0, 34.0)
		var cx := bx + cell_x * 0.5
		var shore := _coast_z(cx)
		var d := 14.0
		while d < CITY_DEPTH:
			var cell_d := rng.randf_range(18.0, 34.0)
			if rng.randf() > 0.06:
				var w := cell_x * rng.randf_range(0.6, 0.92)
				var dd := cell_d * rng.randf_range(0.6, 0.92)
				var inland := d / CITY_DEPTH
				var h := rng.randf_range(6.0, 18.0) + inland * 34.0 * pow(rng.randf(), 1.4)
				if rng.randf() < 0.015 + 0.025 * inland:
					h = rng.randf_range(60.0, 130.0)   # landmark tower
					w *= 0.85
					dd *= 0.85
				var pos := Vector3(cx + rng.randf_range(-2.0, 2.0), h * 0.5, shore - d - cell_d * 0.5)
				boxes.append(_box(pos, Vector3(w, h, dd)))
				## Sometimes a setback block on top for a stepped silhouette.
				if h > 18.0 and rng.randf() < 0.35:
					var h2 := h * rng.randf_range(0.2, 0.5)
					boxes.append(_box(Vector3(pos.x, h + h2 * 0.5, pos.z), Vector3(w * 0.6, h2, dd * 0.6)))
			d += cell_d
		bx += cell_x
	return boxes


func _build_city(world: Node3D, rng: RandomNumberGenerator) -> void:
	var boxes := _generate_tile(rng)

	var mm := MultiMesh.new()
	mm.transform_format = MultiMesh.TRANSFORM_3D
	mm.mesh = BoxMesh.new()
	mm.instance_count = boxes.size()
	for i in boxes.size():
		mm.set_instance_transform(i, boxes[i])

	var mat := StandardMaterial3D.new()
	mat.albedo_color = Color(0.97, 0.98, 1.0)
	mat.roughness = 1.0
	mat.specular_mode = BaseMaterial3D.SPECULAR_DISABLED

	## Mirrored copy for the reflection: flipped winding, so cull the other side.
	var mirror_mat := mat.duplicate() as StandardMaterial3D
	mirror_mat.cull_mode = BaseMaterial3D.CULL_FRONT
	var mirror := Transform3D(Basis.from_scale(Vector3(1, -1, 1)), Vector3(0, 2.0 * WATER_Y, 0))

	## Four tiles side by side. The camera pans through x in [-L, 0]; tiles at
	## [-2L, 2L] cover its whole view (fog hides anything beyond).
	for k: int in [-2, -1, 0, 1]:
		var offset := Transform3D(Basis.IDENTITY, Vector3(TILE_LENGTH * k, 0, 0))

		var city := MultiMeshInstance3D.new()
		city.multimesh = mm
		city.material_override = mat
		city.transform = offset
		world.add_child(city)

		var reflection := MultiMeshInstance3D.new()
		reflection.multimesh = mm
		reflection.material_override = mirror_mat
		reflection.transform = offset * mirror
		world.add_child(reflection)


func _build_water(world: Node3D) -> void:
	var plane := PlaneMesh.new()
	plane.size = Vector2(TILE_LENGTH * 4.0, CITY_DEPTH + CAMERA_OFFSHORE + 900.0)

	var mat := StandardMaterial3D.new()
	mat.transparency = BaseMaterial3D.TRANSPARENCY_ALPHA
	mat.shading_mode = BaseMaterial3D.SHADING_MODE_UNSHADED
	mat.albedo_color = Color(0.86, 0.92, 0.99, 0.5)

	var water := MeshInstance3D.new()
	water.mesh = plane
	water.material_override = mat
	water.position = Vector3(0, WATER_Y + 0.02, CAMERA_OFFSHORE + 300.0 - plane.size.y * 0.5)
	world.add_child(water)
