shader_type canvas_item;

// Rooftop-theme backdrop: an overexposed, pale-blue city skyline built from
// procedural blocks in three parallax layers (each with a shaded side face)
// and white haze toward the horizon.
// Drawn on a full-screen ColorRect behind the UI (see ThemeManager).

uniform vec4 sky_top : source_color = vec4(0.96, 0.98, 1.00, 1.0);
uniform vec4 sky_horizon : source_color = vec4(0.80, 0.89, 0.98, 1.0);
uniform vec4 far_color : source_color = vec4(0.80, 0.88, 0.97, 1.0);
uniform vec4 mid_color : source_color = vec4(0.60, 0.75, 0.93, 1.0);
uniform vec4 near_color : source_color = vec4(0.42, 0.61, 0.88, 1.0);
uniform float drift_speed = 0.006;
uniform vec2 aspect = vec2(1.778, 1.0);   // width / height, set from the node size

float hash(float n) {
	return fract(sin(n * 127.1 + 311.7) * 43758.5453);
}

// One row of buildings. Returns coverage (0/1) and writes a shade factor
// (darker on each building's left face, brighter toward its top).
float skyline(vec2 uv, float scale, float seed, float base_h, float var_h, float speed, out float shade) {
	float x = uv.x * aspect.x * scale + TIME * speed + seed * 17.0;
	float id = floor(x);
	float f = fract(x);

	float h = base_h + var_h * pow(hash(id + seed), 1.6);
	float gap = step(0.82, hash(id * 3.7 + seed));          // some empty lots
	float margin = 0.04 + 0.10 * hash(id * 1.9 + seed);

	float in_x = step(margin, f) * step(f, 1.0 - margin);
	float in_y = step(1.0 - h, uv.y);
	float covered = in_x * in_y * (1.0 - gap);

	// Left ~35% of the building is the shaded side face.
	float face_split = margin + 0.35 * (1.0 - 2.0 * margin);
	float side = step(f, face_split);
	float height_t = clamp((uv.y - (1.0 - h)) / max(h, 0.001), 0.0, 1.0);
	shade = mix(1.0, 0.86, side) * mix(1.06, 0.94, height_t);

	return covered;
}

void fragment() {
	vec2 uv = UV;

	vec3 col = mix(sky_top.rgb, sky_horizon.rgb, smoothstep(0.0, 0.75, uv.y));

	float shade;
	float far = skyline(uv, 9.0, 1.0, 0.30, 0.35, drift_speed * 0.4, shade);
	col = mix(col, far_color.rgb * shade, far);

	float mid = skyline(uv, 6.0, 7.0, 0.18, 0.42, drift_speed * 0.7, shade);
	col = mix(col, mid_color.rgb * shade, mid);

	float near = skyline(uv, 3.5, 13.0, 0.08, 0.30, drift_speed, shade);
	col = mix(col, near_color.rgb * shade, near);

	// Overexposed haze: bloom toward white around the horizon line.
	float haze = exp(-pow((uv.y - 0.62) * 3.2, 2.0));
	col = mix(col, vec3(1.0), haze * 0.35);

	COLOR = vec4(col, 1.0);
}
