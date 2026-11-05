shader_type canvas_item;

// Arcade-theme backdrop: deep navy space, a barrel-warped perspective grid that
// slowly drifts, wandering ribbons of colored light, and a vignette.
// Drawn on a full-screen ColorRect behind the UI (see ThemeManager).

uniform vec4 top_color : source_color = vec4(0.020, 0.024, 0.090, 1.0);
uniform vec4 bottom_color : source_color = vec4(0.045, 0.055, 0.180, 1.0);
uniform vec4 grid_color : source_color = vec4(0.42, 0.50, 0.95, 1.0);
uniform float grid_cells = 22.0;
uniform float grid_strength = 0.30;
uniform float warp = 0.55;
uniform float drift_speed = 0.015;
uniform float ribbon_strength = 0.55;
uniform vec2 aspect = vec2(1.778, 1.0);   // width / height, set from the node size

float grid_line(vec2 p, float width) {
	vec2 g = abs(fract(p - 0.5) - 0.5) / fwidth(p);
	float line = min(g.x, g.y);
	return 1.0 - smoothstep(0.0, width, line);
}

// A soft glowing curve: distance from uv.y to a slowly moving sine path.
float ribbon(vec2 uv, float freq, float phase, float speed, float amp, float center, float thickness) {
	float y = center + amp * sin(uv.x * freq + phase + TIME * speed)
		+ amp * 0.35 * sin(uv.x * freq * 2.3 - TIME * speed * 1.7 + phase);
	float d = abs(uv.y - y);
	return exp(-d * d / (thickness * thickness));
}

void fragment() {
	vec2 uv = UV;

	// Base gradient.
	vec3 col = mix(top_color.rgb, bottom_color.rgb, uv.y);

	// Barrel-warped grid centered slightly right of middle, like a curved screen.
	vec2 p = (uv - vec2(0.56, 0.48)) * aspect;
	float r2 = dot(p, p);
	vec2 warped = p * (1.0 + warp * r2);
	warped += vec2(TIME * drift_speed, TIME * drift_speed * 0.4);

	float lines = grid_line(warped * grid_cells / aspect.y, 1.2);
	float grid_fade = smoothstep(1.6, 0.15, r2);   // brightest near the bulge
	col += grid_color.rgb * lines * grid_strength * (0.35 + 0.65 * grid_fade);

	// Ribbons of light (red, blue-violet, green), additive.
	col += vec3(0.95, 0.18, 0.20) * ribbon(uv, 2.6, 0.0, 0.22, 0.16, 0.36, 0.035) * ribbon_strength;
	col += vec3(0.45, 0.35, 1.00) * ribbon(uv, 1.7, 2.1, -0.17, 0.22, 0.62, 0.05) * ribbon_strength * 0.8;
	col += vec3(0.20, 0.85, 0.45) * ribbon(uv, 3.4, 4.2, 0.12, 0.10, 0.80, 0.018) * ribbon_strength * 0.6;

	// Big soft red glow in the upper right, as in the inspiration.
	float glow = exp(-length((uv - vec2(0.82, 0.12)) * aspect) * 2.2);
	col += vec3(0.55, 0.06, 0.10) * glow * 0.55;

	// Vignette.
	float vig = smoothstep(1.25, 0.25, length((uv - 0.5) * aspect * 0.9));
	col *= mix(0.45, 1.0, vig);

	COLOR = vec4(col, 1.0);
}
