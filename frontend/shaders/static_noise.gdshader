shader_type canvas_item;

// Animated static as SPARSE DOTS on a flat near-black grey base.
//
// Most pixels are exactly base_color. A small random fraction (density) are
// faint, muted-colour specks. Every pixel re-rolls on its own staggered
// schedule (a per-pixel phase offset), so the field twinkles continuously
// instead of the whole thing flipping at once, which reads as an animated
// texture.
//
// Rendered into an off-screen SubViewport (see ThemeManager); GrainStyleBox
// samples a different region per surface at one texel per screen pixel.
//
// Integer PCG hash: float sin/fract hashes band diagonally. The clock is
// wrapped CPU-side (ThemeManager) instead of using TIME, so it never grows
// large enough to lose float precision.

uniform vec4 base_color : source_color = vec4(0.105, 0.110, 0.122, 1.0);
uniform float density = 0.045;    // fraction of pixels that are a speck
uniform float strength = 0.075;   // speck brightness above base
uniform float chroma = 0.35;      // 0 = grey specks, 1 = fully tinted
uniform float rate = 6.0;         // re-rolls per second, per pixel
uniform float clock = 0.0;        // seconds, wrapped by ThemeManager
uniform float resolution = 1024.0;

uint pcg(uint v) {
	uint state = v * 747796405u + 2891336453u;
	uint word = ((state >> ((state >> 28u) + 4u)) ^ state) * 277803737u;
	return (word >> 22u) ^ word;
}

float rnd(uint seed) {
	return float(pcg(seed) >> 8u) / 16777216.0; // 24 bits -> exact in float
}

void fragment() {
	uvec2 px = uvec2(floor(UV * resolution));
	uint cell = pcg(px.x * 1973u + pcg(px.y * 9277u + 26699u));

	// Per-pixel phase so pixels change at different moments.
	float phase = rnd(cell ^ 0x9E3779B9u);
	uint frame = uint(floor(clock * rate + phase));
	uint seed = pcg(cell + frame * 0x632BE5ABu);

	vec3 col = base_color.rgb;
	if (rnd(seed) < density) {
		float amp = strength * (0.35 + 0.65 * rnd(seed + 1u));
		vec3 hue = vec3(rnd(seed + 2u), rnd(seed + 3u), rnd(seed + 4u));
		hue = mix(vec3(1.0), hue / max(max(hue.r, hue.g), max(hue.b, 1e-3)), chroma);
		col += amp * hue;
	}
	COLOR = vec4(col, 1.0);
}
