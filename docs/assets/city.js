// Live 3D backdrop for the project page: the endless white coastal city from
// the app's Rooftop theme, ported from frontend/scripts/theme/rooftop_city_3d.gd.
//
// - Coastline: z = coast(x), a sum of harmonics whose frequencies are whole
//   multiples of 1/TILE, so it repeats exactly every tile (seamless). Random
//   per page load: low harmonics give bays and headlands, random-amplitude
//   high harmonics give jaggedness, and a periodic warp of x elongates some
//   stretches and bunches up others.
// - Land columns, quay blocks angled along the curve, and buildings by
//   distance inland, all as one InstancedMesh of unit boxes, drawn for four
//   tiles side by side.
// - Reflection: the same instances mirrored below the water line (a second
//   mesh with scale.y = -1 sharing the instance buffer), seen through a
//   translucent pale-blue water plane.
// - The canvas is fixed behind the page. The camera pans left continuously;
//   scrolling the page raises and tilts it.

import * as THREE from "./vendor/three.module.min.js";

const TILE = 1200;            // metres of coast per repeating tile
const CITY_DEPTH = 420;
const LAND_STEP = 4;
const QUAY_STEP = 6;
const LAND_HEIGHT = 1.4;
const OFFSHORE = 170;
const PAN_SPEED = 7;          // m/s, moving left
const SWAY_PERIOD = 46;       // s
const SWAY_DEG = 3;

const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const container = document.getElementById("city");
const wash = document.getElementById("wash");

// ── Seeded RNG (new coastline every load) ─────────────────────────────────
function mulberry32(seed) {
  return function () {
    seed |= 0; seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rand = mulberry32((Date.now() ^ (Math.random() * 1e9)) >>> 0);
const range = (a, b) => a + (b - a) * rand();
const srgb = (r, g, b) => new THREE.Color().setRGB(r, g, b, THREE.SRGBColorSpace);

// ── Coastline ─────────────────────────────────────────────────────────────
const harmonics = [];   // [k, amplitude, phase]
const warps = [];       // [m, amplitude, phase]
for (let k = 1; k <= 4; k++) harmonics.push([k, range(14, 48) / Math.sqrt(k), rand() * Math.PI * 2]);
{
  const rough = range(6, 16), falloff = range(0.9, 1.5);
  for (let k = 5; k < 48; k++) harmonics.push([k, rough * rand() * Math.pow(k / 5, -falloff), rand() * Math.PI * 2]);
  // Warp amplitudes keep x monotonic: sum(2*pi*m*A / TILE) < 1.
  let budget = 0.85;
  for (const m of [1, 2, 3]) {
    const share = range(0.15, 0.45) * budget;
    budget -= share;
    warps.push([m, share * TILE / (Math.PI * 2 * m), rand() * Math.PI * 2]);
  }
}
function coastZ(x) {
  let u = x;
  for (const [m, a, p] of warps) u += a * Math.sin((Math.PI * 2 * m * x) / TILE + p);
  let z = 0;
  for (const [k, a, p] of harmonics) z += a * Math.sin((Math.PI * 2 * k * u) / TILE + p);
  return z;
}

// ── One tile of boxes ─────────────────────────────────────────────────────
function generateTile() {
  const boxes = [];   // {p: [x,y,z], s: [w,h,d], yaw}
  const add = (p, s, yaw = 0) => boxes.push({ p, s, yaw });

  for (let x = 0; x < TILE; x += LAND_STEP) {
    const cx = x + LAND_STEP / 2, shore = coastZ(cx), depth = CITY_DEPTH + 60;
    add([cx, LAND_HEIGHT / 2, shore - depth / 2], [LAND_STEP + 0.05, LAND_HEIGHT, depth]);
  }
  for (let x = 0; x < TILE; x += QUAY_STEP) {
    const ax = x, az = coastZ(x), bx = x + QUAY_STEP, bz = coastZ(bx);
    const dx = bx - ax, dz = bz - az, h = LAND_HEIGHT + 0.9;
    add([(ax + bx) / 2, h / 2, (az + bz) / 2 - 2.5], [Math.hypot(dx, dz) + 0.6, h, 6], -Math.atan2(dz, dx));
  }
  for (let bx = 0; bx < TILE;) {
    const cellX = range(18, 34), cx = bx + cellX / 2, shore = coastZ(cx);
    for (let d = 14; d < CITY_DEPTH;) {
      const cellD = range(18, 34);
      if (rand() > 0.06) {
        let w = cellX * range(0.6, 0.92), dd = cellD * range(0.6, 0.92);
        const inland = d / CITY_DEPTH;
        let h = range(6, 18) + inland * 34 * Math.pow(rand(), 1.4);
        if (rand() < 0.015 + 0.025 * inland) { h = range(60, 130); w *= 0.85; dd *= 0.85; }   // landmark tower
        const px = cx + range(-2, 2), pz = shore - d - cellD / 2;
        add([px, h / 2, pz], [w, h, dd]);
        if (h > 18 && rand() < 0.35) {
          const h2 = h * range(0.2, 0.5);
          add([px, h + h2 / 2, pz], [w * 0.6, h2, dd * 0.6]);
        }
      }
      d += cellD;
    }
    bx += cellX;
  }
  return boxes;
}

// ── Scene ─────────────────────────────────────────────────────────────────
let renderer;
try {
  renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, powerPreference: "low-power" });
} catch (e) {
  container.classList.add("fallback");   // CSS shows the still render instead
  throw e;
}
renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.5));
renderer.setClearColor(0x000000, 0);    // sky is the CSS gradient behind the canvas
container.appendChild(renderer.domElement);

const scene = new THREE.Scene();
const fogColor = srgb(0.95, 0.975, 1.0);
scene.fog = new THREE.FogExp2(fogColor, 0.0031);

scene.add(new THREE.AmbientLight(srgb(0.36, 0.56, 0.90), 1.35));
const sun = new THREE.DirectionalLight(srgb(1.0, 0.99, 0.97), 2.1);
// Same direction as the app's sun: rotation (-50°, 130°) shining along -Z.
const sunDir = new THREE.Vector3(0, 0, -1).applyEuler(new THREE.Euler(THREE.MathUtils.degToRad(-50), THREE.MathUtils.degToRad(130), 0, "YXZ"));
sun.position.copy(sunDir).multiplyScalar(-100);
scene.add(sun, sun.target);

const boxes = generateTile();
const offsets = [-2, -1, 0, 1].map((k) => k * TILE);
const count = boxes.length * offsets.length;
const geo = new THREE.BoxGeometry(1, 1, 1);
const mat = new THREE.MeshLambertMaterial({ color: srgb(0.97, 0.98, 1.0) });

const city = new THREE.InstancedMesh(geo, mat, count);
const m4 = new THREE.Matrix4(), q = new THREE.Quaternion(), pos = new THREE.Vector3(), scl = new THREE.Vector3(), up = new THREE.Vector3(0, 1, 0);
let i = 0;
for (const off of offsets) {
  for (const b of boxes) {
    q.setFromAxisAngle(up, b.yaw);
    m4.compose(pos.set(b.p[0] + off, b.p[1], b.p[2]), q, scl.set(b.s[0], b.s[1], b.s[2]));
    city.setMatrixAt(i++, m4);
  }
}
city.instanceMatrix.needsUpdate = true;
city.frustumCulled = false;
scene.add(city);

// Mirrored copy for the reflection. A negative object scale makes three.js
// flip the face winding and normals for us.
const reflection = new THREE.InstancedMesh(geo, mat, count);
reflection.instanceMatrix = city.instanceMatrix;
reflection.scale.y = -1;
reflection.frustumCulled = false;
scene.add(reflection);

const water = new THREE.Mesh(
  new THREE.PlaneGeometry(TILE * 4, CITY_DEPTH + OFFSHORE + 900),
  new THREE.MeshBasicMaterial({ color: srgb(0.86, 0.92, 0.99), transparent: true, opacity: 0.5, depthWrite: false })
);
water.rotation.x = -Math.PI / 2;
water.position.set(0, 0.02, OFFSHORE + 300 - (CITY_DEPTH + OFFSHORE + 900) / 2);
scene.add(water);

const camera = new THREE.PerspectiveCamera(58, 1, 0.5, 1600);
camera.rotation.order = "YXZ";

// ── Size, scroll, animation ───────────────────────────────────────────────
function resize() {
  const w = window.innerWidth, h = window.innerHeight;
  renderer.setSize(w, h, false);
  camera.aspect = w / h;
  camera.updateProjectionMatrix();
}
window.addEventListener("resize", resize);
resize();

let scrollTarget = 0, scrollNow = 0;
function readScroll() {
  const max = document.documentElement.scrollHeight - window.innerHeight;
  scrollTarget = max > 0 ? window.scrollY / max : 0;
  // Fade a light wash over the city once past the hero, for text contrast.
  if (wash) wash.style.opacity = Math.min(1, window.scrollY / (window.innerHeight * 0.8)).toFixed(3);
}
window.addEventListener("scroll", readScroll, { passive: true });
readScroll();

let panT = 0, swayT = 0, last = performance.now();
function frame(now) {
  const dt = Math.min((now - last) / 1000, 0.1);
  last = now;
  if (!reduceMotion) {
    panT = (panT + dt) % (TILE / PAN_SPEED);           // wraps where the view is identical
    swayT = (swayT + dt) % (SWAY_PERIOD * 17);         // whole cycles of both sways
  }
  scrollNow += (scrollTarget - scrollNow) * Math.min(1, dt * 4);   // ease toward scroll

  const yaw = Math.sin((swayT * Math.PI * 2) / SWAY_PERIOD) * THREE.MathUtils.degToRad(SWAY_DEG);
  const bob = Math.sin((swayT * Math.PI * 2) / (SWAY_PERIOD * 1.7)) * 1.5;
  camera.position.set(-panT * PAN_SPEED, 36 + bob + scrollNow * 70, OFFSHORE - scrollNow * 40);
  camera.rotation.set(THREE.MathUtils.degToRad(-6.5 - scrollNow * 10), yaw, 0);

  renderer.render(scene, camera);
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
container.classList.add("live");
