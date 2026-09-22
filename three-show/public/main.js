import * as THREE from "./three.module.js";

const services = [
  { name: "PostgreSQL", color: 0x60a5fa },
  { name: "Kafka", color: 0xfb923c },
  { name: "ClickHouse", color: 0xfde047 },
  { name: "DataHub", color: 0x22d3ee },
];

const renderer = new THREE.WebGLRenderer({ antialias: true });
renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
renderer.setSize(innerWidth, innerHeight);
renderer.setClearColor(0x070b14);
renderer.outputColorSpace = THREE.SRGBColorSpace;
document.body.prepend(renderer.domElement);

const scene = new THREE.Scene();
scene.fog = new THREE.FogExp2(0x070b14, 0.045);
const camera = new THREE.PerspectiveCamera(50, innerWidth / innerHeight, 0.1, 80);

scene.add(new THREE.AmbientLight(0xb9c8e8, 0.45));
const key = new THREE.PointLight(0xffffff, 40, 24);
key.position.set(3, 5, 4);
scene.add(key);

const world = new THREE.Group();
scene.add(world);

const core = new THREE.Mesh(
  new THREE.IcosahedronGeometry(0.72, 1),
  new THREE.MeshStandardMaterial({
    color: 0x0c1424,
    emissive: 0x2563eb,
    emissiveIntensity: 0.9,
    metalness: 0.7,
    roughness: 0.22,
  }),
);
const cage = new THREE.Mesh(
  new THREE.IcosahedronGeometry(1.15, 1),
  new THREE.MeshBasicMaterial({ color: 0x7dd3fc, wireframe: true, transparent: true, opacity: 0.4 }),
);
world.add(core, cage);

function label(text) {
  const canvas = document.createElement("canvas");
  canvas.width = 512;
  canvas.height = 128;
  const ctx = canvas.getContext("2d");
  ctx.font = "600 56px sans-serif";
  ctx.fillStyle = "#f4f7ff";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(text, 256, 64);
  const map = new THREE.CanvasTexture(canvas);
  map.colorSpace = THREE.SRGBColorSpace;
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map, transparent: true, depthWrite: false }));
  sprite.scale.set(2.2, 0.55, 1);
  return sprite;
}

const moons = services.map((service, i) => {
  const angle = (i / services.length) * Math.PI * 2;
  const group = new THREE.Group();
  group.position.set(Math.cos(angle) * 3.5, Math.sin(angle * 2) * 0.35, Math.sin(angle) * 3.5);
  const body = new THREE.Mesh(
    new THREE.SphereGeometry(0.28, 32, 32),
    new THREE.MeshStandardMaterial({
      color: service.color,
      emissive: service.color,
      emissiveIntensity: 0.55,
      roughness: 0.35,
      metalness: 0.2,
    }),
  );
  const glow = new THREE.Mesh(
    new THREE.SphereGeometry(0.48, 24, 24),
    new THREE.MeshBasicMaterial({
      color: service.color,
      transparent: true,
      opacity: 0.16,
      depthWrite: false,
      blending: THREE.AdditiveBlending,
    }),
  );
  const name = label(service.name);
  name.position.y = 0.72;
  group.add(body, glow, name);
  world.add(group);
  return group;
});

const flowCount = 700;
const flowPos = new Float32Array(flowCount * 3);
const flowGeo = new THREE.BufferGeometry();
flowGeo.setAttribute("position", new THREE.BufferAttribute(flowPos, 3));
const flow = new THREE.Points(
  flowGeo,
  new THREE.PointsMaterial({
    color: 0xdbeafe,
    size: 0.055,
    transparent: true,
    opacity: 0.9,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
  }),
);
world.add(flow);
const curve = new THREE.CatmullRomCurve3(
  moons.map((moon) => moon.position.clone()).concat([moons[0].position.clone()]),
  false,
);

const helixCount = 1800;
const helixPos = new Float32Array(helixCount * 3);
const helixGeo = new THREE.BufferGeometry();
helixGeo.setAttribute("position", new THREE.BufferAttribute(helixPos, 3));
world.add(new THREE.Points(
  helixGeo,
  new THREE.PointsMaterial({
    color: 0x67e8f9,
    size: 0.028,
    transparent: true,
    opacity: 0.75,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
  }),
));

const starCount = 900;
const starPos = new Float32Array(starCount * 3);
for (let i = 0; i < starCount; i++) {
  const r = 12 + Math.random() * 18;
  const theta = Math.random() * Math.PI * 2;
  const phi = Math.acos(2 * Math.random() - 1);
  starPos[i * 3] = r * Math.sin(phi) * Math.cos(theta);
  starPos[i * 3 + 1] = r * Math.cos(phi);
  starPos[i * 3 + 2] = r * Math.sin(phi) * Math.sin(theta);
}
const stars = new THREE.BufferGeometry();
stars.setAttribute("position", new THREE.BufferAttribute(starPos, 3));
scene.add(new THREE.Points(stars, new THREE.PointsMaterial({ color: 0x9fb4d8, size: 0.035, transparent: true, opacity: 0.7 })));

let yaw = 0.5;
let pitch = 0.28;
let dist = 11;
let dragging = false;
let lx = 0;
let ly = 0;
addEventListener("pointerdown", (event) => {
  dragging = true;
  lx = event.clientX;
  ly = event.clientY;
});
addEventListener("pointerup", () => { dragging = false; });
addEventListener("pointerleave", () => { dragging = false; });
addEventListener("pointermove", (event) => {
  if (!dragging) return;
  yaw += (event.clientX - lx) * 0.005;
  pitch = Math.max(-0.6, Math.min(1.0, pitch + (event.clientY - ly) * 0.004));
  lx = event.clientX;
  ly = event.clientY;
});
addEventListener("wheel", (event) => {
  dist = Math.max(6.5, Math.min(18, dist + event.deltaY * 0.008));
}, { passive: true });
addEventListener("resize", () => {
  camera.aspect = innerWidth / innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(innerWidth, innerHeight);
});

const rate = document.querySelector("#rate");
const clock = new THREE.Clock();
let spin = 0;

function frame() {
  const t = clock.getElapsedTime();
  if (!dragging) spin += 0.0022;
  world.rotation.y = spin;

  core.rotation.y = t * 0.35;
  core.rotation.x = t * 0.12;
  cage.rotation.y = -t * 0.2;
  cage.rotation.z = t * 0.08;

  for (let i = 0; i < flowCount; i++) {
    const p = curve.getPoint((i / flowCount + t * 0.06) % 1);
    flowPos[i * 3] = p.x;
    flowPos[i * 3 + 1] = p.y;
    flowPos[i * 3 + 2] = p.z;
  }
  flowGeo.attributes.position.needsUpdate = true;

  for (let i = 0; i < helixCount; i++) {
    const u = i / helixCount;
    const a = u * Math.PI * 10 + t * 0.7;
    const strand = i % 2 ? Math.PI : 0;
    const y = u * 4.2 - 2.1;
    helixPos[i * 3] = Math.cos(a + strand) * 1.05;
    helixPos[i * 3 + 1] = y;
    helixPos[i * 3 + 2] = Math.sin(a + strand) * 1.05;
  }
  helixGeo.attributes.position.needsUpdate = true;

  moons.forEach((moon, i) => {
    moon.position.y = Math.sin(t * 0.8 + i) * 0.28;
  });

  camera.position.set(
    Math.sin(yaw) * Math.cos(pitch) * dist,
    Math.sin(pitch) * dist + 0.6,
    Math.cos(yaw) * Math.cos(pitch) * dist,
  );
  camera.lookAt(0, 0.2, 0);
  rate.textContent = Math.round(11840 + Math.sin(t * 1.7) * 860 + Math.sin(t * 0.37) * 220).toLocaleString();
  renderer.render(scene, camera);
  requestAnimationFrame(frame);
}

frame();
