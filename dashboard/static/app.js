/* Experimento Broker — frontend logic.
   All displayed values come from GET /api/estado (real state), polled 1s.
   No simulated counters. */

const POLL_MS = 1000;
const DOT_MAX = 12; // max dots rendered per track

const $ = (id) => document.getElementById(id);

let lastProcessed = null;
let lastDepth = null;
let lastPublished = null;
let experimentRunning = false;

// ------------------------------------------------------------- estado poll
async function pollEstado() {
  try {
    const res = await fetch("/api/estado");
    const estado = await res.json();
    renderEstado(estado);
  } catch (e) {
    console.error("poll /api/estado failed", e);
  }
}

function renderEstado(estado) {
  // counters
  setText("count-publicadas", fmt(estado.publicadas));
  setText("count-cola", fmt(estado.queue_depth));
  setText("count-procesadas", fmt(estado.procesadas));
  setText("counter-publicadas", fmt(estado.publicadas));
  setText("counter-cola", fmt(estado.queue_depth));
  setText("counter-procesadas", fmt(estado.procesadas));
  setText("counter-perdidos", "--");
  setText("counter-duplicados", fmt(estado.duplicados));

  // queue fill (relative to published total, min 100 to normalize)
  const denom = Math.max(estado.publicadas || 0, lastPublished || 0, 100);
  const pct = estado.queue_depth === null ? 0 : Math.min(100, (estado.queue_depth / denom) * 100);
  $("queue-fill").style.width = pct + "%";

  // animation dots: accumulate track1 while published > processed deltas;
  // track2 dots driven by drain delta (depth decrease).
  renderDots("dots-1", estado.publicadas, estado.queue_depth);
  renderDots("dots-2", estado.queue_depth, estado.procesadas);

  // suscripcion node state
  const chip = $("status-suscripcion");
  const status = estado.suscripcion_status;
  if (status === "running") {
    chip.textContent = "arriba";
    chip.className = "status-chip running";
    $("node-suscripcion").classList.remove("down");
  } else if (status === "stopped") {
    chip.textContent = "caído";
    chip.className = "status-chip stopped";
    $("node-suscripcion").classList.add("down");
  } else {
    chip.textContent = "estado: --";
    chip.className = "status-chip";
  }

  // docker mode badge + banner
  const badge = $("docker-badge");
  if (estado.docker_mode === "sdk") {
    badge.textContent = "docker: sdk";
    badge.className = "badge ok";
    $("manual-banner").classList.add("hidden");
  } else if (estado.docker_mode === "fallback") {
    badge.textContent = "docker: fallback";
    badge.className = "badge ok";
    $("manual-banner").classList.add("hidden");
  } else {
    badge.textContent = "docker: manual";
    badge.className = "badge manual";
    if (estado.manual_commands) {
      $("manual-cmds").textContent =
        estado.manual_commands.stop + "  ·  " + estado.manual_commands.start;
    }
    $("manual-banner").classList.remove("hidden");
  }
}

function renderDots(trackId, produced, consumed) {
  const container = $(trackId);
  container.innerHTML = "";
  if (produced === null || consumed === null) return;
  const inflight = Math.max(0, produced - consumed);
  const count = Math.min(DOT_MAX, inflight);
  for (let i = 0; i < count; i++) {
    const dot = document.createElement("div");
    dot.className = "dot";
    // stagger dots across the track; they accumulate as inflight grows
    dot.style.left = 8 + (i / Math.max(1, DOT_MAX - 1)) * 80 + "%";
    dot.style.animationDelay = (i * 0.12) + "s";
    container.appendChild(dot);
  }
}

// dots glide subtly via CSS animation
const style = document.createElement("style");
style.textContent = `
  .dot { animation: dotpulse 1.2s ease-in-out infinite; }
  @keyframes dotpulse { 0%,100% { transform: translateY(-2px); opacity: .7; }
                        50% { transform: translateY(2px); opacity: 1; } }`;
document.head.appendChild(style);

// ------------------------------------------------------------ experimento
async function iniciarExperimento() {
  const n = parseInt($("input-n").value, 10) || 100;
  disableControls(true);
  $("experiment-phase").classList.remove("hidden");
  $("experiment-phase").textContent = "Iniciando…";
  try {
    const res = await fetch("/api/experimento/iniciar", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ n }),
    });
    const data = await res.json();
    if (!data.ok) {
      $("experiment-phase").textContent = "Error: " + (data.error || "desconocido");
      disableControls(false);
      return;
    }
    trackExperiment();
  } catch (e) {
    $("experiment-phase").textContent = "Error de red: " + e;
    disableControls(false);
  }
}

async function trackExperiment() {
  const phaseNames = {
    iniciando: "Iniciando…",
    deteniendo_suscripcion: "Deteniendo Suscripción…",
    publicando: "Publicando eventos…",
    acumulando: "Eventos acumulándose en el broker…",
    reintegrando_suscripcion: "Reintegrando Suscripción…",
    drenando: "Drenando la cola…",
    generando_reporte: "Generando reporte…",
  };
  const timer = setInterval(async () => {
    try {
      const res = await fetch("/api/experimento/estado");
      const estado = await res.json();
      if (estado.status === "idle") return;
      $("experiment-phase").textContent =
        phaseNames[estado.phase] || estado.phase || "";

      if (estado.status === "done") {
        clearInterval(timer);
        disableControls(false);
        $("experiment-phase").classList.add("hidden");
        showReport(estado.report);
      } else if (estado.status === "error" || estado.status === "requires_manual") {
        clearInterval(timer);
        disableControls(false);
        $("experiment-phase").textContent =
          "Error: " + (estado.error || "") +
          (estado.manual ? " — ejecuta: " + estado.manual : "");
        $("experiment-phase").classList.remove("hidden");
      }
    } catch (e) {
      console.error("poll experimento/estado failed", e);
    }
  }, 800);
}

function showReport(report) {
  if (!report) return;
  const rows = [
    ["Cotizaciones publicadas", report.publicadas],
    ["Procesadas", report.procesadas],
    ["En cola (restante)", report.queue_depth],
    ["Perdidos", report.perdidos],
    ["Duplicados", report.duplicados],
  ];
  $("report-body").innerHTML = rows
    .map(
      ([k, v]) =>
        `<div class="report-row"><span>${k}</span><span>${fmt(v)}</span></div>`
    )
    .join("");

  const v = report.verdict || {};
  const ok = v.cero_perdidos && v.en_orden;
  $("report-verdict").innerHTML = `
    <div class="${v.cero_perdidos ? "verdict-ok" : "verdict-bad"}">
      ${v.cero_perdidos ? "✓ Cero eventos perdidos (auditoría evento_id)" : "✗ Se perdieron eventos"}
    </div>
    <div class="${v.en_orden ? "verdict-ok" : "verdict-bad"}">
      ${v.en_orden ? "✓ Procesados en orden" : "✗ Fuera de orden"}
    </div>
    <div class="${(report.duplicates || 0) === 0 ? "verdict-ok" : "verdict-bad"}">
      ${(report.duplicados || 0) === 0
        ? "✓ Sin duplicados"
        : `⚠ ${report.duplicados} duplicado(s) detectado(s)`}
    </div>`;

  $("modal").classList.remove("hidden");
}

// ------------------------------------------------------------ manual controls
async function manualStop() {
  await manualAction("/api/suscripcion/stop", "Deteniendo Suscripción…");
}

async function manualStart() {
  await manualAction("/api/suscripcion/start", "Reiniciando Suscripción…");
}

async function manualPublicar() {
  const n = parseInt($("input-publicar").value, 10) || 10;
  await manualAction(
    "/api/experimento/publicar",
    `Publicando ${n} eventos…`,
    JSON.stringify({ n })
  );
}

async function manualAction(url, msg, body) {
  const fb = $("manual-feedback");
  fb.className = "feedback";
  fb.textContent = msg;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body || "{}",
    });
    const data = await res.json();
    if (data.ok) {
      fb.className = "feedback ok";
      fb.textContent = "OK (" + (data.mode || "done") + ")";
    } else {
      fb.className = "feedback error";
      fb.textContent = data.manual
        ? "Falló — ejecuta manualmente: " + data.manual
        : "Falló: " + (data.error || "desconocido");
    }
  } catch (e) {
    fb.className = "feedback error";
    fb.textContent = "Error de red: " + e;
  }
}

// ---------------------------------------------------------------- helpers
function fmt(v) {
  return v === null || v === undefined ? "--" : v;
}
function setText(id, v) {
  $(id).textContent = v;
}
function disableControls(disabled) {
  ["btn-experimento", "btn-stop", "btn-start", "btn-publicar"].forEach((id) =>
    $(id).disabled = disabled
  );
}

// ------------------------------------------------------------------ wiring
$("btn-experimento").addEventListener("click", iniciarExperimento);
$("btn-stop").addEventListener("click", manualStop);
$("btn-start").addEventListener("click", manualStart);
$("btn-publicar").addEventListener("click", manualPublicar);
$("btn-close-modal").addEventListener("click", () => $("modal").classList.add("hidden"));
$("banner-dismiss").addEventListener("click", () => $("manual-banner").classList.add("hidden"));

pollEstado();
setInterval(pollEstado, POLL_MS);