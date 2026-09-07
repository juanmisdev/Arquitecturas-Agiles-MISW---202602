/* Experimento Broker — frontend logic.
   All displayed values come from GET /api/estado (real state), polled 400 ms.
   No simulated counters. */

const POLL_MS = 400;

const $ = (id) => document.getElementById(id);

let lastProcessed = null;
let lastDepth = null;
let lastPublished = null;
let experimentRunning = false;

// ------------------------------------------------- animaciones de actividad
// Detecta SI hay flujo en cada tramo (delta de contadores reales entre polls)
// y marca cada nodo con su estado: publishing / relaying / consuming / down.
function animateActivity(estado) {
  const now = {
    published: estado.publicadas ?? null,
    depth: estado.queue_depth ?? null,
    processed: estado.procesadas ?? null,
  };

  // --- tramo 1: Cotización -> Broker (publicación: delta de publicadas)
  const publishing = lastPublished !== null && now.published > lastPublished;
  // --- tramo 2: Broker -> Suscripción (consumo: delta de procesadas)
  const consuming = lastProcessed !== null && now.processed > lastProcessed;
  // --- broker reenviando: la cola se está vaciando (delta negativa de depth)
  const relaying = lastDepth !== null && now.depth !== null && now.depth < lastDepth;

  setCotizacionState(publishing);
  setBrokerState(relaying || (publishing && !consuming));
  setSuscripcionState(consuming, estado.suscripcion_status);

  lastPublished = now.published;
  lastDepth = now.depth;
  lastProcessed = now.processed;

  // líneas de flujo activas
  $("track-1").classList.toggle("flowing", publishing);
  $("track-2").classList.toggle("flowing", consuming || relaying);
}

function setCotizacionState(publishing) {
  $("node-cotizacion").classList.toggle("publishing", !!publishing);
}

function setBrokerState(active) {
  $("node-broker").classList.toggle("relaying", !!active);
}

function setSuscripcionState(consuming, status) {
  const node = $("node-suscripcion");
  node.classList.toggle("consuming", !!consuming && status !== "stopped");
  // el chip y la clase .down los maneja renderEstado (arriba/caído)
}

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
  setText("counter-errores", fmt(estado.errores_cliente));
  setText("counter-duplicados", fmt(estado.duplicados));

  // queue fill (relative to published total, min 100 to normalize)
  const denom = Math.max(estado.publicadas || 0, lastPublished || 0, 100);
  const pct = estado.queue_depth === null ? 0 : Math.min(100, (estado.queue_depth / denom) * 100);
  $("queue-fill").style.width = pct + "%";

  // animaciones de actividad por nodo (publicando/relay/consumiendo/caído)
  animateActivity(estado);

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


// ------------------------------------------------------------ experimento
async function iniciarExperimento() {
  const n = parseInt($("input-n").value, 10) || 100;
  const modo = $("select-modo").value;
  disableControls(true);
  $("experiment-phase").classList.remove("hidden");
  $("experiment-phase").textContent = "Iniciando…";
  try {
    const res = await fetch("/api/experimento/iniciar", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ n, modo }),
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
    publicando: "Publicando cotizaciones…",
    acumulando: "Acumulando en el broker…",
    reintegrando_suscripcion: "Reintegrando Suscripción…",
    drenando: "Drenando la cola…",
    generando_reporte: "Generando reporte…",
  };
  const armLabel = { sync: "Sync (REST)", async: "Async (broker)" };
  const timer = setInterval(async () => {
    try {
      const res = await fetch("/api/experimento/estado");
      const estado = await res.json();
      if (estado.status === "idle") return;
      const arm = estado.arm ? `[${armLabel[estado.arm] || estado.arm}] ` : "";
      $("experiment-phase").textContent =
        arm + (phaseNames[estado.phase] || estado.phase || "");
      if (estado.arm) setModeBadge(estado.arm);

      if (estado.status === "done") {
        clearInterval(timer);
        disableControls(false);
        setModeBadge($("select-modo").value);
        $("experiment-phase").classList.add("hidden");
        showReport(estado.report);
      } else if (estado.status === "error" || estado.status === "requires_manual") {
        clearInterval(timer);
        disableControls(false);
        setModeBadge($("select-modo").value);
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
  const arms = ["sync", "async"].filter((a) => report[a]);
  const armHead = { sync: "Sync (REST)", async: "Async (broker)" };
  const metrics = [
    ["operaciones_cliente", "Operaciones del cliente"],
    ["errores_cliente", "Errores vistos por el cliente"],
    ["encolados_durante_caida", "Encolados durante la caída"],
    ["procesados_reintegrar", "Procesados al reintegrar"],
    ["perdidos", "Perdidos"],
    ["duplicados", "Duplicados"],
  ];

  let html = "<table class='cmp'><thead><tr><th>Métrica</th>";
  html += arms.map((a) => `<th>${armHead[a]}</th>`).join("");
  html += "</tr></thead><tbody>";
  for (const [key, label] of metrics) {
    html += `<tr><td>${label}</td>`;
    html += arms.map((a) => `<td>${fmt(report[a][key])}</td>`).join("");
    html += "</tr>";
  }
  for (const stat of ["p50", "p95", "max"]) {
    html += `<tr><td>Latencia ${stat} (ms)</td>`;
    html += arms
      .map((a) => `<td>${fmt(report[a].latencia_ms && report[a].latencia_ms[stat])}</td>`)
      .join("");
    html += "</tr>";
  }
  html += "</tbody></table>";
  $("report-body").innerHTML = html;

  const v = report.verdict || {};
  const lines = [];
  if ("sync_propaga_falla" in v) {
    lines.push(
      `<div class="${v.sync_propaga_falla ? "verdict-bad" : "verdict-ok"}">${
        v.sync_propaga_falla
          ? "✗ Sync: la caída del consumidor llega al cliente (errores)"
          : "○ Sync: no se observaron errores en el cliente"
      }</div>`
    );
  }
  if ("broker_enmascara_falla" in v) {
    lines.push(
      `<div class="${v.broker_enmascara_falla ? "verdict-ok" : "verdict-bad"}">${
        v.broker_enmascara_falla
          ? "✓ Async: el broker enmascara la falla (0 errores, 0 perdidos)"
          : "✗ Async: no se cumplió — revisar cola durable / ack manual"
      }</div>`
    );
  }
  if (v.recomendacion) {
    lines.push(`<div class="verdict-reco">→ ${v.recomendacion}</div>`);
  }
  $("report-verdict").innerHTML = lines.join("");

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
  const modo = $("select-publicar-modo").value;
  await manualAction(
    "/api/experimento/publicar",
    `Publicando ${n} cotizaciones (${modo})…`,
    JSON.stringify({ n, modo })
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
const MODE_LABELS = {
  compare: "conector: comparar",
  sync: "conector: sync (REST)",
  async: "conector: async (broker)",
};
function setModeBadge(mode) {
  $("mode-badge").textContent = MODE_LABELS[mode] || `conector: ${mode}`;
}

function fmt(v) {
  return v === null || v === undefined ? "--" : v;
}
function setText(id, v) {
  const el = $(id);
  if (el) el.textContent = v;
}
function disableControls(disabled) {
  ["btn-experimento", "btn-stop", "btn-start", "btn-publicar"].forEach((id) =>
    $(id).disabled = disabled
  );
}

// ------------------------------------------------------------------ wiring
$("select-modo").addEventListener("change", (e) => setModeBadge(e.target.value));
setModeBadge($("select-modo").value);
$("btn-experimento").addEventListener("click", iniciarExperimento);
$("btn-stop").addEventListener("click", manualStop);
$("btn-start").addEventListener("click", manualStart);
$("btn-publicar").addEventListener("click", manualPublicar);
$("btn-close-modal").addEventListener("click", () => $("modal").classList.add("hidden"));
$("banner-dismiss").addEventListener("click", () => $("manual-banner").classList.add("hidden"));

pollEstado();
setInterval(pollEstado, POLL_MS);