function getCSRFToken() {
  const el = document.querySelector("[name=csrfmiddlewaretoken]");
  return el ? el.value : "";
}

function setBathyStatus(message, isError = false) {
  const el = document.getElementById("source-bathy-status");
  if (!el) return;

  el.textContent = message || "";
  el.classList.toggle("text-danger", isError);
  el.classList.toggle("text-muted", !isError);
}

function localFileUrl(path) {
  return "file:///" + String(path || "").replaceAll("\\", "/");
}

async function loadSourceBathyOptions(pane) {
  const res = await fetch(pane.dataset.optionsUrl);
  const data = await res.json();

  if (!data.ok) {
    throw new Error(data.error || "Could not load map options.");
  }

  const shapeSelect = document.getElementById("source-bathy-shape");
  const fireSelect = document.getElementById("source-bathy-fire-code");

  if (shapeSelect) {
    shapeSelect.innerHTML = `<option value="">No shape clipping</option>`;

    for (const shape of data.shapes || []) {
      const opt = document.createElement("option");
      opt.value = shape.filename;
      opt.textContent = shape.exists
        ? shape.filename
        : `${shape.filename} (missing file)`;
      opt.title = shape.fullname || "";
      shapeSelect.appendChild(opt);
    }
  }

  if (fireSelect) {
    fireSelect.innerHTML = "";

    for (const fc of data.fire_codes || []) {
      const opt = document.createElement("option");
      opt.value = fc.fire_code;
      opt.textContent = `${fc.fire_code} (${fc.count})`;
      fireSelect.appendChild(opt);
    }

    if ([...fireSelect.options].some((o) => o.value === "A")) {
      fireSelect.value = "A";
    }
  }
}

function renderOutputLinks(data) {
  const openLink = document.getElementById("source-bathy-open-link");

  if (openLink && data.html_path) {
    openLink.href = localFileUrl(data.html_path);
    openLink.classList.remove("disabled");
  }

  const statusParts = [];

  if (data.tif_path) statusParts.push(`TIF: ${data.tif_path}`);
  if (data.html_path) statusParts.push(`HTML: ${data.html_path}`);
  if (data.png_path) statusParts.push(`PNG: ${data.png_path}`);

  if (statusParts.length) {
    setBathyStatus(statusParts.join(" | "));
  }
}

async function generateSourceBathyMap(pane) {
  const btn = document.getElementById("source-bathy-generate-btn");

  const payload = {
    map_type: document.getElementById("source-bathy-map-type")?.value || "bathymetry_3d",
    fire_code: document.getElementById("source-bathy-fire-code")?.value || "A",
    cell_size: Number(document.getElementById("source-bathy-cell-size")?.value || 100),
    algorithm: document.getElementById("source-bathy-algorithm")?.value || "average",
    shape_filename: document.getElementById("source-bathy-shape")?.value || "",
    colorscale: document.getElementById("source-bathy-colorscale")?.value || "Earth",
    vertical_exaggeration: Number(document.getElementById("source-bathy-ve")?.value || 3),
    export_tif: document.getElementById("source-bathy-export-tif")?.checked ?? true,
    export_html: document.getElementById("source-bathy-export-html")?.checked ?? true,
    export_png: document.getElementById("source-bathy-export-png")?.checked ?? false,
  };

  try {
    if (btn) btn.disabled = true;

    setBathyStatus("Generating map...");

    const res = await fetch(pane.dataset.generateUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": getCSRFToken(),
      },
      body: JSON.stringify(payload),
    });

    const data = await res.json();

    if (!res.ok || !data.ok) {
      throw new Error(data.error || "Map generation failed.");
    }

    const resultCard = document.getElementById("source-bathy-result");
    const plotDiv = document.getElementById("source-bathy-plotly");
    const title = document.getElementById("source-bathy-result-title");

    if (title) title.textContent = data.map_title || "Generated map";

    if (data.plotly_json && plotDiv && window.Plotly) {
      const fig = JSON.parse(data.plotly_json);

      await Plotly.react(
  plotDiv,
  fig.data,
  {
    ...fig.layout,

    autosize: true,

    margin: {
      l: 0,
      r: 0,
      t: 40,
      b: 0,
    },

    scene: {
      ...fig.layout.scene,

      aspectmode: "data",

      camera: {
        eye: {
          x: 1.6,
          y: 1.6,
          z: 0.8,
        }
      }
    }
  },
  {
    responsive: true,
    displaylogo: false,
    scrollZoom: true,
  }
);

// initial resize after render
requestAnimationFrame(() => {
  Plotly.Plots.resize(plotDiv);
});

// global resize trigger
window.dispatchEvent(new Event("resize"));

// attach resize handlers only once
if (!plotDiv.dataset.resizeAttached) {

  plotDiv.dataset.resizeAttached = "1";

  // browser resize
  window.addEventListener("resize", () => {
    Plotly.Plots.resize(plotDiv);
  });

  // bootstrap/flex/container resize
  const resizeObserver = new ResizeObserver(() => {
    Plotly.Plots.resize(plotDiv);
  });

  resizeObserver.observe(plotDiv);

  // tab activation resize
  const tabEl = document.getElementById("source-bathy-map-tab");

  if (tabEl) {
    tabEl.addEventListener("shown.bs.tab", () => {
      setTimeout(() => {
        Plotly.Plots.resize(plotDiv);
      }, 50);
    });
  }

  // delayed resize after sidebar toggle/layout updates
  setTimeout(() => {
    Plotly.Plots.resize(plotDiv);
  }, 150);
}
    }

    if (resultCard) resultCard.classList.remove("d-none");

    renderOutputLinks(data);
  } catch (err) {
    setBathyStatus(err.message, true);
  } finally {
    if (btn) btn.disabled = false;
  }
}

export function initSourceBathyMaps() {
  const pane = document.getElementById("source-bathy-map-pane");
  if (!pane || pane.dataset.ready === "1") return;

  pane.dataset.ready = "1";

  loadSourceBathyOptions(pane).catch((err) => {
    setBathyStatus(err.message, true);
  });

  const btn = document.getElementById("source-bathy-generate-btn");
  if (btn) {
    btn.addEventListener("click", () => generateSourceBathyMap(pane));
  }
}