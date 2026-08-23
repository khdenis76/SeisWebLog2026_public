export function initRecDbStatisticsChart() {
  const tab = document.getElementById("pills-recdb-stat-tab");
  const target = document.getElementById("recdb-max-distance-chart");
  const dataElement = document.getElementById("recdb-max-distance-data");

  if (!tab || !target || !dataElement) return;

  let rendered = false;

  function renderChart(attempt = 0) {
    if (rendered) {
      if (window.Plotly) window.Plotly.Plots.resize(target);
      return;
    }

    if (!window.Plotly) {
      if (attempt < 20) window.setTimeout(() => renderChart(attempt + 1), 150);
      return;
    }

    let rows = [];
    try {
      rows = JSON.parse(dataElement.textContent || "[]");
    } catch (error) {
      console.error("Could not parse REC_DB maximum-distance data", error);
      return;
    }

    if (!rows.length) return;

    const isDark = document.documentElement.getAttribute("data-bs-theme") === "dark";
    const textColor = isDark ? "#dee2e6" : "#212529";
    const gridColor = isDark ? "#495057" : "#dee2e6";

    window.Plotly.newPlot(target, [{
      type: "scatter",
      mode: "lines+markers",
      x: rows.map((row) => row.line),
      y: rows.map((row) => row.max_distance),
      line: {color: "#0d6efd", width: 2},
      marker: {color: "#0d6efd", size: 7},
      hovertemplate: "Line %{x}<br>Max distance: %{y:.3f} m<extra></extra>",
      name: "Max distance"
    }], {
      autosize: true,
      height: 360,
      margin: {l: 65, r: 20, t: 20, b: 60},
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: {color: textColor},
      xaxis: {
        title: "Receiver Line",
        type: "linear",
        gridcolor: gridColor,
        zeroline: false
      },
      yaxis: {
        title: "Maximum Distance REC_DB to Preplot (m)",
        rangemode: "tozero",
        gridcolor: gridColor,
        zeroline: true
      },
      hovermode: "closest",
      showlegend: false
    }, {
      responsive: true,
      displaylogo: false
    });

    rendered = true;
  }

  tab.addEventListener("shown.bs.tab", () => renderChart());
}
