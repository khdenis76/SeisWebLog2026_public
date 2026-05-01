export function initSpsRowLinePlot() {
    const plotEl = document.getElementById("source-line-map-plot");
    const depthEl = document.getElementById("gun-depth-plot");

    if (!plotEl) return;

    const url = plotEl.dataset.url;
    if (!url) {
        console.error("source-line-map-plot is missing data-url");
        return;
    }

    let currentLine = null;
    let currentRequestId = 0;

    function setPlotMessage(html) {
        plotEl.innerHTML = `${html}`;
    }

    function setDepthMessage(html) {
        if (depthEl) depthEl.innerHTML = `${html}`;
    }

    function clearSelectedRows() {
        document.querySelectorAll("tr.sps-row-active").forEach(tr => {
            tr.classList.remove("sps-row-active");
        });
    }

    function markRowSelected(tr) {
        clearSelectedRows();
        tr.classList.add("sps-row-active");
    }

    function shouldIgnoreClick(event) {
        return !!event.target.closest(
            "input, button, a, label, select, textarea, option"
        );
    }

    async function loadLinePlot(line, rowEl, forceReload = false) {
        line = parseInt(line, 10);

        if (!Number.isFinite(line) || line <= 0) {
            setPlotMessage("Invalid line");
            setDepthMessage("Invalid line");
            return;
        }

        if (!forceReload && currentLine === line) {
            markRowSelected(rowEl);
            return;
        }

        currentLine = line;
        currentRequestId += 1;
        const requestId = currentRequestId;

        markRowSelected(rowEl);

        setPlotMessage(`Loading line ${line} map...`);
        setDepthMessage(`Loading line ${line} depths...`);

        try {
            const qs = new URLSearchParams({ line: String(line) });

            const resp = await fetch(`${url}?${qs.toString()}`, {
                method: "GET",
                headers: { "X-Requested-With": "XMLHttpRequest" }
            });

            const data = await resp.json();

            if (requestId !== currentRequestId) return;

            if (!resp.ok || !data.ok) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }

            if (!window.Bokeh || !window.Bokeh.embed || !window.Bokeh.embed.embed_item) {
                throw new Error("Bokeh JS is not loaded");
            }

            plotEl.innerHTML = "";
            window.Bokeh.embed.embed_item(data.item, plotEl);

            if (depthEl) {
    depthEl.innerHTML = "";

    if (data.depth_item) {
        await window.Bokeh.embed.embed_item(data.depth_item, depthEl);
    } else {
        depthEl.innerHTML = `<div class="text-danger p-2">${data.depth_error || "No depth plot"}</div>`;
    }
}

        } catch (err) {
            if (requestId !== currentRequestId) return;

            console.error("Failed to load line plot:", err);

            setPlotMessage(`Failed to load map: ${err.message || err}`);
            setDepthMessage(`Failed to load depth plot: ${err.message || err}`);
        }
    }

    document.addEventListener("click", function (event) {
        const tr = event.target.closest("tr[id^='sps-row-']");
        if (!tr) return;
        if (shouldIgnoreClick(event)) return;

        const line = tr.dataset.line;
        loadLinePlot(line, tr);
    });

    window.reloadCurrentSourceLinePlot = function () {
        const activeRow = document.querySelector("tr.sps-row-active");
        if (!activeRow) return;
        loadLinePlot(activeRow.dataset.line, activeRow, true);
    };
}