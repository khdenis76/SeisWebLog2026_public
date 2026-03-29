export function initClickOnSTLine(options = {}) {
  const {
    tableId = "st-sailline-summary-table",
    tbodyId = "shot-summary-tbody",
    targetTbodyId = "sps-shot-compare-tbody",
    tableScrollId = "sps-shot-compare-table",
    selectedLineLabelId = "selected-sail-line"
  } = options;

  const table = document.getElementById(tableId);
  if (!table) return;

  const url = table.dataset.url;
  if (!url) {
    console.error("Missing data-url on table:", tableId);
    return;
  }

  const summaryTbody = document.getElementById(tbodyId);
  if (!summaryTbody) return;

  function isMismatchOnly() {
    const radio = document.getElementById("compareMismatch");
    return !!(radio && radio.checked);
  }

  function setSelectedLineLabel(lineCode) {
    const label = document.getElementById(selectedLineLabelId);
    if (label) {
      label.textContent = lineCode || "—";
    }
  }

  function highlightRow(row) {
    summaryTbody.querySelectorAll("tr.st-selected")
      .forEach(r => r.classList.remove("st-selected"));
    if (row) {
      row.classList.add("st-selected");
    }
  }

  async function loadCompare(lineCode, row = null) {
    if (!lineCode) return;

    if (row) {
      highlightRow(row);
    }
    setSelectedLineLabel(lineCode);

    const tbody = document.getElementById(targetTbodyId);
    if (!tbody) {
      console.error("Missing target tbody:", targetTbodyId);
      return;
    }

    tbody.innerHTML = `
      <tr>
        <td colspan="200" class="text-center py-3 text-muted">
          Loading...
        </td>
      </tr>
    `;

    try {
      const requestUrl = new URL(url, window.location.origin);
      requestUrl.searchParams.set("line_code", lineCode);

      if (isMismatchOnly()) {
        requestUrl.searchParams.set("mismatches_only", "1");
      }

      console.log("Request URL:", requestUrl.toString());

      const res = await fetch(requestUrl, {
        headers: { "X-Requested-With": "XMLHttpRequest" }
      });

      if (!res.ok) {
        const txt = await res.text();
        console.error("HTTP error:", res.status, txt);
        throw new Error("Request failed");
      }

      const data = await res.json();
      if (!data.ok) throw new Error(data.error || "Server error");

      const targetTbody = document.getElementById(targetTbodyId);
      if (!targetTbody) {
        console.error("Target tbody disappeared:", targetTbodyId);
        return;
      }

      targetTbody.innerHTML = data.tbody_html;

      document.getElementById(tableScrollId)?.scrollIntoView({
        behavior: "smooth",
        block: "start"
      });

    } catch (err) {
      console.error("Compare load error:", err);

      const tbodyErr = document.getElementById(targetTbodyId);
      if (tbodyErr) {
        tbodyErr.innerHTML = `
          <tr>
            <td colspan="200" class="text-danger text-center py-3">
              Failed to load data
            </td>
          </tr>
        `;
      }
    }
  }

  if (summaryTbody.dataset.clickBound !== "1") {
    summaryTbody.addEventListener("click", async (e) => {
      const row = e.target.closest("tr[data-line-code]");
      if (!row) return;

      if (e.target.closest('input[type="checkbox"]')) {
        return;
      }

      const lineCode = row.dataset.lineCode;
      if (!lineCode) return;

      await loadCompare(lineCode, row);
    });

    summaryTbody.dataset.clickBound = "1";
  }

  if (summaryTbody.dataset.compareModeBound !== "1") {
    document.querySelectorAll('input[name="compareMode"]').forEach((el) => {
      el.addEventListener("change", async () => {
        const activeRow = summaryTbody.querySelector("tr.st-selected[data-line-code]");
        if (!activeRow) return;

        const lineCode = activeRow.dataset.lineCode;
        if (!lineCode) return;

        await loadCompare(lineCode, activeRow);
      });
    });

    summaryTbody.dataset.compareModeBound = "1";
  }
}