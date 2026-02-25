// DSRLineSort.js
// Sorts rows inside tbody#dsr-line-table-body using <tr data-...> attributes.
// Supported keys: line, daymax, preplot, deployment, retrieval, total

export function initDSRLineSort() {
  const tbody = document.getElementById("dsr-line-table-body");
  if (!tbody) return;

  const sortBtn = document.getElementById("dsrSortBtn"); // optional: updates label
  // Dropdown items should be: .dropdown-item[data-sort][data-dir]

  const parseISO = (s) => {
    // supports "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS"
    if (!s) return null;
    const t = String(s).trim();
    if (!t) return null;
    const iso = t.includes("T") ? t : t.replace(" ", "T");
    const ms = Date.parse(iso);
    return Number.isNaN(ms) ? null : ms;
  };

  const num = (v) => {
    // safe number parser: handles "", null, undefined
    if (v === null || v === undefined) return 0;
    const s = String(v).trim();
    if (!s) return 0;
    const n = Number(s);
    return Number.isNaN(n) ? 0 : n;
  };

  const getKey = (tr, key) => {
    if (key === "line") return num(tr.dataset.line);

    if (key === "daymax") {
      // prefer LastDeployTime / Last activity; fallback to start
      return (
        parseISO(tr.dataset.depend) ??
        parseISO(tr.dataset.depstart) ??
        parseISO(tr.dataset.day) ??
        0
      );
    }

    if (key === "preplot") return num(tr.dataset.preplotNodes);

    // NEW: hours
    if (key === "deployment") return num(tr.dataset.deploymentHours);
    if (key === "retrieval") return num(tr.dataset.retrievalHours);
    if (key === "total") return num(tr.dataset.totalHours);

    return 0;
  };

  const sortRows = (key, dir) => {
    const rows = Array.from(tbody.querySelectorAll("tr[data-line]"));
    const mult = dir === "desc" ? -1 : 1;

    rows.sort((a, b) => {
      const ka = getKey(a, key);
      const kb = getKey(b, key);

      if (ka < kb) return -1 * mult;
      if (ka > kb) return 1 * mult;

      // stable tie-breakers:
      // 1) line asc
      const la = num(a.dataset.line);
      const lb = num(b.dataset.line);
      if (la !== lb) return la - lb;

      // 2) daymax desc (latest first) to keep consistent ordering
      const da = getKey(a, "daymax");
      const db = getKey(b, "daymax");
      return db - da;
    });

    const frag = document.createDocumentFragment();
    rows.forEach((r) => frag.appendChild(r));
    tbody.appendChild(frag);
  };

  // Handle dropdown clicks (event delegation)
  document.addEventListener("click", (e) => {
    const item = e.target.closest(".dropdown-item[data-sort]");
    if (!item) return;

    const key = item.dataset.sort;
    const dir = (item.dataset.dir || "asc").toLowerCase();

    sortRows(key, dir);

    // update button label (optional)
    if (sortBtn) {
      const label = item.textContent.trim().replace(/\s+/g, " ");
      sortBtn.innerHTML = `<i class="fa-solid fa-arrow-down-a-z me-1"></i> ${label}`;
    }
  });
}