import { getCSRFToken } from "./csrf.js";
import { updateRLPreplotTable } from "./updaterltable.js";
import { renderBokehInto } from "./renderBokeh.js";
import { showAppToast } from "./toast.js";

export function initDeleteRL() {
  const btnDelete = document.getElementById("btnDeleteRL");
  const modalEl = document.getElementById("confirmDeleteModal");
  const summaryEl = document.getElementById("deleteSummary");
  const btnConfirm = document.getElementById("btnConfirmDeleteRL");

  if (!btnDelete || !modalEl || !btnConfirm) return;

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
  let pendingIds = [];

  btnDelete.addEventListener("click", () => {
    const checked = Array.from(document.querySelectorAll(".rl-preplot-checkbox:checked"));

    pendingIds = checked
      .map(cb => Number(cb.dataset.lineId))
      .filter(n => Number.isFinite(n));

    if (pendingIds.length === 0) {
      showAppToast("Select receiver lines first.", { title: "Nothing selected", variant: "warning" });
      return;
    }

    if (summaryEl) summaryEl.textContent = `Selected lines: ${pendingIds.length}`;
    modal.show();
  });

  btnConfirm.addEventListener("click", async () => {
    if (pendingIds.length === 0) return;

    btnConfirm.disabled = true;
    try {
      const resp = await fetch(window.RL_DELETE_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
          "X-Requested-With": "XMLHttpRequest",
        },
        body: JSON.stringify({ ids: pendingIds }),
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }

      const data = await resp.json();
      if (data.preplot_map) renderBokehInto("preplot-map-div", data.preplot_map);
      if (data.prep_stat) document.getElementById("preplot-statcard").innerHTML = data.prep_stat;
      updateRLPreplotTable(data.rl_rows);

      const main = document.getElementById("MainRLPreplotCheckbox");
      if (main) main.checked = false;

      modal.hide();
      pendingIds = [];
      showAppToast("Receiver lines deleted successfully.", { title: "Project updated", variant: "success" });
    } catch (e) {
      console.error(e);
      showAppToast(`Delete failed: ${e.message || e}`, { title: "Delete failed", variant: "danger" });
    } finally {
      btnConfirm.disabled = false;
    }
  });
}

export function initDeletePreplot(configs) {
  const modalEl = document.getElementById("confirmDeleteModal");
  const titleEl = document.getElementById("confirmDeleteTitle");
  const summaryEl = document.getElementById("deleteSummary");
  const btnConfirm = document.getElementById("btnConfirmDelete");

  if (!modalEl || !btnConfirm) return;

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
  let ctx = null;

  if (!btnConfirm.__boundConfirm) {
    btnConfirm.addEventListener("click", async () => {
      if (!ctx || ctx.pendingIds.length === 0) return;

      btnConfirm.disabled = true;
      try {
        const resp = await fetch(ctx.deleteUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": getCSRFToken(),
            "X-Requested-With": "XMLHttpRequest",
          },
          body: JSON.stringify({ ids: ctx.pendingIds }),
        });

        if (!resp.ok) throw new Error(await resp.text());
        const data = await resp.json();
        if (data.preplot_map) renderBokehInto("preplot-map-div", data.preplot_map);
        if (data.prep_stat) document.getElementById("preplot-statcard").innerHTML = data.prep_stat;

        const freshRows = Array.isArray(data?.[ctx.rowsKey]) ? data[ctx.rowsKey] : [];
        ctx.onUpdatedRows(freshRows);

        const main = document.getElementById(ctx.mainCheckboxId);
        if (main) main.checked = false;

        modal.hide();
        showAppToast(`${ctx.pendingIds.length} line(s) deleted.`, { title: "Project updated", variant: "success" });
        ctx = null;
      } catch (e) {
        console.error(e);
        showAppToast(`Delete failed: ${e.message || e}`, { title: "Delete failed", variant: "danger" });
      } finally {
        btnConfirm.disabled = false;
      }
    });
    btnConfirm.__boundConfirm = true;
  }

  if (!document.__boundDeletePreplot) {
    document.addEventListener("click", (e) => {
      const clickedBtn = e.target.closest("button");
      if (!clickedBtn || !clickedBtn.id) return;

      const c = configs.find(x => x.deleteBtnId === clickedBtn.id);
      if (!c) return;

      const checked = Array.from(document.querySelectorAll(`.${c.checkboxClass}:checked`));
      const ids = checked
        .map(cb => cb.dataset.lineId)
        .filter(v => v && v.trim() !== "")
        .map(Number)
        .filter(Number.isFinite);

      if (ids.length === 0) {
        showAppToast("Select line rows first.", { title: "Nothing selected", variant: "warning" });
        return;
      }

      if (titleEl) titleEl.textContent = c.title || "Delete";
      if (summaryEl) summaryEl.textContent = `Selected lines: ${ids.length}`;

      ctx = {
        pendingIds: ids,
        deleteUrl: c.deleteUrl,
        rowsKey: c.rowsKey,
        onUpdatedRows: c.onUpdatedRows,
        mainCheckboxId: c.mainCheckboxId,
      };

      modal.show();
    });

    document.__boundDeletePreplot = true;
  }
}
