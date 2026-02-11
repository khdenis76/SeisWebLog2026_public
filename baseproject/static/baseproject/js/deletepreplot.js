import {getCSRFToken} from "./csrf.js";
import {updatePreplotTable, updateRLPreplotTable} from "./updaterltable.js"
import {renderBokehInto} from "./renderBokeh.js";
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
      alert("No lines selected");
      return;
    }

    if (summaryEl) summaryEl.textContent = `Selected lines: ${pendingIds.length}`;
    modal.show();
  });

  btnConfirm.addEventListener("click", async () => {
    if (pendingIds.length === 0) return;

    btnConfirm.disabled = true;
    console.log("SENDING IDS:", pendingIds);
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
      if (data.preplot_map) {
          renderBokehInto("preplot-map-div", data.preplot_map);
      }
      if (data.prep_stat){
          document.getElementById("preplot-statcard").innerHTML = data.prep_stat
      }
      // ✅ перерисовать из свежих данных
      updateRLPreplotTable(data.rl_rows)

      // снять main checkbox
      const main = document.getElementById("MainRLPreplotCheckbox");
      if (main) main.checked = false;

      modal.hide();
      pendingIds = [];
    } catch (e) {
      console.error(e);
      alert("Delete failed: " + (e.message || e));
    } finally {
      btnConfirm.disabled = false;
    }
  });
}
function bindOnce(el, event, handler, flag) {
  if (!el) return;
  const key = `__bound_${flag}`;
  if (el[key]) return;
  el.addEventListener(event, handler);
  el[key] = true;
}

export function initDeletePreplot(configs) {
  const modalEl = document.getElementById("confirmDeleteModal");
  const titleEl = document.getElementById("confirmDeleteTitle");
  const summaryEl = document.getElementById("deleteSummary");
  const btnConfirm = document.getElementById("btnConfirmDelete");

  if (!modalEl || !btnConfirm) return;

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl);

  let ctx = null;

  // ✅ confirm handler (один раз)
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
        if (data.preplot_map) {
          console.log("Preplot map")
          renderBokehInto("preplot-map-div", data.preplot_map);
      }
      if (data.prep_stat){
         console.log("Preplot stat")
          document.getElementById("preplot-statcard").innerHTML = data.prep_stat
      }

        const freshRows = Array.isArray(data?.[ctx.rowsKey]) ? data[ctx.rowsKey] : [];
        ctx.onUpdatedRows(freshRows);

        const main = document.getElementById(ctx.mainCheckboxId);
        if (main) main.checked = false;

        modal.hide();
        ctx = null;
      } catch (e) {
        console.error(e);
        alert("Delete failed: " + (e.message || e));
      } finally {
        btnConfirm.disabled = false;
      }
    });
    btnConfirm.__boundConfirm = true;
  }

  // ✅ document click handler (ловит кнопки даже если появились позже)
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
        alert("No lines selected");
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




function collectSelectedRL() {
  const boxes = document.querySelectorAll(".rl-preplot-checkbox:checked");
  const ids = [];
  const tierLines = [];

  boxes.forEach(cb => {
    const id = cb.dataset.lineId;
    const tl = cb.dataset.tierLine;
    if (id) ids.push(Number(id));
    if (tl) tierLines.push(Number(tl));
  });

  return { ids, tierLines, count: boxes.length };
}
