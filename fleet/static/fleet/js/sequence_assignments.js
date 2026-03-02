export function initSequenceAssignments() {

  const tblBody = document.querySelector("#tbl-assign tbody");
  if (!tblBody) return;   // safety if page not loaded

  const btnAdd = document.getElementById("btn-add");
  const btnSave = document.getElementById("btn-save");

  const modalEl = document.getElementById("assignModal");
  const modal = new bootstrap.Modal(modalEl);

  const modalTitle = document.getElementById("modalTitle");
  const modalErr = document.getElementById("modalErr");

  const rowId = document.getElementById("row-id");
  const seqFirst = document.getElementById("seq-first");
  const seqLast = document.getElementById("seq-last");
  const vesselId = document.getElementById("vessel-id");
  const purposeSel = document.getElementById("purpose");
  const comments = document.getElementById("comments");
  const isActive = document.getElementById("is-active");
  const allowOverlap = document.getElementById("allow-overlap");

  let cachedVessels = [];
  let cachedRows = [];
  let cachedPurposes = [];

  function showErr(msg) {
    modalErr.textContent = msg || "Error";
    modalErr.classList.remove("d-none");
  }

  function clearErr() {
    modalErr.classList.add("d-none");
    modalErr.textContent = "";
  }

  function csrfHeaders() {
    return {
      "Content-Type": "application/json",
      "X-CSRFToken": window.SEQ_API.csrf,
    };
  }

  async function apiGet(url) {
    const r = await fetch(url, { credentials: "same-origin" });
    return await r.json();
  }

  async function apiPost(url, data) {
    const r = await fetch(url, {
      method: "POST",
      headers: csrfHeaders(),
      body: JSON.stringify(data || {}),
      credentials: "same-origin",
    });
    const j = await r.json();
    if (!r.ok || !j.ok) throw new Error(j.error || "Request failed");
    return j;
  }

  function escapeHtml(s) {
    return String(s || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function fillVesselsSelect(selectedId) {
    vesselId.innerHTML = "";
    for (const v of cachedVessels) {
      const opt = document.createElement("option");
      opt.value = v.id;
      opt.textContent = v.vessel_name || ("Vessel #" + v.id);
      vesselId.appendChild(opt);
    }
    if (selectedId) vesselId.value = String(selectedId);
  }

  function fillPurposeSelect(selectedId) {
  purposeSel.innerHTML = "";

  for (const p of cachedPurposes) {
    const opt = document.createElement("option");
    opt.value = String(p.id);
    opt.textContent = p.label;
    purposeSel.appendChild(opt);
  }

  purposeSel.value = selectedId ? String(selectedId) : "1";
}

  function badgeActive(val) {
    return val
      ? `<span class="badge bg-success">Yes</span>`
      : `<span class="badge bg-secondary">No</span>`;
  }

  function renderTable() {
    tblBody.innerHTML = "";

    for (const r of cachedRows) {
      const tr = document.createElement("tr");
      const active = (r.is_active === 1 || r.is_active === true);

      tr.innerHTML = `
        <td class="text-muted">${r.id}</td>
        <td>${r.seq_first}</td>
        <td>${r.seq_last}</td>
        <td>${escapeHtml(r.vessel_name || "")}</td>
        <td>${escapeHtml(r.purpose_label || "")}</td>
        <td>${escapeHtml(r.comments || "")}</td>
        <td>${badgeActive(active)}</td>
        <td class="text-end">
          <button class="btn btn-sm btn-outline-primary me-1 btn-edit" data-id="${r.id}">
            <i class="fas fa-pen"></i>
          </button>
          <button class="btn btn-sm btn-outline-danger btn-del" data-id="${r.id}">
            <i class="fas fa-trash"></i>
          </button>
        </td>
      `;
      tblBody.appendChild(tr);
    }
  }

  function openAddModal() {
    clearErr();
    modalTitle.textContent = "Add Assignment";
    rowId.value = "";
    seqFirst.value = "";
    seqLast.value = "";
    comments.value = "";
    isActive.checked = true;
    allowOverlap.checked = false;

    fillVesselsSelect(cachedVessels.length ? cachedVessels[0]?.id : "");
    fillPurposeSelect("Production");

    modal.show();
  }

  function openEditModal(id) {
    const r = cachedRows.find(x => String(x.id) === String(id));
    if (!r) return;

    clearErr();
    modalTitle.textContent = "Edit Assignment";

    rowId.value = r.id;
    seqFirst.value = r.seq_first;
    seqLast.value = r.seq_last;
    comments.value = r.comments || "";
    isActive.checked = (r.is_active === 1 || r.is_active === true);
    allowOverlap.checked = false;

    fillVesselsSelect(r.vessel_id);
    fillPurposeSelect(r.purpose_id || 1);

    modal.show();
  }

  async function loadAll() {
    const j = await apiGet(window.SEQ_API.list);
    if (!j.ok) throw new Error(j.error || "Failed to load");

    cachedRows = j.rows || [];
    cachedVessels = j.vessels || [];
    cachedPurposes = j.purposes || [];

    renderTable();
  }

  async function saveModal() {
    clearErr();

    const payload = {
      id: rowId.value ? Number(rowId.value) : undefined,
      seq_first: Number(seqFirst.value),
      seq_last: Number(seqLast.value),
      vessel_id: Number(vesselId.value),
      purpose_id: Number(purposeSel.value),
      comments: comments.value || "",
      is_active: isActive.checked ? 1 : 0,
      allow_overlap: allowOverlap.checked ? 1 : 0,
    };

    if (
  !Number.isFinite(payload.seq_first) ||
  !Number.isFinite(payload.seq_last) ||
  !Number.isFinite(payload.vessel_id) ||
  !Number.isFinite(payload.purpose_id)
) {
  showErr("Seq First, Seq Last, Vessel and Purpose are required.");
  return;
}

    try {
      if (!payload.id)
        await apiPost(window.SEQ_API.add, payload);
      else
        await apiPost(window.SEQ_API.update, payload);

      modal.hide();
      await loadAll();
    } catch (e) {
      showErr(e.message || "Save failed");
    }
  }

  async function deleteRow(id) {
    if (!confirm("Delete this assignment?")) return;
    try {
      await apiPost(window.SEQ_API.del, { id: Number(id) });
      await loadAll();
    } catch (e) {
      alert(e.message || "Delete failed");
    }
  }

  // Events
  btnAdd.addEventListener("click", openAddModal);
  btnSave.addEventListener("click", saveModal);

  tblBody.addEventListener("click", (ev) => {
    const btnEdit = ev.target.closest(".btn-edit");
    const btnDel = ev.target.closest(".btn-del");
    if (btnEdit) openEditModal(btnEdit.dataset.id);
    if (btnDel) deleteRow(btnDel.dataset.id);
  });

  loadAll().catch(err => {
    console.error(err);
    alert(err.message || "Failed to load sequence assignments");
  });
}