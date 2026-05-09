export function initBaseProjectSolutions() {
  const tabBtn = document.getElementById("bp-solutions-tab-btn");
  const pane = document.getElementById("bp-solutions-pane");
  const container = document.getElementById("bp-solutions-container");

  if (!tabBtn || !pane || !container) return;

  let loaded = false;

  function getCSRFToken() {
    const input = document.querySelector("[name=csrfmiddlewaretoken]");
    if (input) return input.value;

    const match = document.cookie.match(/csrftoken=([^;]+)/);
    return match ? match[1] : "";
  }

  function showOnlySolutionsPane() {
    document.querySelectorAll(".bp-workspace-pane").forEach((el) => {
      el.classList.add("d-none");
    });

    pane.classList.remove("d-none");

    document.querySelectorAll(".bp-tab-btn").forEach((btn) => {
      btn.classList.remove("active");
    });

    tabBtn.classList.add("active");
  }

  async function loadSolutions(force = false) {
    if (loaded && !force) return;

    container.innerHTML = `
      <div class="p-3 text-muted small">
        <span class="spinner-border spinner-border-sm me-2"></span>
        Loading solutions...
      </div>
    `;

    const url = tabBtn.dataset.url;

    try {
      const response = await fetch(url, {
        method: "GET",
        headers: {
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const data = await response.json();

      if (!data.ok) {
        throw new Error(data.error || "Failed to load solutions.");
      }

      container.innerHTML = data.html;
      loaded = true;
      bindSolutionsEvents();

    } catch (err) {
      container.innerHTML = `
        <div class="alert alert-danger m-3">
          ${err.message}
        </div>
      `;
    }
  }

  function setMessage(message, type = "muted") {
    const msg = document.getElementById("bp-solutions-message");
    if (!msg) return;

    msg.className = `small text-${type}`;
    msg.textContent = message || "";
  }

  function bindSolutionsEvents() {
    const form = document.getElementById("bp-solution-add-form");

    if (form) {
      form.addEventListener("submit", async (event) => {
        event.preventDefault();

        const url = form.dataset.url;
        const formData = new FormData(form);

        try {
          const response = await fetch(url, {
            method: "POST",
            body: formData,
            headers: {
              "X-CSRFToken": getCSRFToken(),
              "X-Requested-With": "XMLHttpRequest",
            },
          });

          const data = await response.json();

          if (!data.ok) {
            throw new Error(data.error || "Failed to add solution.");
          }

          document.getElementById("bp-solutions-tbody").innerHTML = data.html;
          form.reset();
          setMessage("Solution added.", "success");
          bindSolutionsEvents();

        } catch (err) {
          setMessage(err.message, "danger");
        }
      });
    }

    document.querySelectorAll(".bp-delete-solution-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const solutionId = btn.dataset.id;
        const url = btn.dataset.url;

        if (!confirm("Delete this solution?")) return;

        const formData = new FormData();
        formData.append("solution_id", solutionId);

        try {
          const response = await fetch(url, {
            method: "POST",
            body: formData,
            headers: {
              "X-CSRFToken": getCSRFToken(),
              "X-Requested-With": "XMLHttpRequest",
            },
          });

          const data = await response.json();

          if (!data.ok) {
            throw new Error(data.error || "Failed to delete solution.");
          }

          document.getElementById("bp-solutions-tbody").innerHTML = data.html;
          setMessage("Solution deleted.", "success");
          bindSolutionsEvents();

        } catch (err) {
          setMessage(err.message, "danger");
        }
      });
    });
  }

  tabBtn.addEventListener("click", async () => {
    showOnlySolutionsPane();
    await loadSolutions();
  });
}