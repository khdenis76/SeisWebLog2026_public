import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed

export function initBBCsvHeaderFetch() {
  const fileInput = document.getElementById("bbox-file-input");
  if (!fileInput) return;

  const url = fileInput.dataset.headersUrl;
  const err = document.getElementById("csvErr");

  function showErr(msg) {
    if (!err) return;
    err.textContent = msg;
    err.classList.remove("d-none");
  }
  function clearErr() {
    if (!err) return;
    err.textContent = "";
    err.classList.add("d-none");
  }

  fileInput.addEventListener("change", async () => {
    clearErr();

    const file = fileInput.files?.[0];
    if (!file) return;

    const fd = new FormData();
    fd.append("csv_file", file);

    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "X-CSRFToken": getCSRFToken() },
        body: fd,
      });

      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data.ok) {
        showErr(data.error || "Failed to read CSV headers.");
        return;
      }

      // ✅ Update options in all selects BUT KEEP current selection if possible
      if (data && typeof data.options_html === "string") {
        document.querySelectorAll(".bbox-config-selector").forEach((el) => {
          const prev = el.value;            // remember current selection
          el.innerHTML = data.options_html; // replace options
          window.__bboxApplyMappingToSelectors();
          // restore selection if still present in new options
          if (prev && Array.from(el.options).some((o) => o.value === prev)) {
            el.value = prev;
          }
        });

        // ✅ Apply mapping from selected config (if any)
        if (typeof window.__bboxApplyMappingToSelectors === "function") {
          window.__bboxApplyMappingToSelectors();
        }

      } else {
        console.error("bbox-config-selector: invalid options_html", data);
      }

    } catch (e) {
      console.error(e);
      if (e instanceof TypeError) {
        showErr("Network error while uploading CSV.");
      } else {
        showErr(e.message || "Unexpected error.");
      }
    }
  });
}
