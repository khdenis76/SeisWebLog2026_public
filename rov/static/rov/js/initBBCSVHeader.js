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

      const data = await res.json();
      if (!res.ok || !data.ok) {
        showErr(data.error || "Failed to read CSV headers.");
        return;
      }

      // Put same options into all selects
      if (data && typeof data.options_html === "string") {
             document.querySelectorAll(".bbox-config-selector").forEach(el => {
                 el.innerHTML = data.options_html;
             });
      } else {
                   console.error("bbox-config-selector: invalid options_html", data);
      }

      // (Optional) auto-select common names (client side)
      const headersLower = (data.headers || []).map(h => h.toLowerCase());
      const pick = (sel, aliases) => {
        for (let i = 0; i < headersLower.length; i++) {
          const h = headersLower[i];
          if (aliases.some(a => h === a || h.includes(a))) {
            sel.value = data.headers[i];
            return;
          }
        }
      };
      //pick(selPoint, ["point", "point_name", "name", "id", "station", "pt"]);
      //pick(selX, ["x", "easting", "east", "lon", "longitude"]);
      //pick(selY, ["y", "northing", "north", "lat", "latitude"]);
      //pick(selZ, ["z", "elev", "elevation", "depth"]);

    } catch (e) {
      console.error(e);
      if (e instanceof TypeError) {
         // usually fetch/network problems
         showErr("Network error while uploading CSV.");
      } else {
               showErr(e.message || "Unexpected error.");
      }
    }
  });
}
