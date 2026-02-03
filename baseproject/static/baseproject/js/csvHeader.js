import {getCSRFToken} from "./csrf.js";

export function initCsvHeaderFetch() {
  const fileInput = document.getElementById("csvFileInput");
  if (!fileInput) return;

  const url = fileInput.dataset.headersUrl;

  const selPoint = document.getElementById("selPointName");
  const selX = document.getElementById("selX");
  const selY = document.getElementById("selY");
  const selZ = document.getElementById("selZ");
  const attr1 = document.getElementById("attr1");
  const attr2 = document.getElementById("attr2");
  const attr3 = document.getElementById("attr3");
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
      selPoint.innerHTML = data.options_html;
      selX.innerHTML = data.options_html;
      selY.innerHTML = data.options_html;
      selZ.innerHTML = data.options_html;
      attr1.innerHTML = data.options_html;
      attr2.innerHTML = data.options_html;
      attr3.innerHTML = data.options_html;

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
      pick(selPoint, ["point", "point_name", "name", "id", "station", "pt"]);
      pick(selX, ["x", "easting", "east", "lon", "longitude"]);
      pick(selY, ["y", "northing", "north", "lat", "latitude"]);
      pick(selZ, ["z", "elev", "elevation", "depth"]);

    } catch (e) {
      showErr("Network error while uploading CSV.");
    }
  });
}
