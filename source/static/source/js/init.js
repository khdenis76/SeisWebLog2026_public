import {initSourceUploadModal} from "./initSourceUploadModal.js";
import {initSourceUpload} from "./initSourceUpload.js";
import {initColumnToggles} from "../../rov/js/initColumnToggles.js";
import {initSpsTableSelection} from "./initSPSTableSelection.js";

export function initAllJSForSource() {
    initSourceUploadModal();
    initSourceUpload();
    initSpsTableSelection();
    initColumnToggles(
    "source_toggle-left-rov-btn",
    "source_left-rov-col",
    "source_left-rov-toggle-icon",
    { toggleElId: "source_right-rov-col", divOn: "col-12", divOff: "col-4" }
  );

  initColumnToggles(
    "source_toggle-right-rov-btn",
    "source_right-rov-col",
    "source_right-rov-toggle-icon",
    { toggleElId: "source_left-rov-col", divOn: "col-12", divOff: "col-4" }
  );
}