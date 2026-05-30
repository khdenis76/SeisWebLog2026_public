import {initNoarLayout} from "./initNoarLayout.js";
import {initNoarLoadSps} from "./initNoarLoadSps.js";
import {initNoarRLSolutionsDelete} from "./initNoarRLSolutionsDelete.js";
import {initNoarRLSolutionsSelection} from "./initNoarRLSolutionsSelection.js";

document.addEventListener("DOMContentLoaded", () => {
     initNoarLayout();
     initNoarLoadSps();
     initNoarRLSolutionsDelete();
     initNoarRLSolutionsSelection();

});