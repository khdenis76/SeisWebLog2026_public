export function initDsrDeleteDropdown() {
  const menu = document.querySelector("#dsr-delete-btn + .dropdown-menu");

  if (!menu) return;

  menu.addEventListener("click", (e) => {
    const item = e.target.closest(".dropdown-item");
    if (!item) return;

    e.preventDefault();
    e.stopPropagation();

    const action = item.textContent.trim();

    switch (action) {
      case "All":
        console.log("Delete: ALL DSR records");
        // deleteAllDsr();
        break;

      case "SM (Deployment)":
        console.log("Delete: SM Deployment");
        // deleteDsrByType("SM_DEPLOYMENT");
        break;

      case "SM (Recovered)":
        console.log("Delete: SM Recovered");
        // deleteDsrByType("SM_RECOVERED");
        break;

      case "Rec DB":
        console.log("Delete: Rec DB");
        // deleteRecDb();
        break;

      default:
        console.warn("Unknown delete action:", action);
    }
  });
}
