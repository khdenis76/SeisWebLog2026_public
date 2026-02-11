export function activateTab(tabButtonId) {
  const tabBtn = document.getElementById(tabButtonId);
  if (!tabBtn) return;

  const tab = new bootstrap.Tab(tabBtn);
  tab.show();
}
