export function initSVPRowSelect() {
  const rows = document.querySelectorAll('.svp-profile-row[data-profile-url]');
  rows.forEach((row) => {
    row.addEventListener('click', () => {
      const url = row.dataset.profileUrl;
      if (url) window.location.href = url;
    });
  });
}
