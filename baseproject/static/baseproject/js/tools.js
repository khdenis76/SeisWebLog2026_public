export function initPointTypeSwitch() {
  const btn = document.getElementById('point-type-switch');
  if (!btn) return;

  btn.addEventListener('click', () => {
    const isR = (btn.dataset.pointType ?? 'R') === 'R';
    const newType = isR ? 'S' : 'R';

    // --- update switch button ---
    btn.dataset.pointType = newType;

    btn.classList.toggle('btn-primary', newType === 'R');
    btn.classList.toggle('btn-danger', newType === 'S');

    btn.innerHTML = newType === 'R'
      ? '<i class="fas fa-sailboat me-1"></i> RECEIVERS'
      : '<i class="fas fa-bomb me-1"></i> SOURCES';

    // --- update tool buttons ---
    document.querySelectorAll('.tool-btn').forEach(el => {
      el.dataset.pointType = newType;
      el.classList.toggle('btn-primary', newType === 'R');
      el.classList.toggle('btn-danger', newType === 'S');
    });

    console.log('Point type switched to', newType);
  });
}
