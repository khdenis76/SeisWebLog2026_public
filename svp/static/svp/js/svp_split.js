(() => {
  const layout = document.getElementById('svp-split-layout');
  const divider = document.getElementById('svp-divider');
  const leftExpandBtn = document.getElementById('svp-left-expand-btn');
  const rightExpandBtn = document.getElementById('svp-right-expand-btn');
  const resetBtn = document.getElementById('svp-reset-layout-btn');
  const page = document.querySelector('.svp-page');

  if (!layout || !page) return;

  const setLeftSize = (percent) => {
    const clamped = Math.max(22, Math.min(78, percent));
    page.style.setProperty('--svp-left-size', `${clamped}%`);
  };

  if (divider && window.matchMedia('(min-width: 992px)').matches) {
    let isDragging = false;

    divider.addEventListener('mousedown', () => {
      isDragging = true;
      document.body.classList.add('user-select-none');
    });

    window.addEventListener('mousemove', (event) => {
      if (!isDragging) return;
      const rect = layout.getBoundingClientRect();
      const percent = ((event.clientX - rect.left) / rect.width) * 100;
      setLeftSize(percent);
    });

    window.addEventListener('mouseup', () => {
      isDragging = false;
      document.body.classList.remove('user-select-none');
    });
  }

  leftExpandBtn?.addEventListener('click', () => setLeftSize(60));
  rightExpandBtn?.addEventListener('click', () => setLeftSize(28));
  resetBtn?.addEventListener('click', () => setLeftSize(34));
})();
