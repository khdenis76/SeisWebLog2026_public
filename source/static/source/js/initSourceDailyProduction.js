export function initDailyProductionTab() {
  const tabBtn = document.getElementById('source-production-tab');
  const pane = document.getElementById('source-production-pane');
  const target = document.getElementById('daily-production');
  const msgEl = document.getElementById('source-stats-msg');

  if (!tabBtn || !pane || !target) return;

  let isLoaded = false;
  let isLoading = false;

  function setMsg(msg = '', isError = false) {
    if (!msgEl) return;
    msgEl.textContent = msg;
    msgEl.classList.toggle('text-muted', !isError);
    msgEl.classList.toggle('text-danger', isError);
  }

  function clearPlot() {
    target.innerHTML = '';
  }

  async function loadPlot(force = false) {
    if (isLoading) return;
    if (isLoaded && !force) return;

    const url = tabBtn.dataset.url;
    if (!url) {
      setMsg('Missing data-url on Daily Production tab button.', true);
      return;
    }

    isLoading = true;
    setMsg('Loading daily production...');
    clearPlot();

    try {
      const resp = await fetch(url, {
        method: 'GET',
        headers: {
          'X-Requested-With': 'XMLHttpRequest'
        }
      });

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }

      const item = await resp.json();

      if (!window.Bokeh || !window.Bokeh.embed) {
        throw new Error('Bokeh JS is not loaded.');
      }

      window.Bokeh.embed.embed_item(item, target);
      isLoaded = true;
      setMsg('');
    } catch (err) {
      console.error('Daily Production load failed:', err);
      setMsg(`Failed to load daily production: ${err.message}`, true);
    } finally {
      isLoading = false;
    }
  }

  tabBtn.addEventListener('shown.bs.tab', () => {
    loadPlot(false);
  });

  if (pane.classList.contains('active') && pane.classList.contains('show')) {
    loadPlot(false);
  }
}