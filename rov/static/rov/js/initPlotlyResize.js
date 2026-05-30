// initPlotlyResize.js

export function initPlotlyResize() {

    document.querySelectorAll(
        '[data-bs-toggle="tab"]'
    ).forEach(tab => {

        tab.addEventListener('shown.bs.tab', () => {

            // Force Plotly resize after tab becomes visible
            window.dispatchEvent(
                new Event('resize')
            );

        });

    });

}