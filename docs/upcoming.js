(() => {
    const liveStatuses = new Set(['mvp-internal', 'validation-ready', 'production-candidate', 'implemented']);

    function applyAutoLabels() {
        const tables = document.querySelectorAll('table[data-auto-labels]');
        tables.forEach((table) => {
            const headers = [...table.querySelectorAll('thead th')].map((th) => th.textContent.trim());
            table.querySelectorAll('tbody tr').forEach((tr) => {
                [...tr.children].forEach((td, idx) => {
                    td.setAttribute('data-label', headers[idx] || '');
                });
            });
        });
    }

    function renderBacklog(rows) {
        const tbody = document.getElementById('upcoming-backlog-body');
        if (!tbody) return;

        tbody.innerHTML = rows
            .filter((row) => Number.isInteger(row.rank) && (row.backlogModule || !liveStatuses.has(row.status)))
            .sort((a, b) => a.rank - b.rank)
            .map((row) => `
                <tr>
                    <td>${row.rank}</td>
                    <td>${row.backlogModule || row.title}</td>
                    <td>${row.pillarCode}</td>
                    <td>${row.pillar}</td>
                    <td>${row.impact}</td>
                    <td>${row.friction}</td>
                    <td>${row.whyNow}</td>
                </tr>
            `)
            .join('');

        applyAutoLabels();
    }

    async function init() {
        try {
            const response = await fetch('./data/pipeline-modules.json');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const rows = await response.json();
            renderBacklog(rows);
        } catch (error) {
            console.error('Failed to load upcoming backlog data:', error);
            const tbody = document.getElementById('upcoming-backlog-body');
            if (tbody) {
                tbody.innerHTML = '<tr><td colspan="7">Failed to load backlog data.</td></tr>';
                applyAutoLabels();
            }
        }
    }

    init();
})();
