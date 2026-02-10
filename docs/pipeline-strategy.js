(() => {
    let modules = [];

    const trackName = {
        ops: 'Operations',
        clinical: 'Clinical Decision Support',
        governance: 'Governance and Compliance',
        continuity: 'Care Continuity'
    };

    const cards = document.getElementById('cards');
    const liveToday = document.getElementById('liveToday');
    const filterButtons = [...document.querySelectorAll('button[data-group][data-filter]')];
    const isMobile = window.matchMedia('(max-width: 760px)').matches;
    const css = getComputedStyle(document.documentElement);
    const chartText = css.getPropertyValue('--chart-text').trim();
    const chartAxis = css.getPropertyValue('--chart-axis').trim();
    const chartGrid = css.getPropertyValue('--chart-grid').trim();
    const chartBorder = css.getPropertyValue('--chart-border').trim();
    const bubbleGood = css.getPropertyValue('--bubble-good').trim();
    const bubbleMid = css.getPropertyValue('--bubble-mid').trim();
    const bubbleHard = css.getPropertyValue('--bubble-hard').trim();
    const focus1 = css.getPropertyValue('--focus-1').trim();
    const focus2 = css.getPropertyValue('--focus-2').trim();
    const focus3 = css.getPropertyValue('--focus-3').trim();
    const focus4 = css.getPropertyValue('--focus-4').trim();
    let activeTrackFilter = 'all';
    let activeStatusFilter = 'all';

    function frictionBand(v) {
        if (v <= 3) return 'low';
        if (v <= 6) return 'medium';
        return 'high';
    }

    function impactBand(v) {
        if (v <= 3) return 'low';
        if (v <= 6) return 'medium';
        return 'high';
    }

    function statusLabel(v) {
        if (v === 'mvp-internal') return 'MVP Internal';
        if (v === 'validation-ready') return 'Validation Ready';
        if (v === 'production-candidate') return 'Production Candidate';
        if (v === 'in-review') return 'In Review';
        if (v === 'exploratory') return 'Exploratory';
        if (v === 'poc') return 'POC';
        if (v === 'implemented') return 'Implemented';
        return 'Planned';
    }

    function filterModules() {
        return modules.filter((m) => {
            const trackMatch = activeTrackFilter === 'all' || m.track === activeTrackFilter;
            const statusMatch = activeStatusFilter === 'all' || m.status === activeStatusFilter;
            return trackMatch && statusMatch;
        });
    }

    function renderLiveToday() {
        liveToday.innerHTML = '';
        const liveStatuses = new Set(['mvp-internal', 'validation-ready', 'production-candidate', 'implemented']);
        modules
            .filter((m) => liveStatuses.has(m.status))
            .forEach((m) => {
                const li = document.createElement('li');
                li.textContent = `${m.title}: ${m.note}`;
                liveToday.appendChild(li);
            });
    }

    function renderCards() {
        cards.innerHTML = '';
        const list = filterModules();
        list.forEach((m) => {
            const el = document.createElement('article');
            el.className = 'card';
            el.innerHTML = `
                <div class="card-top">
                    <span class="top-meta horizon ${m.horizon}"><span class="dot"></span>${m.horizon}</span>
                    <span class="top-meta status status-${m.status}"><span class="dot"></span>${statusLabel(m.status)}</span>
                </div>
                <h3>${m.title}</h3>
                <div class="track">${trackName[m.track]}</div>
                <div class="card-body">
                    <p class="small">${m.note}</p>
                </div>
                <div class="meta">
                    <span class="metric-pill impact ${impactBand(m.impact)}">Impact ${m.impact}/10</span>
                    <span class="metric-pill friction ${frictionBand(m.friction)}">Friction ${m.friction}/10</span>
                </div>
            `;
            cards.appendChild(el);
        });
    }

    filterButtons.forEach((btn) => {
        btn.addEventListener('click', () => {
            const group = btn.dataset.group;
            const value = btn.dataset.filter;
            filterButtons
                .filter((b) => b.dataset.group === group)
                .forEach((b) => b.classList.remove('active'));
            btn.classList.add('active');
            if (group === 'track') activeTrackFilter = value;
            if (group === 'status') activeStatusFilter = value;
            renderCards();
        });
    });

    const statusBorder = {
        planned: '#cbd5e1',
        poc: '#9fb2cf',
        'mvp-internal': '#6ee7ff',
        'validation-ready': '#66e2b0',
        'production-candidate': '#ffca8d',
        implemented: '#c6a4ff',
        'in-review': '#ffca8d',
        exploratory: '#f4a4ad'
    };

    function buildPriorityPoints(list) {
        const groups = new Map();
        list.forEach((m) => {
            const key = `${m.friction}-${m.impact}`;
            if (!groups.has(key)) groups.set(key, []);
            groups.get(key).push(m);
        });

        const jitterStep = 0.14;
        const jitterMax = 0.28;
        const radius = isMobile ? 8 : 9;

        const points = [];
        for (const [, group] of groups.entries()) {
            const n = group.length;
            group.forEach((m, idx) => {
                const center = (n - 1) / 2;
                let jitter = (idx - center) * jitterStep;
                if (jitter > jitterMax) jitter = jitterMax;
                if (jitter < -jitterMax) jitter = -jitterMax;
                points.push({
                    x: m.friction,
                    y: m.impact + jitter,
                    r: radius,
                    title: m.title,
                    status: m.status,
                    impact: m.impact,
                    friction: m.friction
                });
            });
        }
        return points;
    }

    function initCharts() {
        const priorityPoints = buildPriorityPoints(modules);

        new Chart(document.getElementById('priorityChart'), {
            type: 'bubble',
            data: {
                datasets: [{
                    data: priorityPoints,
                    backgroundColor: priorityPoints.map((p) => {
                        if (p.friction <= 4) return bubbleGood;
                        if (p.friction <= 7) return bubbleMid;
                        return bubbleHard;
                    }),
                    borderColor: priorityPoints.map((p) => statusBorder[p.status] || chartBorder),
                    borderWidth: 3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: isMobile ? 6 : 10
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => `${ctx.raw.title} | ${ctx.raw.status} | Impact ${ctx.raw.impact} | Friction ${ctx.raw.friction}`
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'linear',
                        min: 2,
                        max: 10,
                        title: { display: !isMobile, text: 'Implementation Friction', color: chartAxis },
                        ticks: { color: chartText, maxTicksLimit: isMobile ? 6 : 9, stepSize: 1 },
                        grid: { color: chartGrid }
                    },
                    y: {
                        type: 'linear',
                        min: 4,
                        max: 10,
                        title: { display: !isMobile, text: 'Clinical/Operational Impact', color: chartAxis },
                        ticks: { color: chartText, maxTicksLimit: isMobile ? 6 : 7, stepSize: 1 },
                        grid: { color: chartGrid }
                    }
                }
            }
        });

        const trackOrder = ['ops', 'clinical', 'governance', 'continuity'];
        const focusBreakdown = modules.reduce((acc, m) => {
            acc[m.track] = (acc[m.track] || 0) + 1;
            return acc;
        }, {});

        new Chart(document.getElementById('focusChart'), {
            type: 'doughnut',
            data: {
                labels: trackOrder.map((k) => trackName[k]),
                datasets: [{
                    data: trackOrder.map((k) => focusBreakdown[k] || 0),
                    backgroundColor: [focus1, focus2, focus3, focus4],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: isMobile ? 2 : 8
                },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            color: chartText,
                            usePointStyle: true,
                            pointStyle: 'rect',
                            boxWidth: isMobile ? 10 : 12,
                            padding: isMobile ? 8 : 12,
                            font: {
                                size: isMobile ? 11 : 13
                            }
                        }
                    }
                }
            }
        });
    }

    async function init() {
        try {
            const response = await fetch('./data/pipeline-modules.json');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            modules = await response.json();
            renderLiveToday();
            renderCards();
            initCharts();
        } catch (error) {
            console.error('Failed to load strategy modules:', error);
        }
    }

    init();
})();
