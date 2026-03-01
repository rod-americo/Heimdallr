(() => {
    const STAGE_ORDER = ['before', 'during', 'after'];
    const STATUS_ORDER = {
        implemented: 0,
        'production-candidate': 1,
        'validation-ready': 2,
        'mvp-internal': 3,
        'in-review': 4,
        planned: 5,
        poc: 6,
        exploratory: 7
    };
    let activeRepoStateFilter = 'all';

    function repoStateLabel(state) {
        if (state === 'implemented') return 'Implemented';
        if (state === 'prototype') return 'Prototype';
        return 'Not Started';
    }

    function statusLabel(status) {
        if (status === 'implemented') return 'Implemented';
        if (status === 'production-candidate') return 'Production Candidate';
        if (status === 'validation-ready') return 'Validation Ready';
        if (status === 'mvp-internal') return 'MVP Internal';
        if (status === 'poc') return 'POC';
        if (status === 'in-review') return 'In Review';
        if (status === 'exploratory') return 'Exploratory';
        return 'Planned';
    }

    function compareProjects(a, b) {
        const aIsLive = a.repoState === 'implemented';
        const bIsLive = b.repoState === 'implemented';

        if (aIsLive !== bIsLive) return aIsLive ? -1 : 1;

        const statusDiff = (STATUS_ORDER[a.status] ?? 99) - (STATUS_ORDER[b.status] ?? 99);
        if (statusDiff !== 0) return statusDiff;

        const rankDiff = (a.rank ?? 999) - (b.rank ?? 999);
        if (rankDiff !== 0) return rankDiff;

        return a.title.localeCompare(b.title);
    }

    function render(projects) {
        STAGE_ORDER.forEach((stage) => {
            const items = projects
                .filter((project) => project.stage === stage)
                .filter((project) => activeRepoStateFilter === 'all' || project.repoState === activeRepoStateFilter)
                .sort(compareProjects);
            const host = document.getElementById(`cards-${stage}`);
            const counter = document.getElementById(`count-${stage}`);

            if (!host || !counter) return;

            counter.textContent = `${items.length} cards`;
            host.innerHTML = '';

            items.forEach((project) => {
                const card = document.createElement('article');
                card.className = 'project';
                card.innerHTML = `
                    <div>
                        <div class="project-title">${project.title}</div>
                        <small class="project-note">${project.note}</small>
                    </div>
                    <div style="display:flex; gap:6px; flex-wrap:wrap; justify-content:flex-end;">
                        <span class="flag repo-state repo-${project.repoState || 'not-started'} status-caps">${repoStateLabel(project.repoState)}</span>
                        <span class="flag ${project.status} status-caps">${statusLabel(project.status)}</span>
                    </div>
                `;
                host.appendChild(card);
            });
        });
    }

    async function init() {
        try {
            const response = await fetch('./data/pipeline-modules.json');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const projects = await response.json();
            const filterButtons = [...document.querySelectorAll('button[data-group=\"repo-state\"][data-filter]')];
            filterButtons.forEach((btn) => {
                btn.addEventListener('click', () => {
                    filterButtons.forEach((b) => b.classList.remove('active'));
                    btn.classList.add('active');
                    activeRepoStateFilter = btn.dataset.filter;
                    render(projects);
                });
            });
            render(projects);
        } catch (error) {
            console.error('Failed to load pipeline positioning data:', error);
        }
    }

    init();
})();
