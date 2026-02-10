(() => {
    const STAGE_ORDER = ['before', 'during', 'after'];

    function statusLabel(status) {
        if (status === 'mvp-internal') return 'MVP Internal';
        if (status === 'poc') return 'POC';
        if (status === 'in-review') return 'In Review';
        if (status === 'exploratory') return 'Exploratory';
        return 'Planned';
    }

    function render(projects) {
        STAGE_ORDER.forEach((stage) => {
            const items = projects.filter((project) => project.stage === stage);
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
                    <span class="flag ${project.status} status-caps">${statusLabel(project.status)}</span>
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
            render(projects);
        } catch (error) {
            console.error('Failed to load pipeline positioning data:', error);
        }
    }

    init();
})();
