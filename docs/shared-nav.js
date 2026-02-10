(() => {
    const REPO_BASE = "https://github.com/rod-americo/Heimdallr/blob/main";

    const HERO_LINKS = [
        { href: "./index.html", label: "Open Docs Hub" },
        { href: "./motivation-justification.html", label: "Open Motivation Guide" },
        { href: "./pipeline-strategy.html", label: "Open Strategy Board" },
        { href: "./pipeline-product-positioning.html", label: "Open Product Positioning" },
        { href: "./upcoming.html", label: "Open Upcoming Roadmap" },
        { href: "./radiology-preprocessing-ecosystem-report.html", label: "Open Research Report" },
        { href: "./index-technical.html", label: "Open Technical Hub" },
        { href: `${REPO_BASE}/README.md`, label: "Open README" }
    ];

    const RELATED_LINKS = [
        { href: "./index.html", label: "Docs Hub" },
        { href: "./motivation-justification.html", label: "Motivation Guide" },
        { href: "./pipeline-strategy.html", label: "Strategy Board" },
        { href: "./pipeline-product-positioning.html", label: "Product Positioning" },
        { href: "./upcoming.html", label: "Upcoming Roadmap" },
        { href: "./radiology-preprocessing-ecosystem-report.html", label: "Research Report" },
        { href: "./index-technical.html", label: "Technical Hub" },
        { href: `${REPO_BASE}/README.md`, label: "README (Repo)" }
    ];

    function renderNav(container, links) {
        container.innerHTML = links
            .map((item) => {
                const isExternal = /^https?:\/\//.test(item.href);
                const attrs = isExternal ? ' target="_blank" rel="noopener noreferrer"' : "";
                return `<a href="${item.href}"${attrs}>${item.label}</a>`;
            })
            .join("");
    }

    document.querySelectorAll("[data-shared-nav]").forEach((container) => {
        const mode = container.getAttribute("data-shared-nav");
        if (mode === "hero") {
            renderNav(container, HERO_LINKS);
            return;
        }
        if (mode === "related") {
            renderNav(container, RELATED_LINKS);
        }
    });
})();
