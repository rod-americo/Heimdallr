(() => {
    const HERO_LINKS = [
        { href: "./index.html", label: "Open Docs Hub" },
        { href: "./pipeline-strategy.html", label: "Open Strategy Board" },
        { href: "./upcoming.html", label: "Open Upcoming Roadmap" },
        { href: "./motivation-justification.html", label: "Open Motivation Guide" },
        { href: "./radiology-preprocessing-ecosystem-report.html", label: "Open Research Report" },
        { href: "https://github.com/rod-americo/Heimdallr/blob/main/README.md", label: "Open README" }
    ];

    const RELATED_LINKS = [
        { href: "./index.html", label: "Docs Hub" },
        { href: "./pipeline-strategy.html", label: "Strategy Board" },
        { href: "./motivation-justification.html", label: "Motivation Guide" },
        { href: "./upcoming.html", label: "Upcoming Roadmap" },
        { href: "./radiology-preprocessing-ecosystem-report.html", label: "Research Report" },
        { href: "https://github.com/rod-americo/Heimdallr/blob/main/docs/ARCHITECTURE.md", label: "Architecture Overview" },
        { href: "https://github.com/rod-americo/Heimdallr/blob/main/docs/validation-stage-manual.md", label: "Validation Stage Manual" },
        { href: "https://github.com/rod-americo/Heimdallr/blob/main/SECURITY.md", label: "Security Policy" },
        { href: "https://github.com/rod-americo/Heimdallr/blob/main/README.md", label: "README.md" },
        { href: "https://github.com/rod-americo/Heimdallr/blob/main/UPCOMING.md", label: "UPCOMING Source" },
        { href: "https://github.com/rod-americo/Heimdallr/blob/main/docs/radiology-preprocessing-ecosystem-report.md", label: "Research Report (Repository)" }
    ];

    function renderNav(container, links) {
        container.innerHTML = links
            .map((item) => `<a href="${item.href}">${item.label}</a>`)
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
