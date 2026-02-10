#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();

async function resolveDocsDir() {
  const cwdDocs = path.join(ROOT, "radiology-preprocessing-ecosystem-report.md");
  const nestedDocs = path.join(ROOT, "docs", "radiology-preprocessing-ecosystem-report.md");
  try {
    await fs.access(cwdDocs);
    return ROOT;
  } catch {
    await fs.access(nestedDocs);
    return path.join(ROOT, "docs");
  }
}

function slugify(text) {
  return text
    .toLowerCase()
    .replace(/<[^>]*>/g, "")
    .replace(/&[a-z]+;/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function renderInline(text) {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/\[(.+?)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2">$1</a>')
    .replace(/\[(\d+)\]/g, '<a class="cite" href="#ref-$1" aria-label="Reference $1">[$1]</a>');
}

function renderInlineNoCite(text) {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/\[(.+?)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2">$1</a>');
}

function parseReferences(sectionContent) {
  const lines = sectionContent.split("\n").map((line) => line.trim()).filter(Boolean);
  const refs = [];
  for (const line of lines) {
    const match = line.match(/^\d+\.\s+(.*)$/);
    if (!match) continue;
    const body = match[1];
    const urlMatch = body.match(/(https?:\/\/\S+)\s*$/);
    if (!urlMatch) {
      refs.push(renderInlineNoCite(body));
      continue;
    }
    const url = urlMatch[1];
    const label = body.slice(0, body.length - url.length).trim();
    refs.push(`${renderInlineNoCite(label)} <a href="${url}">${url}</a>`);
  }
  return refs;
}

function markdownToHtml(markdown) {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  let i = 0;
  let title = "";
  let subtitle = "";
  let intro = [];
  const sections = [];
  let current = null;

  while (i < lines.length) {
    const raw = lines[i];
    const line = raw.trim();

    if (line.startsWith("# ")) {
      title = line.slice(2).trim();
      i += 1;
      continue;
    }

    if (!subtitle && line.startsWith("## ")) {
      subtitle = line.slice(3).trim();
      i += 1;
      continue;
    }

    if (line.startsWith("## ")) {
      if (current) sections.push(current);
      current = { heading: line.slice(3).trim(), content: [] };
      i += 1;
      continue;
    }

    if (current) {
      current.content.push(raw);
    } else {
      intro.push(raw);
    }
    i += 1;
  }

  if (current) sections.push(current);

  const introHtml = renderSectionBody(intro.join("\n"));
  const referencesSection = sections.find((s) => s.heading.toLowerCase() === "references");
  const mainSections = sections.filter((s) => s.heading.toLowerCase() !== "references");
  const refs = referencesSection ? parseReferences(referencesSection.content.join("\n")) : [];

  return { title, subtitle, introHtml, sections: mainSections, refs };
}

function renderSectionBody(content) {
  const lines = content.split("\n");
  const html = [];
  let i = 0;

  while (i < lines.length) {
    const raw = lines[i];
    const line = raw.trim();

    if (!line) {
      i += 1;
      continue;
    }

    if (line.startsWith("### ")) {
      html.push(`<h3>${renderInline(line.slice(4).trim())}</h3>`);
      i += 1;
      continue;
    }

    if (line.match(/^\d+\.\s+/)) {
      const items = [];
      while (i < lines.length && lines[i].trim().match(/^\d+\.\s+/)) {
        items.push(lines[i].trim().replace(/^\d+\.\s+/, ""));
        i += 1;
      }
      html.push("<ol>");
      for (const item of items) html.push(`<li>${renderInline(item)}</li>`);
      html.push("</ol>");
      continue;
    }

    if (line.startsWith("- ")) {
      const items = [];
      while (i < lines.length && lines[i].trim().startsWith("- ")) {
        items.push(lines[i].trim().slice(2));
        i += 1;
      }
      html.push("<ul>");
      for (const item of items) html.push(`<li>${renderInline(item)}</li>`);
      html.push("</ul>");
      continue;
    }

    html.push(`<p>${renderInline(line)}</p>`);
    i += 1;
  }

  return html.join("\n");
}

function buildReportHtml(parsed) {
  const toc = parsed.sections
    .filter((section) => section.heading.toLowerCase() !== "executive motivation")
    .map((section, idx) => {
      const id = slugify(section.heading.replace(/^\d+\.\s*/, ""));
      return `<a href="#${id}">${idx + 1}. ${renderInline(section.heading.replace(/^\d+\.\s*/, ""))}</a>`;
    })
    .join("\n                ");

  const panels = parsed.sections
    .map((section) => {
      const id = slugify(section.heading.replace(/^\d+\.\s*/, ""));
      return `
            <article class="panel" id="${id}">
                <h2>${renderInline(section.heading)}</h2>
                ${renderSectionBody(section.content.join("\n"))}
            </article>`;
    })
    .join("\n");

  const refsHtml = parsed.refs
    .map((ref, idx) => `<li id="ref-${idx + 1}">${ref}</li>`)
    .join("\n                    ");

  const introPanel = parsed.introHtml.trim()
    ? `
            <article class="panel">
                ${parsed.introHtml}
            </article>`
    : "";

  return `<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Radiology Preprocessing Ecosystem Report</title>
    <style>
        :root {
            --bg: #070c16;
            --bg-soft: #101a2d;
            --bg-card: #121f36;
            --text: #e8f0ff;
            --muted: #9eb0ca;
            --accent: #39d0ff;
            --line: rgba(126, 157, 210, 0.25);
            --radial-1: rgba(57, 208, 255, 0.15);
            --radial-2: rgba(138, 164, 255, 0.10);
            --hero-grad-a: rgba(18, 31, 54, 0.95);
            --hero-grad-b: rgba(10, 18, 32, 0.98);
            --link-text: #d6e8ff;
            --link-bg: #12203a;
            --ring: rgba(57, 208, 255, 0.2);
            --radius: 14px;
            color-scheme: dark light;
        }
        @media (prefers-color-scheme: light) {
            :root {
                --bg: #f3f7ff;
                --bg-soft: #ffffff;
                --bg-card: #f7faff;
                --text: #0f1b33;
                --muted: #4d6283;
                --accent: #0f87c9;
                --line: rgba(38, 68, 125, 0.18);
                --radial-1: rgba(15, 135, 201, 0.10);
                --radial-2: rgba(86, 115, 216, 0.08);
                --hero-grad-a: rgba(240, 247, 255, 0.95);
                --hero-grad-b: rgba(233, 242, 255, 0.98);
                --link-text: #12345f;
                --link-bg: #eaf2ff;
                --ring: rgba(15, 135, 201, 0.2);
            }
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: "Inter", "Segoe UI", sans-serif;
            color: var(--text);
            background:
                radial-gradient(circle at 15% 20%, var(--radial-1), transparent 30%),
                radial-gradient(circle at 85% 80%, var(--radial-2), transparent 30%),
                var(--bg);
            min-height: 100vh;
            overflow-x: hidden;
        }
        .wrap {
            max-width: 1280px;
            margin: 0 auto;
            padding: 28px 20px 60px;
        }
        .wrap > section + section { margin-top: 14px; }
        .hero, .panel {
            background: var(--bg-soft);
            border: 1px solid var(--line);
            border-radius: var(--radius);
            padding: 20px;
            min-width: 0;
        }
        .hero {
            margin-bottom: 18px;
            background: linear-gradient(140deg, var(--hero-grad-a), var(--hero-grad-b));
        }
        h1 {
            margin: 0 0 8px;
            font-size: clamp(1.4rem, 2vw, 2.1rem);
        }
        h2 { margin: 0 0 10px; font-size: 1.2rem; }
        h3 { margin: 12px 0 8px; font-size: 1.05rem; }
        p { margin: 0; color: var(--muted); line-height: 1.65; }
        .lead { color: var(--text); opacity: 0.95; }
        .nav-links {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 12px;
        }
        .nav-links a {
            color: var(--link-text);
            text-decoration: none;
            border: 1px solid var(--line);
            border-radius: 999px;
            padding: 6px 12px;
            font-size: 0.82rem;
            background: var(--link-bg);
        }
        .nav-links a:hover {
            border-color: var(--accent);
            box-shadow: 0 0 0 2px var(--ring);
        }
        .toc {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 12px;
            margin-top: 14px;
        }
        .toc a {
            display: block;
            text-decoration: none;
            color: var(--text);
            background: var(--bg-card);
            border: 1px solid var(--line);
            border-radius: 12px;
            padding: 12px;
            font-size: 0.92rem;
        }
        .stack { display: grid; gap: 14px; }
        .panel ul, .panel ol {
            margin: 8px 0 0 20px;
            color: var(--muted);
            line-height: 1.6;
        }
        .panel li { margin-bottom: 6px; }
        .refs {
            column-count: 2;
            column-gap: 18px;
            margin-top: 8px;
            list-style: decimal;
            list-style-position: outside;
            padding-left: 20px;
            font-size: 0.9rem;
            line-height: 1.45;
        }
        .refs li {
            break-inside: avoid-column;
            display: list-item;
            margin-bottom: 8px;
            color: var(--muted);
            overflow-wrap: anywhere;
            word-break: break-word;
        }
        a { color: var(--accent); overflow-wrap: anywhere; }
        .refs a {
            white-space: normal;
            overflow-wrap: anywhere;
            word-break: break-word;
            text-decoration-thickness: from-font;
        }
        .cite { text-decoration: none; font-weight: 600; margin: 0 1px; }
        .cite:hover { text-decoration: underline; }
        @media (max-width: 980px) {
            .toc { grid-template-columns: 1fr; }
            .refs { column-count: 1; }
        }
        @media (max-width: 760px) {
            .wrap { padding: 16px 12px 40px; }
            .hero, .panel { padding: 14px; }
        }
    </style>
</head>

<body>
    <main class="wrap">
        <section class="hero">
            <h1>${renderInline(parsed.title)}</h1>
            <p class="lead">${renderInline(parsed.subtitle)}</p>
            <div class="nav-links" data-shared-nav="hero"></div>
            <div class="toc">
                ${toc}
            </div>
        </section>

        <section class="stack">
${introPanel}
${panels}
            <article class="panel">
                <h2>References</h2>
                <ol class="refs">
                    ${refsHtml}
                </ol>
            </article>

            <article class="panel">
                <h2>Related Documents</h2>
                <div class="nav-links" data-shared-nav="related"></div>
            </article>
        </section>
    </main>
    <script src="./shared-nav.js"></script>
</body>

</html>
`;
}

async function buildReport() {
  const docsDir = await resolveDocsDir();
  const sourcePath = path.join(docsDir, "radiology-preprocessing-ecosystem-report.md");
  const targetPath = path.join(docsDir, "radiology-preprocessing-ecosystem-report.html");
  const markdown = await fs.readFile(sourcePath, "utf8");
  const parsed = markdownToHtml(markdown);
  const html = buildReportHtml(parsed);
  await fs.writeFile(targetPath, html, "utf8");
  process.stdout.write(`Built ${path.relative(ROOT, targetPath)} from markdown.\n`);
}

async function main() {
  const mode = process.argv[2] || "all";
  if (mode === "report" || mode === "all") {
    await buildReport();
    return;
  }
  process.stderr.write(`Unknown mode: ${mode}\n`);
  process.exit(1);
}

main().catch((err) => {
  process.stderr.write(`${err.stack || String(err)}\n`);
  process.exit(1);
});
