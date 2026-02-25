import { getFilteredPatients, state } from '../state.js';
import { formatDate, escapeHtml } from '../utils.js';
import { showResults } from './ResultsModal.js';

export function renderPatients() {
    const tbody = document.getElementById('patients-body');
    const filtered = getFilteredPatients().sort((a, b) => {
        const nameA = a.case_id.split('_')[0].toLowerCase();
        const nameB = b.case_id.split('_')[0].toLowerCase();
        return nameA.localeCompare(nameB);
    });

    if (filtered.length === 0) {
        const message = state.patients.length === 0 ? "Nenhum paciente encontrado" : "Nenhum resultado para a busca";
        tbody.innerHTML = `
            <tr>
                <td colspan="6">
                    <div class="empty-state">
                        <div class="empty-state-icon">🔍</div>
                        <h3>${message}</h3>
                        <p>${state.patients.length === 0 ? "Arquivos NIfTI aparecerão aqui após processamento" : "Tente ajustar o termo pesquisado"}</p>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = filtered.map(p => {
        const displayName = p.case_id.split('_')[0];

        return `
        <tr class="${p.has_hemorrhage ? 'hemorrhage-positive' : ''}">
            <td class="patient-name">${escapeHtml(displayName)}</td>
            <td class="date">${formatDate(p.study_date)}</td>
            <td class="accession">${escapeHtml(p.accession)}</td>
            <td class="processing-time">${p.elapsed_seconds ? p.elapsed_seconds + 's' : '-'}</td>
            <td><span class="modality ${p.modality}">${p.modality || '-'}</span></td>
            <td class="regions">${p.body_regions.map(r => `<span class="region-tag">${r}</span>`).join('')}</td>
            <td>
                <div class="actions">
                    <div class="dropdown">
                        <button class="btn btn-primary">⬇ Downloads</button>
                        <div class="dropdown-content">
                            ${p.has_hemorrhage ? `<a href="/api/patients/${encodeURIComponent(p.case_id)}/download/bleed" download>🔴 Bleed (ZIP)</a>` : ''}
                            <a href="/api/patients/${encodeURIComponent(p.case_id)}/download/tissue_types" download>🧠 Tissue Types (ZIP)</a>
                            <a href="/api/patients/${encodeURIComponent(p.case_id)}/download/total" download>🦴 Total (ZIP)</a>
                            <a href="/api/patients/${encodeURIComponent(p.case_id)}/nifti" download="${p.filename}">📄 Original NIfTI</a>
                        </div>
                    </div>
                    ${p.has_results
                ? `<button class="btn btn-secondary result-btn" data-id="${escapeHtml(p.case_id)}">📊 Resultados</button>`
                : `<button class="btn btn-disabled" disabled>Sem resultados</button>`
            }
                </div>
            </td>
        </tr>
    `;
    }).join('');

    // Attach event listeners to result buttons
    document.querySelectorAll('.result-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const caseId = e.target.getAttribute('data-id');
            showResults(caseId);
        });
    });
}
