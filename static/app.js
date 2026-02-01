// ============================================
// Heimdallr Dashboard - JavaScript
// ============================================

const API_BASE = '';

// State
let patients = [];

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadPatients();
    // Refresh every 30 seconds
    setInterval(loadPatients, 30000);
});

// Load patient list from API
async function loadPatients() {
    try {
        const response = await fetch(`${API_BASE}/api/patients`);
        const data = await response.json();
        patients = data.patients || [];
        renderPatients();

    } catch (error) {
        console.error('Error loading patients:', error);
        showError('Erro ao carregar pacientes');
    }
}

// Render patient table
function renderPatients() {
    const tbody = document.getElementById('patients-body');

    if (patients.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7">
                    <div class="empty-state">
                        <div class="empty-state-icon">📭</div>
                        <h3>Nenhum paciente encontrado</h3>
                        <p>Arquivos NIfTI aparecerão aqui após processamento</p>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = patients.map(p => {
        // Extract just the name part (before first underscore with date)
        const displayName = p.case_id.split('_')[0];

        return `
        <tr class="${p.has_hemorrhage ? 'hemorrhage-positive' : ''}">
            <td class="patient-name">${escapeHtml(displayName)}</td>
            <td class="date">${formatDate(p.study_date)}</td>
            <td class="accession">${escapeHtml(p.accession)}</td>
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
                ? `<button class="btn btn-secondary" onclick="showResults('${escapeHtml(p.case_id)}')">📊 Resultados</button>`
                : `<button class="btn btn-disabled" disabled>Sem resultados</button>`
            }
                </div>
            </td>
        </tr>
    `;
    }).join('');
}



// Show results modal
async function showResults(caseId) {
    try {
        const response = await fetch(`${API_BASE}/api/patients/${encodeURIComponent(caseId)}/results`);
        if (!response.ok) throw new Error('Results not found');

        const results = await response.json();

        document.getElementById('modal-title').textContent = `Resultados: ${caseId}`;
        document.getElementById('modal-body').innerHTML = renderResults(results, caseId);
        document.getElementById('modal').classList.add('active');

    } catch (error) {
        console.error('Error loading results:', error);
        alert('Erro ao carregar resultados');
    }
}

// Render results in modal
function renderResults(results, caseId) {
    const sections = [];

    // Basic Info
    sections.push(`
        <h3 style="margin-bottom: 1rem; color: var(--text-secondary);">Informações Gerais</h3>
        <div class="results-grid">
            <div class="result-card">
                <div class="result-label">Modalidade</div>
                <div class="result-value">${results.modality || '-'}</div>
            </div>
            <div class="result-card">
                <div class="result-label">Regiões do Corpo</div>
                <div class="result-value">${(results.body_regions || []).join(', ') || '-'}</div>
            </div>
        </div>
    `);

    // Hemorrhage (if present)
    if (results.hemorrhage_vol_cm3 !== undefined && results.hemorrhage_vol_cm3 > 0) {
        sections.push(`
            <h3 style="margin: 1.5rem 0 1rem; color: var(--danger);">⚠️ Hemorragia Detectada</h3>
            <div class="results-grid">
                <div class="result-card">
                    <div class="result-label">Volume</div>
                    <div class="result-value danger">${results.hemorrhage_vol_cm3.toFixed(1)} <span class="result-unit">cm³</span></div>
                </div>
            </div>
        `);
    }

    // Sarcopenia (L3)
    if (results.SMA_cm2 !== undefined) {
        sections.push(`
            <h3 style="margin: 1.5rem 0 1rem; color: var(--text-secondary);">Análise de Sarcopenia (L3)</h3>
            <div class="results-grid">
                <div class="result-card">
                    <div class="result-label">Área Muscular (SMA)</div>
                    <div class="result-value highlight">${results.SMA_cm2.toFixed(2)} <span class="result-unit">cm²</span></div>
                </div>
                <div class="result-card">
                    <div class="result-label">Densidade Muscular</div>
                    <div class="result-value">${results.muscle_HU_mean?.toFixed(1) || '-'} <span class="result-unit">HU</span></div>
                </div>
                <div class="result-card">
                    <div class="result-label">Fatia L3</div>
                    <div class="result-value">${results.slice_L3 || '-'}</div>
                </div>
            </div>
        `);
    }

    // Organs
    const organs = [
        { key: 'liver', name: 'Fígado', icon: '🫀' },
        { key: 'spleen', name: 'Baço', icon: '🩸' },
        { key: 'kidney_right', name: 'Rim Direito', icon: '🫘' },
        { key: 'kidney_left', name: 'Rim Esquerdo', icon: '🫘' }
    ];

    const organCards = organs.map(organ => {
        const vol = results[`${organ.key}_vol_cm3`];
        const hu = results[`${organ.key}_hu_mean`];

        if (vol === undefined || vol === 0) return '';

        return `
            <div class="result-card">
                <div class="result-label">${organ.icon} ${organ.name}</div>
                <div class="result-value">${vol.toFixed(1)} <span class="result-unit">cm³</span></div>
                ${hu !== null && hu !== undefined ? `<div style="font-size: 0.85rem; color: var(--text-secondary); margin-top: 0.25rem;">${hu.toFixed(1)} HU</div>` : ''}
            </div>
        `;
    }).filter(Boolean);

    if (organCards.length > 0) {
        sections.push(`
            <h3 style="margin: 1.5rem 0 1rem; color: var(--text-secondary);">Volumetria de Órgãos</h3>
            <div class="results-grid">
                ${organCards.join('')}
            </div>
        `);
    }

    if (results.images && results.images.length > 0) {
        const imageCards = results.images.map(img => {
            const url = `/api/patients/${encodeURIComponent(caseId)}/images/${encodeURIComponent(img)}`;
            let label = img.replace(/_/g, ' ').replace('.png', '');
            // Capitalize
            label = label.charAt(0).toUpperCase() + label.slice(1);

            return `
                <div class="result-card" style="width: 100%; text-align: center;">
                    <div class="result-label" style="text-align: center;">${label}</div>
                    <img src="${url}" alt="${img}" style="max-width: 100%; border-radius: 8px; margin-top: 10px; border: 1px solid #333;">
                </div>
            `;
        }).join('');

        sections.push(`
            <h3 style="margin: 1.5rem 0 1rem; color: var(--text-secondary);">Visualizações</h3>
            <div class="results-grid" style="grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));">
                ${imageCards}
            </div>
        `);
    }

    return sections.join('');
}

// Close modal
function closeModal() {
    document.getElementById('modal').classList.remove('active');
}

// Close modal on backdrop click
document.getElementById('modal')?.addEventListener('click', (e) => {
    if (e.target.id === 'modal') {
        closeModal();
    }
});

// Close modal on Escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeModal();
    }
});

// Utility: Format date YYYYMMDD -> DD/MM/YYYY
function formatDate(dateStr) {
    if (!dateStr || dateStr.length !== 8) return dateStr || '-';
    return `${dateStr.slice(6, 8)}/${dateStr.slice(4, 6)}/${dateStr.slice(0, 4)}`;
}

// Utility: Escape HTML
function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// Utility: Show error
function showError(message) {
    const tbody = document.getElementById('patients-body');
    tbody.innerHTML = `
        <tr>
            <td colspan="7" class="loading" style="color: var(--danger);">
                ❌ ${escapeHtml(message)}
            </td>
        </tr>
    `;
}
