/*
 * Copyright (c) 2026 Rodrigo Americo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ============================================
// Heimdallr Dashboard - JavaScript
// ============================================

const API_BASE = '';

// State
let patients = [];
let currentFilter = '';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadPatients();
    // Refresh every 30 seconds
    setInterval(loadPatients, 30000);

    // Filter input
    document.getElementById('searchInput').addEventListener('input', (e) => {
        currentFilter = e.target.value.toLowerCase();
        renderPatients();
    });
});

// Load patient list from API
async function loadPatients() {
    try {
        const response = await fetch(`${API_BASE}/api/patients`);
        const data = await response.json();
        patients = data.patients || [];

        // Sort alphabetically by Patient Name
        patients.sort((a, b) => {
            const nameA = a.case_id.split('_')[0].toLowerCase();
            const nameB = b.case_id.split('_')[0].toLowerCase();
            return nameA.localeCompare(nameB);
        });

        renderPatients();

    } catch (error) {
        console.error('Error loading patients:', error);
        showError('Erro ao carregar pacientes');
    }
}

// Render patient table
function renderPatients() {
    const tbody = document.getElementById('patients-body');

    // Filter list
    const filtered = patients.filter(p => {
        if (!currentFilter) return true;

        const name = p.case_id.split('_')[0].toLowerCase();
        const accession = (p.accession || '').toLowerCase();
        const date = (p.study_date || '').toLowerCase();

        return name.includes(currentFilter) ||
            accession.includes(currentFilter) ||
            date.includes(currentFilter);
    });

    if (filtered.length === 0) {
        // Show proper empty state depending on whether it's filter or no-data
        const message = patients.length === 0 ? "Nenhum paciente encontrado" : "Nenhum resultado para a busca";
        tbody.innerHTML = `
            <tr>
                <td colspan="6">
                    <div class="empty-state">
                        <div class="empty-state-icon">🔍</div>
                        <h3>${message}</h3>
                        <p>${patients.length === 0 ? "Arquivos NIfTI aparecerão aqui após processamento" : "Tente ajustar o termo pesquisado"}</p>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = filtered.map(p => {
        // Extract just the name part (before first underscore with date)
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

        // Also fetch metadata to get biometric data
        let metadata = {};
        try {
            const metaResponse = await fetch(`${API_BASE}/api/patients/${encodeURIComponent(caseId)}/metadata`);
            if (metaResponse.ok) {
                metadata = await metaResponse.json();
            }
        } catch (err) {
            console.warn('Could not load metadata:', err);
        }

        document.getElementById('modal-title').textContent = `Resultados: ${caseId}`;
        document.getElementById('modal-body').innerHTML = renderResults(results, caseId, metadata);
        document.getElementById('modal').classList.add('active');

    } catch (error) {
        console.error('Error loading results:', error);
        alert('Erro ao carregar resultados');
    }
}

// Render results in modal
function renderResults(results, caseId, metadata = {}) {
    const sections = [];

    // Biometric Data Section (at the top)
    sections.push(renderBiometricSection(caseId, metadata, results));

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

    // Hemorrhage (if present and > 0.1)
    if (results.hemorrhage_vol_cm3 !== undefined && results.hemorrhage_vol_cm3 > 0.1) {
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
        let sarcopeniaStatusHtml = '';

        if (metadata.Height && metadata.Sex) {
            const smi = results.SMA_cm2 / (metadata.Height * metadata.Height);
            let hasSarcopenia = false;

            if (metadata.Sex === 'M' || metadata.Sex === 'Masculino') {
                hasSarcopenia = smi <= 52.4;
            } else if (metadata.Sex === 'F' || metadata.Sex === 'Feminino') {
                hasSarcopenia = smi <= 38.5;
            }

            if (metadata.Sex === 'M' || metadata.Sex === 'F' || metadata.Sex === 'Masculino' || metadata.Sex === 'Feminino') {
                const badgeColor = hasSarcopenia ? 'var(--danger, #ef4444)' : 'var(--success, #22c55e)';
                const badgeEmoji = hasSarcopenia ? '🔴' : '🟢';
                const statusText = hasSarcopenia ? 'Sarcopenia Detectada' : 'Normal';

                sarcopeniaStatusHtml = `
                    <div class="result-card">
                        <div class="result-label">Classificação (Prado et al.)</div>
                        <div class="result-value">
                            <span style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 999px; background: ${badgeColor}22; color: ${badgeColor}; border: 1px solid ${badgeColor}; font-weight: 600; font-size: 0.85rem;">
                                ${badgeEmoji} ${statusText}
                            </span>
                        </div>
                    </div>
                `;
            }
        }

        sections.push(`
            <h3 style="margin: 1.5rem 0 1rem; color: var(--text-secondary);">Análise de Sarcopenia (L3)</h3>
            <div class="results-grid">
                ${sarcopeniaStatusHtml}
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

    // BMD / Bone Mineral Density (L1)
    if (results.L1_bmd_classification) {
        const bmdClass = results.L1_bmd_classification;
        let badgeColor, badgeEmoji;
        if (bmdClass === 'Normal') {
            badgeColor = 'var(--success, #22c55e)';
            badgeEmoji = '🟢';
        } else if (bmdClass === 'Osteopenia') {
            badgeColor = '#eab308';
            badgeEmoji = '🟡';
        } else {
            badgeColor = 'var(--danger, #ef4444)';
            badgeEmoji = '🔴';
        }

        sections.push(`
            <h3 style="margin: 1.5rem 0 1rem; color: var(--text-secondary);">Análise de Osteoporose (L1)</h3>
            <div class="results-grid">
                <div class="result-card">
                    <div class="result-label">Classificação — Pickhardt et al., Radiology 2013</div>
                    <div class="result-value">
                        <span style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 999px; background: ${badgeColor}22; color: ${badgeColor}; border: 1px solid ${badgeColor}; font-weight: 600; font-size: 0.85rem;">
                            ${badgeEmoji} ${bmdClass}
                        </span>
                    </div>
                </div>
                <div class="result-card">
                    <div class="result-label">HU Trabecular (L1)</div>
                    <div class="result-value">${results.L1_trabecular_HU_mean?.toFixed(1) || '-'} ± ${results.L1_trabecular_HU_std?.toFixed(1) || '-'} <span class="result-unit">HU</span></div>
                </div>
                <div class="result-card">
                    <div class="result-label">Voxels Analisados</div>
                    <div class="result-value">${(results.L1_trabecular_voxel_count || 0).toLocaleString()}</div>
                </div>
            </div>
        `);
    }

    // Liver Section (Volume + Fat Content)
    const liverVol = results.liver_vol_cm3;
    const liverHU = results.liver_hu_mean;
    const hasPDFF = results.liver_pdff_percent !== undefined;

    if (liverVol > 0 || hasPDFF) {
        let liverCards = [];

        // Liver Volume Card
        if (liverVol > 0) {
            liverCards.push(`
                <div class="result-card" style="padding-bottom: 0.75rem;">
                    <div class="result-label" style="text-transform: uppercase; margin-bottom: 1.25rem; font-size: 0.7rem; border-bottom: 1px solid var(--border-subtle); padding-bottom: 0.5rem; display: block;">Volume Total do Fígado</div>
                    <div class="result-value" style="font-size: 1.8rem; line-height: 1;">${liverVol.toFixed(1)} <span class="result-unit">cm³</span></div>
                    ${liverHU !== null && liverHU !== undefined ? `<div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.5rem;">Densidade Média: ${liverHU.toFixed(1)} HU</div>` : ''}
                </div>
            `);
        }

        // Liver Fat Content Card
        if (hasPDFF) {
            let kvpBadge = '';
            const pdff = results.liver_pdff_percent;
            const kvp = results.liver_pdff_kvp;

            // New Thresholds for PDFF (2026-02-24)
            let pdffColor = "var(--text-primary, #fff)"; // Default
            if (pdff < 5.0) {
                pdffColor = "var(--success, #22c55e)"; // Normal
            } else if (pdff < 15.0) {
                pdffColor = "var(--warning, #eab308)"; // Leve
            } else if (pdff < 30.0) {
                pdffColor = "#f97316"; // Moderado (Orange)
            } else {
                pdffColor = "var(--danger, #ef4444)"; // Acentuado
            }

            if (kvp === "120" || kvp === "120.0") {
                kvpBadge = `<span style="display: inline-block; padding: 0.2rem 0.6rem; border-radius: 6px; background: var(--success, #06d6a0)15; color: var(--success, #06d6a0); border: 1px solid var(--success, #06d6a0); font-size: 0.75rem; font-weight: 600; white-space: nowrap;">120 KV</span>`;
            } else if (kvp === "Unknown") {
                kvpBadge = `<span title="KV Desconhecido" style="display: inline-block; padding: 0.2rem 0.4rem; border-radius: 6px; background: var(--warning, #ff8c42)15; color: var(--warning, #ff8c42); border: 1px solid var(--warning, #ff8c42); font-size: 0.75rem; font-weight: 600; white-space: nowrap;">❓ KV</span>`;
            } else {
                kvpBadge = `<span title="Cálculo idealizado para 120 kV" style="display: inline-block; padding: 0.2rem 0.6rem; border-radius: 6px; background: var(--warning, #ff8c42)15; color: var(--warning, #ff8c42); border: 1px solid var(--warning, #ff8c42); font-size: 0.75rem; font-weight: 600; white-space: nowrap;">⚠️ ${kvp} KV</span>`;
            }

            liverCards.push(`
                <div class="result-card" style="padding-bottom: 0.75rem;">
                    <div class="result-label" style="text-transform: uppercase; margin-bottom: 1.25rem; font-size: 0.7rem; border-bottom: 1px solid var(--border-subtle); padding-bottom: 0.5rem; display: block;">Conteúdo de Gordura Hepática Estimado</div>
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div style="flex: 1;">
                            <div class="result-value" style="font-size: 1.8rem; line-height: 1; color: ${pdffColor};">${pdff.toFixed(1)} <span class="result-unit">%</span></div>
                            <div style="font-size: 0.75rem; font-weight: 600; color: var(--text-secondary); margin-top: 0.4rem; opacity: 0.9;">MRI-PDFF Equivalente</div>
                        </div>
                        <div style="margin-left: 1rem; flex-shrink: 0;">
                            ${kvpBadge}
                        </div>
                    </div>
                    <div style="font-size: 0.65rem; color: var(--text-secondary); margin-top: 1.5rem; opacity: 0.6; font-style: italic;">Pickhardt et al., AJR 2018</div>
                </div>
            `);
        }

        sections.push(`
            <h3 style="margin: 1.5rem 0 1rem; color: var(--text-secondary);">Fígado</h3>
            <div class="results-grid">
                ${liverCards.join('')}
            </div>
        `);
    }

    // Pulmonary Emphysema (Lobar)
    if (results.lung_analysis_status === "Complete") {
        const lobes = [
            { key: 'lung_upper_lobe_right', name: 'Lobo Superior Direito' },
            { key: 'lung_middle_lobe_right', name: 'Lobo Médio' },
            { key: 'lung_lower_lobe_right', name: 'Lobo Inferior Direito' },
            { key: 'lung_upper_lobe_left', name: 'Lobo Superior Esquerdo' },
            { key: 'lung_lower_lobe_left', name: 'Lobo Inferior Esquerdo' }
        ];

        const getProgressColor = (perc) => {
            if (perc < 5) return 'var(--success, #22c55e)';  // Normal
            if (perc < 15) return '#eab308';                // Mild (Amarelo)
            if (perc < 25) return '#f97316';                // Moderate (Laranja)
            return 'var(--danger, #ef4444)';                // Severe (Vermelho)
        };

        const lobarHtml = lobes.map(lobe => {
            const perc = results[`${lobe.key}_emphysema_percent`];
            const volTotal = results[`${lobe.key}_vol_cm3`];
            const volEmph = results[`${lobe.key}_emphysema_vol_cm3`];
            const color = getProgressColor(perc);

            return `
                <div style="margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid rgba(255,255,255,0.03);">
                    <div style="display: flex; justify-content: space-between; font-size: 0.85rem; margin-bottom: 0.4rem; color: #eee;">
                        <span style="font-weight: 500;">${lobe.name}</span>
                        <span style="font-weight: 600; color: ${color};">${perc.toFixed(1)}%</span>
                    </div>
                    <div style="height: 8px; background: rgba(255,255,255,0.05); border-radius: 4px; overflow: hidden; margin-bottom: 0.4rem;">
                        <div style="height: 100%; width: ${Math.min(100, perc)}%; background: ${color}; box-shadow: 0 0 10px ${color}44;"></div>
                    </div>
                    <div style="display: flex; justify-content: space-between; font-size: 0.7rem; color: var(--text-secondary); opacity: 0.8;">
                        <span>Vol. Total: ${volTotal ? volTotal.toFixed(0) : '-'} cm³</span>
                        <span>Vol. Enfisema: ${volEmph ? volEmph.toFixed(0) : '-'} cm³</span>
                    </div>
                </div>
            `;
        }).join('');

        sections.push(`
            <h3 style="margin: 1.5rem 0 1rem; color: var(--text-secondary);">Enfisema Pulmonar Quantitativo</h3>
            <div class="results-grid" style="grid-template-columns: 1fr 1.6fr; gap: 1.5rem;">
                <div class="result-card" style="display: flex; flex-direction: column; justify-content: center; align-items: center; background: linear-gradient(145deg, rgba(30,30,50,0.4), rgba(20,20,35,0.4)); padding: 1.5rem;">
                    <div class="result-label" style="text-align: center; margin-bottom: 1.5rem; font-size: 0.8rem; letter-spacing: 0.05em;">LAA TOTAL (-950 HU)</div>
                    <div style="position: relative; width: 120px; height: 120px; display: flex; align-items: center; justify-content: center; margin-bottom: 1.5rem;">
                         <svg viewBox="0 0 36 36" style="width: 100%; height: 100%; transform: rotate(-90deg);">
                            <path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831" fill="none" stroke="rgba(255,255,255,0.05)" stroke-width="2.5" />
                            <path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831" fill="none" stroke="${getProgressColor(results.total_lung_emphysema_percent)}" stroke-width="3" stroke-dasharray="${results.total_lung_emphysema_percent}, 100" stroke-linecap="round" />
                        </svg>
                        <div style="position: absolute; text-align: center;">
                            <div style="font-size: 1.8rem; font-weight: 700; color: #eee; line-height: 1;">${results.total_lung_emphysema_percent.toFixed(1)}<span style="font-size: 1rem; margin-left:1px;">%</span></div>
                        </div>
                    </div>
                    <div style="width: 100%; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 1rem; text-align: center;">
                        <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.3rem;">Volume Pulmonar: <span style="color: #eee; font-weight: 500;">${results.total_lung_vol_cm3?.toFixed(0) || '-'} cm³</span></div>
                        <div style="font-size: 0.75rem; color: var(--text-secondary);">Carga de Enfisema: <span style="color: #eee; font-weight: 500;">${results.total_lung_emphysema_vol_cm3?.toFixed(0) || '-'} cm³</span></div>
                    </div>
                </div>
                <div class="result-card" style="padding: 1.5rem; background: rgba(15,15,25,0.3);">
                    ${lobarHtml}
                </div>
            </div>
        `);
    }

    const organs = [
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
            <div class="results-grid" style="grid-template-columns: 1fr;">
                ${imageCards}
            </div>
        `);
    }

    return sections.join('');
}

// Render biometric data section with BMI and SMI calculations
function renderBiometricSection(caseId, metadata, results) {
    const weight = metadata.Weight || null;
    const height = metadata.Height || null;
    const sma = results.SMA_cm2 || null;

    // Calculate BMI if both weight and height are available
    let bmi = null;
    if (weight && height) {
        bmi = (weight / (height * height)).toFixed(1);
    }

    // Calculate SMI if both height and SMA are available
    let smi = null;
    if (height && sma) {
        smi = (sma / (height * height)).toFixed(2);
    }

    return `
        <h3 style="margin-bottom: 1rem; color: var(--text-secondary);">Dados Biométricos</h3>
        <div class="results-grid" id="biometric-section">
            <div class="result-card">
                <div class="result-label">Peso</div>
                <div class="result-value" id="weight-display">${weight ? weight + ' <span class="result-unit">kg</span>' : '<span style="color: var(--text-secondary);">Não informado</span>'}</div>
                <input type="number" id="weight-input" step="0.1" min="1" max="500" placeholder="Ex: 75.5" style="display: none; margin-top: 0.5rem; padding: 0.5rem; border-radius: 4px; border: 1px solid #333; background: #1a1a2e; color: #eee; width: 100%;" value="${weight || ''}">
            </div>
            <div class="result-card">
                <div class="result-label">Altura</div>
                <div class="result-value" id="height-display">${height ? height + ' <span class="result-unit">m</span>' : '<span style="color: var(--text-secondary);">Não informado</span>'}</div>
                <input type="number" id="height-input" step="0.01" min="0.5" max="3.0" placeholder="Ex: 1.75" style="display: none; margin-top: 0.5rem; padding: 0.5rem; border-radius: 4px; border: 1px solid #333; background: #1a1a2e; color: #eee; width: 100%;" value="${height || ''}">
            </div>
            ${bmi ? `
            <div class="result-card">
                <div class="result-label">IMC</div>
                <div class="result-value highlight" id="bmi-display">${bmi} <span class="result-unit">kg/m²</span></div>
            </div>
            ` : ''}
            ${smi ? `
            <div class="result-card">
                <div class="result-label">SMI (Índice Músculo-Esquelético)</div>
                <div class="result-value highlight" id="smi-display">${smi} <span class="result-unit">cm²/m²</span></div>
            </div>
            ` : ''}
        </div>
        <div style="margin-top: 1rem; text-align: right;">
            <button class="btn btn-secondary" id="edit-biometrics-btn" onclick="toggleBiometricEdit()">✏️ Editar</button>
            <button class="btn btn-primary" id="save-biometrics-btn" onclick="saveBiometrics('${escapeHtml(caseId)}')", style="display: none;">💾 Salvar</button>
            <button class="btn btn-secondary" id="cancel-biometrics-btn" onclick="toggleBiometricEdit()", style="display: none;">❌ Cancelar</button>
        </div>
        <div id="biometric-message" style="margin-top: 0.5rem; text-align: center; font-size: 0.9rem;"></div>
    `;
}

// Toggle biometric edit mode
function toggleBiometricEdit() {
    const weightDisplay = document.getElementById('weight-display');
    const heightDisplay = document.getElementById('height-display');
    const weightInput = document.getElementById('weight-input');
    const heightInput = document.getElementById('height-input');
    const editBtn = document.getElementById('edit-biometrics-btn');
    const saveBtn = document.getElementById('save-biometrics-btn');
    const cancelBtn = document.getElementById('cancel-biometrics-btn');

    const isEditing = weightInput.style.display !== 'none';

    if (isEditing) {
        // Cancel editing - hide inputs, show displays
        weightInput.style.display = 'none';
        heightInput.style.display = 'none';
        weightDisplay.style.display = 'block';
        heightDisplay.style.display = 'block';
        editBtn.style.display = 'inline-block';
        saveBtn.style.display = 'none';
        cancelBtn.style.display = 'none';
    } else {
        // Start editing - show inputs, hide displays
        weightInput.style.display = 'block';
        heightInput.style.display = 'block';
        weightDisplay.style.display = 'none';
        heightDisplay.style.display = 'none';
        editBtn.style.display = 'none';
        saveBtn.style.display = 'inline-block';
        cancelBtn.style.display = 'inline-block';

        // Focus on first empty field
        if (!weightInput.value) {
            weightInput.focus();
        } else if (!heightInput.value) {
            heightInput.focus();
        }
    }
}

// Save biometric data
async function saveBiometrics(caseId) {
    const weightInput = document.getElementById('weight-input');
    const heightInput = document.getElementById('height-input');
    const messageDiv = document.getElementById('biometric-message');

    const weight = parseFloat(weightInput.value);
    const height = parseFloat(heightInput.value);

    // Validation
    if (!weight || weight <= 0 || weight > 500) {
        messageDiv.innerHTML = '<span style="color: var(--danger);">❌ Peso inválido (deve estar entre 1 e 500 kg)</span>';
        return;
    }

    if (!height || height <= 0 || height > 3.0) {
        messageDiv.innerHTML = '<span style="color: var(--danger);">❌ Altura inválida (deve estar entre 0.5 e 3.0 m)</span>';
        return;
    }

    try {
        messageDiv.innerHTML = '<span style="color: var(--text-secondary);">⏳ Salvando...</span>';

        const response = await fetch(`${API_BASE}/api/patients/${encodeURIComponent(caseId)}/biometrics`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ weight, height })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Erro ao salvar');
        }

        const result = await response.json();

        // Update displays
        const weightDisplay = document.getElementById('weight-display');
        const heightDisplay = document.getElementById('height-display');
        const bmiDisplay = document.getElementById('bmi-display');

        weightDisplay.innerHTML = `${weight} <span class="result-unit">kg</span>`;
        heightDisplay.innerHTML = `${height} <span class="result-unit">m</span>`;

        // Update or create BMI display
        if (bmiDisplay) {
            bmiDisplay.innerHTML = `${result.bmi} <span class="result-unit">kg/m²</span>`;
        } else {
            // Add BMI card if it didn't exist
            const biometricSection = document.getElementById('biometric-section');
            const bmiCard = document.createElement('div');
            bmiCard.className = 'result-card';
            bmiCard.innerHTML = `
                <div class="result-label">IMC</div>
                <div class="result-value highlight" id="bmi-display">${result.bmi} <span class="result-unit">kg/m²</span></div>
            `;
            biometricSection.appendChild(bmiCard);
        }

        // Update SMI if SMA is available
        const smiDisplay = document.getElementById('smi-display');
        // Get SMA from the page if it exists
        const smaElements = document.querySelectorAll('.result-value');
        let smaValue = null;
        smaElements.forEach(el => {
            const text = el.textContent;
            if (text.includes('cm²') && el.closest('.result-card')?.querySelector('.result-label')?.textContent.includes('SMA')) {
                smaValue = parseFloat(text);
            }
        });

        if (smaValue && height) {
            const smi = (smaValue / (height * height)).toFixed(2);
            if (smiDisplay) {
                smiDisplay.innerHTML = `${smi} <span class="result-unit">cm²/m²</span>`;
            } else {
                // Add SMI card if it didn't exist
                const biometricSection = document.getElementById('biometric-section');
                const smiCard = document.createElement('div');
                smiCard.className = 'result-card';
                smiCard.innerHTML = `
                    <div class="result-label">SMI (Índice Músculo-Esquelético)</div>
                    <div class="result-value highlight" id="smi-display">${smi} <span class="result-unit">cm²/m²</span></div>
                `;
                biometricSection.appendChild(smiCard);
            }

            // Save SMI to resultados.json and database
            try {
                const smiResponse = await fetch(`${API_BASE}/api/patients/${encodeURIComponent(caseId)}/smi`, {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ smi: parseFloat(smi) })
                });

                if (smiResponse.ok) {
                    console.log('SMI saved successfully to resultados.json and database');
                } else {
                    console.warn('Failed to save SMI:', await smiResponse.text());
                }
            } catch (smiError) {
                console.error('Error saving SMI:', smiError);
                // Don't fail the entire operation if SMI save fails
            }
        }

        // Exit edit mode
        toggleBiometricEdit();

        messageDiv.innerHTML = '<span style="color: var(--success);">✅ Dados salvos com sucesso!</span>';
        setTimeout(() => {
            messageDiv.innerHTML = '';
        }, 3000);

    } catch (error) {
        console.error('Error saving biometrics:', error);
        messageDiv.innerHTML = `<span style="color: var(--danger);">❌ Erro: ${escapeHtml(error.message)}</span>`;
    }
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
