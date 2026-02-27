import { fetchResults, fetchMetadata } from '../api.js';
import { renderBiometricSection } from './BiometricsCard.js';
import { renderLiverSection } from './LiverCard.js';
import { getProgressColor, escapeHtml } from '../utils.js';

export async function showResults(caseId) {
    try {
        const [results, metadata] = await Promise.all([
            fetchResults(caseId),
            fetchMetadata(caseId).catch(() => ({}))
        ]);

        document.getElementById('modal-title').textContent = `Resultados: ${caseId}`;
        document.getElementById('modal-body').innerHTML = renderResults(results, caseId, metadata);
        document.getElementById('modal').classList.add('active');

    } catch (error) {
        console.error('Error loading results:', error);
        alert('Erro ao carregar resultados');
    }
}

function renderResults(results, caseId, metadata = {}) {
    const sections = [];

    sections.push(renderBiometricSection(caseId, metadata, results));

    sections.push(`
        <h3 class="section-title">Informações Gerais</h3>
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

    if (results.hemorrhage_vol_cm3 !== undefined && results.hemorrhage_vol_cm3 > 0.1) {
        sections.push(`
            <h3 class="section-title text-danger">⚠️ Hemorragia Detectada</h3>
            <div class="results-grid">
                <div class="result-card">
                    <div class="result-label">Volume</div>
                    <div class="result-value text-danger">${results.hemorrhage_vol_cm3.toFixed(1)} <span class="result-unit">cm³</span></div>
                </div>
            </div>
        `);
    }

    if (results.SMA_cm2 !== undefined) {
        let sarcopeniaStatusHtml = '';

        if (metadata.Height && metadata.Sex) {
            const smi = results.SMA_cm2 / (metadata.Height * metadata.Height);
            let hasSarcopenia = false;

            if (metadata.Sex === 'M' || metadata.Sex === 'Masculino') hasSarcopenia = smi <= 52.4;
            else if (metadata.Sex === 'F' || metadata.Sex === 'Feminino') hasSarcopenia = smi <= 38.5;

            if (['M', 'F', 'Masculino', 'Feminino'].includes(metadata.Sex)) {
                const badgeClass = hasSarcopenia ? 'badge-danger' : 'badge-success';
                const badgeEmoji = hasSarcopenia ? '🔴' : '🟢';
                const statusText = hasSarcopenia ? 'Sarcopenia Detectada' : 'Normal';

                sarcopeniaStatusHtml = `
                    <div class="result-card">
                        <div class="result-label">Classificação (Prado et al.)</div>
                        <div class="result-value">
                            <span class="status-badge ${badgeClass}">${badgeEmoji} ${statusText}</span>
                        </div>
                    </div>
                `;
            }
        }

        sections.push(`
            <h3 class="section-title">Análise de Sarcopenia (L3)</h3>
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

    if (results.L1_bmd_classification) {
        const bmdClass = results.L1_bmd_classification;
        let badgeClass = 'badge-success';
        let badgeEmoji = '🟢';
        if (bmdClass === 'Osteopenia') { badgeClass = 'badge-warning'; badgeEmoji = '🟡'; }
        else if (bmdClass !== 'Normal') { badgeClass = 'badge-danger'; badgeEmoji = '🔴'; }

        sections.push(`
            <h3 class="section-title">Análise de Osteoporose (L1)</h3>
            <div class="results-grid">
                <div class="result-card">
                    <div class="result-label">Classificação — Pickhardt et al., Radiology 2013</div>
                    <div class="result-value">
                        <span class="status-badge ${badgeClass}">${badgeEmoji} ${bmdClass}</span>
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

    const liverSection = renderLiverSection(results);
    if (liverSection) sections.push(liverSection);

    if (results.lung_analysis_status === "Complete") {
        const lobes = [
            { key: 'lung_upper_lobe_right', name: 'Lobo Superior Direito' },
            { key: 'lung_middle_lobe_right', name: 'Lobo Médio' },
            { key: 'lung_lower_lobe_right', name: 'Lobo Inferior Direito' },
            { key: 'lung_upper_lobe_left', name: 'Lobo Superior Esquerdo' },
            { key: 'lung_lower_lobe_left', name: 'Lobo Inferior Esquerdo' }
        ];

        const lobarHtml = lobes.map(lobe => {
            const perc = results[`${lobe.key}_emphysema_percent`];
            const color = getProgressColor(perc);

            return `
                <div class="lobe-row">
                    <div class="lobe-header">
                        <span>${lobe.name}</span>
                        <span style="color: ${color};">${perc.toFixed(1)}%</span>
                    </div>
                    <div class="lobe-bar-bg">
                        <div class="lobe-bar-fill" style="width: ${Math.min(100, perc)}%; background: ${color}; box-shadow: 0 0 10px ${color}44;"></div>
                    </div>
                    <div class="lobe-footer">
                        <span>Vol. Total: ${results[`${lobe.key}_vol_cm3`]?.toFixed(0) || '-'} cm³</span>
                        <span>Vol. Enfisema: ${results[`${lobe.key}_emphysema_vol_cm3`]?.toFixed(0) || '-'} cm³</span>
                    </div>
                </div>
            `;
        }).join('');

        sections.push(`
            <h3 class="section-title">Enfisema Pulmonar Quantitativo</h3>
            <div class="results-grid lung-grid">
                <div class="result-card lung-total-card">
                    <div class="lung-label">LAA<sub>I-950</sub></div>
                    <div class="lung-chart-container">
                         <svg viewBox="0 0 36 36" class="lung-svg">
                            <path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831" class="lung-bg" />
                            <path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831" class="lung-fill" stroke="${getProgressColor(results.total_lung_emphysema_percent)}" stroke-dasharray="${results.total_lung_emphysema_percent}, 100" />
                        </svg>
                        <div class="lung-percentage">${results.total_lung_emphysema_percent.toFixed(1)}<span>%</span></div>
                    </div>
                    <div class="lung-footer">
                        <div>Volume Pulmonar: <span>${results.total_lung_vol_cm3?.toFixed(0) || '-'} cm³</span></div>
                        <div>Carga de Enfisema: <span>${results.total_lung_emphysema_vol_cm3?.toFixed(0) || '-'} cm³</span></div>
                    </div>
                </div>
                <div class="result-card lobes-card">
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
        if (!vol) return '';

        return `
            <div class="result-card">
                <div class="result-label">${organ.icon} ${organ.name}</div>
                <div class="result-value">${vol.toFixed(1)} <span class="result-unit">cm³</span></div>
                ${hu !== null && hu !== undefined ? `<div class="organ-hu">${hu.toFixed(1)} HU</div>` : ''}
            </div>
        `;
    }).filter(Boolean);

    if (organCards.length > 0) {
        sections.push(`
            <h3 class="section-title">Volumetria de Órgãos</h3>
            <div class="results-grid">
                ${organCards.join('')}
            </div>
        `);
    }

    if (results.images && results.images.length > 0) {
        const imageCards = results.images.map(img => {
            const url = `/api/patients/${encodeURIComponent(caseId)}/images/${encodeURIComponent(img)}`;
            let label = img.replace(/_/g, ' ').replace('.png', '');
            label = label.charAt(0).toUpperCase() + label.slice(1);

            return `
                <div class="result-card img-card">
                    <div class="result-label text-center">${label}</div>
                    <img src="${url}" alt="${img}">
                </div>
            `;
        }).join('');

        sections.push(`
            <h3 class="section-title">Visualizações</h3>
            <div class="results-grid single-col">
                ${imageCards}
            </div>
        `);
    }

    return sections.join('');
}

export function closeModal() {
    document.getElementById('modal').classList.remove('active');
}
