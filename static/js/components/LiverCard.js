export function renderLiverSection(results) {
    const liverVol = results.liver_vol_cm3;
    const liverHU = results.liver_hu_mean;
    const hasPDFF = results.liver_pdff_percent !== undefined;

    if (!liverVol && !hasPDFF) return '';

    let liverCards = [];

    if (liverVol > 0) {
        liverCards.push(`
            <div class="result-card liver-card">
                <div class="result-label liver-label">Volume Total do Fígado</div>
                <div class="result-value liver-vol">${liverVol.toFixed(1)} <span class="result-unit">cm³</span></div>
                ${liverHU !== null && liverHU !== undefined ? `<div class="liver-hu">Densidade Média: ${liverHU.toFixed(1)} HU</div>` : ''}
            </div>
        `);
    }

    if (hasPDFF) {
        let kvpBadge = '';
        const pdff = results.liver_pdff_percent;
        const kvp = results.liver_pdff_kvp;

        let pdffClass = "text-primary";
        if (pdff < 5.0) pdffClass = "text-success";
        else if (pdff < 15.0) pdffClass = "text-warning";
        else if (pdff < 30.0) pdffClass = "text-orange";
        else pdffClass = "text-danger";

        const is120 = (kvp === "120" || kvp === "120.0");
        const badgeClass = is120 ? 'badge-success' : (kvp === "Unknown" ? 'badge-warning' : 'badge-warning');
        const badgeText = is120 ? '120 KV' : (kvp === "Unknown" ? '❓ KV' : `⚠️ ${kvp} KV`);
        const badgeTitle = is120 ? '' : (kvp === "Unknown" ? 'KV Desconhecido' : 'Cálculo idealizado para 120 kV');

        kvpBadge = `<span title="${badgeTitle}" class="kvp-badge ${badgeClass}">${badgeText}</span>`;

        liverCards.push(`
            <div class="result-card liver-card">
                <div class="result-label liver-label">Conteúdo de Gordura Hepática Estimado</div>
                <div class="pdff-container">
                    <div class="pdff-values">
                        <div class="result-value pdff-vol ${pdffClass}">${pdff.toFixed(1)} <span class="result-unit">%</span></div>
                        <div class="pdff-subtitle">MRI-PDFF Equivalente</div>
                    </div>
                    <div class="pdff-badge-container">
                        ${kvpBadge}
                    </div>
                </div>
                <div class="pdff-reference">Pickhardt et al., AJR 2018</div>
            </div>
        `);
    }

    return `
        <h3 class="section-title">Fígado</h3>
        <div class="results-grid">
            ${liverCards.join('')}
        </div>
    `;
}
