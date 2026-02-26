import { escapeHtml } from '../utils.js';
import { updateBiometrics, updateSMI } from '../api.js';

export function renderBiometricSection(caseId, metadata, results) {
    const weight = metadata.Weight || null;
    const height = metadata.Height || null;
    const sma = results.SMA_cm2 || null;

    let bmi = null;
    if (weight && height) {
        bmi = (weight / (height * height)).toFixed(1);
    }

    let bsa = null;
    if (weight && height) {
        bsa = calculateBsa(weight, height);
    }

    let smi = null;
    if (height && sma) {
        smi = (sma / (height * height)).toFixed(2);
    }

    setTimeout(() => bindBiometricEvents(caseId), 0);

    return `
        <h3 class="section-title">Dados Biométricos</h3>
        <div class="results-grid" id="biometric-section">
            <div class="result-card">
                <div class="result-label">Peso</div>
                <div class="result-value" id="weight-display">${weight ? weight + ' <span class="result-unit">kg</span>' : '<span class="text-secondary">Não informado</span>'}</div>
                <input type="number" id="weight-input" class="biometric-input" step="0.1" min="1" max="500" placeholder="Ex: 75.5" value="${weight || ''}">
            </div>
            <div class="result-card">
                <div class="result-label">Altura</div>
                <div class="result-value" id="height-display">${height ? height + ' <span class="result-unit">m</span>' : '<span class="text-secondary">Não informado</span>'}</div>
                <input type="number" id="height-input" class="biometric-input" step="0.01" min="0.5" max="3.0" placeholder="Ex: 1.75" value="${height || ''}">
            </div>
            ${bmi ? `
            <div class="result-card" id="bmi-card">
                <div class="result-label">IMC</div>
                <div class="result-value highlight" id="bmi-display">${bmi} <span class="result-unit">kg/m²</span></div>
            </div>
            ` : '<div id="bmi-card-container"></div>'}
            ${bsa ? `
            <div class="result-card" id="bsa-card">
                <div class="result-label">ASC</div>
                <div class="result-value highlight" id="bsa-display">${bsa} <span class="result-unit">m²</span></div>
            </div>
            ` : '<div id="bsa-card-container"></div>'}
            ${smi ? `
            <div class="result-card" id="smi-card">
                <div class="result-label">SMI (Índice Músculo-Esquelético)</div>
                <div class="result-value highlight" id="smi-display">${smi} <span class="result-unit">cm²/m²</span></div>
            </div>
            ` : '<div id="smi-card-container"></div>'}
        </div>
        <div class="biometric-actions">
            <button class="btn btn-secondary" id="edit-biometrics-btn">✏️ Editar</button>
            <button class="btn btn-primary" id="save-biometrics-btn" style="display: none;">💾 Salvar</button>
            <button class="btn btn-secondary" id="cancel-biometrics-btn" style="display: none;">❌ Cancelar</button>
        </div>
        <div id="biometric-message" class="biometric-msg"></div>
    `;
}

function bindBiometricEvents(caseId) {
    const editBtn = document.getElementById('edit-biometrics-btn');
    const saveBtn = document.getElementById('save-biometrics-btn');
    const cancelBtn = document.getElementById('cancel-biometrics-btn');

    if (editBtn) editBtn.addEventListener('click', toggleBiometricEdit);
    if (cancelBtn) cancelBtn.addEventListener('click', toggleBiometricEdit);
    if (saveBtn) saveBtn.addEventListener('click', () => saveBiometrics(caseId));
}

function toggleBiometricEdit() {
    const displays = ['weight-display', 'height-display'].map(id => document.getElementById(id));
    const inputs = ['weight-input', 'height-input'].map(id => document.getElementById(id));
    const btns = {
        edit: document.getElementById('edit-biometrics-btn'),
        save: document.getElementById('save-biometrics-btn'),
        cancel: document.getElementById('cancel-biometrics-btn')
    };

    const isEditing = inputs[0].style.display !== 'none' && inputs[0].style.display !== '';

    if (isEditing) {
        inputs.forEach(i => i.style.display = 'none');
        displays.forEach(d => d.style.display = 'block');
        btns.edit.style.display = 'inline-block';
        btns.save.style.display = 'none';
        btns.cancel.style.display = 'none';
    } else {
        inputs.forEach(i => i.style.display = 'block');
        displays.forEach(d => d.style.display = 'none');
        btns.edit.style.display = 'none';
        btns.save.style.display = 'inline-block';
        btns.cancel.style.display = 'inline-block';

        if (!inputs[0].value) inputs[0].focus();
        else if (!inputs[1].value) inputs[1].focus();
    }
}

async function saveBiometrics(caseId) {
    const weightInput = document.getElementById('weight-input');
    const heightInput = document.getElementById('height-input');
    const messageDiv = document.getElementById('biometric-message');

    const weight = parseFloat(weightInput.value);
    const height = parseFloat(heightInput.value);

    if (!weight || weight <= 0 || weight > 500) {
        messageDiv.innerHTML = '<span class="text-danger">❌ Peso inválido</span>';
        return;
    }
    if (!height || height <= 0 || height > 3.0) {
        messageDiv.innerHTML = '<span class="text-danger">❌ Altura inválida</span>';
        return;
    }

    try {
        messageDiv.innerHTML = '<span class="text-secondary">⏳ Salvando...</span>';
        const result = await updateBiometrics(caseId, weight, height);

        document.getElementById('weight-display').innerHTML = `${weight} <span class="result-unit">kg</span>`;
        document.getElementById('height-display').innerHTML = `${height} <span class="result-unit">m</span>`;

        let bmiDisplay = document.getElementById('bmi-display');
        if (bmiDisplay) {
            bmiDisplay.innerHTML = `${result.bmi} <span class="result-unit">kg/m²</span>`;
        } else {
            document.getElementById('bmi-card-container').outerHTML = `
                <div class="result-card" id="bmi-card">
                    <div class="result-label">IMC</div>
                    <div class="result-value highlight" id="bmi-display">${result.bmi} <span class="result-unit">kg/m²</span></div>
                </div>`;
        }

        const bsa = calculateBsa(weight, height);
        let bsaDisplay = document.getElementById('bsa-display');
        if (bsaDisplay) {
            bsaDisplay.innerHTML = `${bsa} <span class="result-unit">m²</span>`;
        } else {
            document.getElementById('bsa-card-container').outerHTML = `
                <div class="result-card" id="bsa-card">
                    <div class="result-label">ASC</div>
                    <div class="result-value highlight" id="bsa-display">${bsa} <span class="result-unit">m²</span></div>
                </div>`;
        }

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
            let smiDisplay = document.getElementById('smi-display');
            if (smiDisplay) {
                smiDisplay.innerHTML = `${smi} <span class="result-unit">cm²/m²</span>`;
            } else {
                document.getElementById('smi-card-container').outerHTML = `
                    <div class="result-card" id="smi-card">
                        <div class="result-label">SMI (Índice Músculo-Esquelético)</div>
                        <div class="result-value highlight" id="smi-display">${smi} <span class="result-unit">cm²/m²</span></div>
                    </div>`;
            }
            try {
                await updateSMI(caseId, parseFloat(smi));
            } catch (e) {
                console.error('Failed to save SMI', e);
            }
        }

        toggleBiometricEdit();
        messageDiv.innerHTML = '<span class="text-success">✅ Dados salvos com sucesso!</span>';
        setTimeout(() => messageDiv.innerHTML = '', 3000);
    } catch (error) {
        messageDiv.innerHTML = `<span class="text-danger">❌ Erro: ${escapeHtml(error.message)}</span>`;
    }
}

function calculateBsa(weightKg, heightMeters) {
    const heightCm = heightMeters * 100;
    return Math.sqrt((heightCm * weightKg) / 3600).toFixed(2);
}
