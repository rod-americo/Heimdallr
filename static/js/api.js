// ============================================
// API Service Module
// ============================================

const API_BASE = '';

export async function fetchPatients() {
    const response = await fetch(`${API_BASE}/api/patients`);
    if (!response.ok) throw new Error('Failed to fetch patients');
    const data = await response.json();
    return data.patients || [];
}

export async function fetchResults(caseId) {
    const response = await fetch(`${API_BASE}/api/patients/${encodeURIComponent(caseId)}/results`);
    if (!response.ok) throw new Error('Results not found');
    return response.json();
}

export async function fetchMetadata(caseId) {
    const response = await fetch(`${API_BASE}/api/patients/${encodeURIComponent(caseId)}/metadata`);
    if (!response.ok) throw new Error('Metadata not found');
    return response.json();
}

export async function updateBiometrics(caseId, weight, height) {
    const response = await fetch(`${API_BASE}/api/patients/${encodeURIComponent(caseId)}/biometrics`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ weight, height })
    });
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Erro ao salvar biométricos');
    }
    return response.json();
}

export async function updateSMI(caseId, smi) {
    const response = await fetch(`${API_BASE}/api/patients/${encodeURIComponent(caseId)}/smi`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ smi: parseFloat(smi) })
    });
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Erro ao salvar SMI');
    }
    return response.json();
}
