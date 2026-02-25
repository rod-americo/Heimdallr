import { fetchPatients } from './api.js';
import { setPatients, setFilter } from './state.js';
import { renderPatients } from './components/PatientTable.js';
import { closeModal } from './components/ResultsModal.js';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadData();
    // Refresh every 30 seconds
    setInterval(loadData, 30000);

    // Filter input
    document.getElementById('searchInput').addEventListener('input', (e) => {
        setFilter(e.target.value);
        renderPatients();
    });

    // Close modal on backdrop click
    const modal = document.getElementById('modal');
    if (modal) {
        modal.addEventListener('click', (e) => {
            if (e.target.id === 'modal') closeModal();
        });
    }

    // Close modal on Escape key
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') closeModal();
    });
});

async function loadData() {
    try {
        const patients = await fetchPatients();
        setPatients(patients);
        renderPatients();
    } catch (error) {
        console.error('Error loading patients:', error);
        showError('Erro ao carregar pacientes');
    }
}

function showError(message) {
    const tbody = document.getElementById('patients-body');
    if (tbody) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="loading text-danger">
                    ❌ ${message}
                </td>
            </tr>
        `;
    }
}
