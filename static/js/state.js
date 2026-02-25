// ============================================
// State Management Module
// ============================================

export const state = {
    patients: [],
    currentFilter: '',
};

export function setPatients(newPatients) {
    state.patients = newPatients;
}

export function setFilter(filter) {
    state.currentFilter = filter.toLowerCase();
}

export function getFilteredPatients() {
    return state.patients.filter(p => {
        if (!state.currentFilter) return true;

        const name = p.case_id.split('_')[0].toLowerCase();
        const accession = (p.accession || '').toLowerCase();
        const date = (p.study_date || '').toLowerCase();

        return name.includes(state.currentFilter) ||
            accession.includes(state.currentFilter) ||
            date.includes(state.currentFilter);
    });
}
