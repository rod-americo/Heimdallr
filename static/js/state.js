// ============================================
// State Management Module
// ============================================

export const state = {
    patients: [],
    currentFilter: '',
    currentDateFilter: 'all',
};

export function setPatients(newPatients) {
    state.patients = newPatients;
}

export function setFilter(filter) {
    state.currentFilter = filter.toLowerCase();
}

export function setDateFilter(filter) {
    state.currentDateFilter = filter;
}

export function getFilteredPatients() {
    return state.patients.filter(p => {
        const name = p.case_id.split('_')[0].toLowerCase();
        const accession = (p.accession || '').toLowerCase();
        const date = (p.study_date || '').toLowerCase();
        const matchesText = !state.currentFilter ||
            name.includes(state.currentFilter) ||
            accession.includes(state.currentFilter) ||
            date.includes(state.currentFilter);

        return matchesText && matchesDateFilter(p.study_date);
    });
}

function matchesDateFilter(studyDate) {
    if (state.currentDateFilter === 'all') {
        return true;
    }

    const parsedDate = parseStudyDate(studyDate);
    if (!parsedDate) {
        return false;
    }

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    if (state.currentDateFilter === 'today') {
        return parsedDate.getTime() === today.getTime();
    }

    if (state.currentDateFilter === 'yesterday') {
        const yesterday = new Date(today);
        yesterday.setDate(today.getDate() - 1);
        return parsedDate.getTime() === yesterday.getTime();
    }

    if (state.currentDateFilter === '7days') {
        const startDate = new Date(today);
        startDate.setDate(today.getDate() - 6);
        return parsedDate >= startDate && parsedDate <= today;
    }

    return true;
}

function parseStudyDate(studyDate) {
    if (!studyDate || studyDate.length !== 8) {
        return null;
    }

    const year = Number(studyDate.slice(0, 4));
    const month = Number(studyDate.slice(4, 6)) - 1;
    const day = Number(studyDate.slice(6, 8));
    const parsed = new Date(year, month, day);
    parsed.setHours(0, 0, 0, 0);

    if (Number.isNaN(parsed.getTime())) {
        return null;
    }

    return parsed;
}
