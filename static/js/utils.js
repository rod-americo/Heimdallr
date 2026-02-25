// ============================================
// Utility Functions
// ============================================

export function formatDate(dateStr) {
    if (!dateStr || dateStr.length !== 8) return dateStr || '-';
    return `${dateStr.slice(6, 8)}/${dateStr.slice(4, 6)}/${dateStr.slice(0, 4)}`;
}

export function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

export function getProgressColor(perc) {
    if (perc < 5) return 'var(--success, #22c55e)';  // Normal
    if (perc < 15) return '#eab308';                // Mild
    if (perc < 25) return '#f97316';                // Moderate
    return 'var(--danger, #ef4444)';                // Severe
}
