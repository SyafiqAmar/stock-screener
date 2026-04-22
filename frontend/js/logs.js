/**
 * Scraper Logs & Activity Monitoring.
 */
const Logger = {
    init() {
        console.log('📜 Initializing Scraper Logger...');
        this.setupEventListeners();
    },

    setupEventListeners() {
        const btnRefresh = document.getElementById('btn-refresh-logs');
        if (btnRefresh) {
            btnRefresh.addEventListener('click', () => this.refresh());
        }
    },

    async refresh() {
        const logsTable = document.getElementById('logs-table');
        if (!logsTable) return;

        const tbody = logsTable.querySelector('tbody');
        
        // Show loading state
        const btnIcon = document.querySelector('#btn-refresh-logs i');
        if (btnIcon) btnIcon.classList.add('fa-spin');

        const response = await Utils.apiRequest('/api/logs/scraper?limit=100');
        
        if (btnIcon) btnIcon.classList.remove('fa-spin');
        
        if (!response || !response.logs) return;

        this.renderLogs(response.logs);
    },

    renderLogs(logs) {
        const tbody = document.querySelector('#logs-table tbody');
        tbody.innerHTML = '';

        if (!logs.length) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center text-dim" style="padding:24px">No activity logs found.</td></tr>';
            return;
        }

        logs.forEach((log, idx) => {
            const tr = document.createElement('tr');
            tr.className = 'animate-up';
            tr.style.animationDelay = `${idx * 0.02}s`;

            const statusClass = log.status === 'success' ? 'text-green' : 'text-red';
            const statusIcon = log.status === 'success' ? 'check-circle' : 'exclamation-circle';
            const timeStr = Utils.formatDate(log.time, true);

            tr.innerHTML = `
                <td class="text-dim" style="font-size: 0.85rem">${timeStr}</td>
                <td><b>${log.symbol}</b></td>
                <td><span class="tf-badge">${log.timeframe}</span></td>
                <td class="${statusClass}">
                    <i class="fas fa-${statusIcon}"></i> ${log.status.toUpperCase()}
                </td>
                <td class="text-dim" style="font-size: 0.85rem; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${log.error || ''}">
                    ${log.error || 'Successfully processed'}
                </td>
            `;

            tbody.appendChild(tr);
        });
    }
};

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    Logger.init();
});
