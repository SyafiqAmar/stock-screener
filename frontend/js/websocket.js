/**
 * WebSocket client for real-time updates.
 */

const WSClient = {
    socket: null,
    reconnectInterval: 3000,
    callbacks: {},

    init() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.host;
        const wsUrl = `${protocol}//${host}/ws/live`;
        
        console.log(`Connecting to WebSocket: ${wsUrl}`);
        this.socket = new WebSocket(wsUrl);

        this.socket.onopen = () => {
            console.log('Successfully connected to WebSocket');
            document.querySelector('.dot').classList.add('green');
            document.querySelector('.status-indicator span').textContent = 'System Live';
        };

        this.socket.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleMessage(data);
        };

        this.socket.onclose = () => {
            console.warn('WebSocket connection closed. Attempting to reconnect...');
            document.querySelector('.dot').classList.remove('green');
            document.querySelector('.status-indicator span').textContent = 'Disconnected';
            setTimeout(() => this.init(), this.reconnectInterval);
        };

        this.socket.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.socket.close();
        };
    },

    handleMessage(data) {
        // Log to activity feed if relevant
        if (data.type === 'scan_progress') {
            this.updateActivityFeed(`Scanning <b>${data.current_ticker}</b> (${data.completed}/${data.total})`);
        } else if (data.type === 'scan_complete') {
            this.updateActivityFeed(`✅ Scan complete! Found <b>${data.total_signals}</b> new signals.`);
            Utils.showNotification('Market scan completed successfully!', 'success');
            // Refresh screener if active
            if (typeof Screener !== 'undefined' && Screener.refresh) Screener.refresh();
        } else if (data.type === 'new_signal') {
            this.updateActivityFeed(`🚨 New Signal: <b>${data.ticker}</b> - ${Utils.formatSignalType(data.signal_type)}`);
            Utils.showNotification(`New ${Utils.formatSignalType(data.signal_type)} on ${data.ticker}`, 'info');
        }

        // Run registered callbacks
        if (this.callbacks[data.type]) {
            this.callbacks[data.type].forEach(cb => cb(data));
        }
    },

    on(type, callback) {
        if (!this.callbacks[type]) this.callbacks[type] = [];
        this.callbacks[type].push(callback);
    },

    updateActivityFeed(message) {
        const feed = document.getElementById('activity-feed');
        if (!feed) return;

        const emptyState = feed.querySelector('.empty-state');
        if (emptyState) emptyState.remove();

        const item = document.createElement('div');
        item.className = 'activity-item animate-up';
        
        const now = new Date();
        const timeStr = now.getHours().toString().padStart(2, '0') + ':' + 
                        now.getMinutes().toString().padStart(2, '0');

        item.innerHTML = `
            <span class="activity-time">${timeStr}</span>
            <span class="activity-desc">${message}</span>
        `;

        feed.insertBefore(item, feed.firstChild);

        // Keep only last 10 items
        while (feed.children.length > 10) {
            feed.removeChild(feed.lastChild);
        }
    },

    send(data) {
        if (this.socket && this.socket.readyState === WebSocket.OPEN) {
            this.socket.send(JSON.stringify(data));
        }
    }
};
