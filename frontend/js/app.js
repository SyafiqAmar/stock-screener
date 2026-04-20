/**
 * Main application orchestrator.
 * Handles SPA routing, global state, and initializing components.
 */

const App = {
    currentView: 'dashboard',

    async init() {
        console.log('🚀 Initializing Stock Screener App...');
        
        // 1. Initialize Components
        WSClient.init();
        Screener.init();
        Chart.init();

        // 2. Setup Routing
        this.setupRouting();
        
        // 3. Global Event Listeners
        this.setupEventListeners();

        // 4. Load initial summary data
        this.loadSummary();

        console.log('✅ App ready');
    },

    setupRouting() {
        /**
         * Simple Hash-based Router
         */
        const handleRoute = () => {
            const hash = window.location.hash.substring(1) || 'dashboard';
            this.switchView(hash);
        };

        window.addEventListener('hashchange', handleRoute);
        handleRoute(); // Initial load
    },

    switchView(viewId) {
        if (this.currentView === viewId) return;

        console.log(`Switching view to: ${viewId}`);
        
        // Update Sidebar UI
        document.querySelectorAll('.menu-item').forEach(item => {
            item.classList.remove('active');
            if (item.id === `menu-${viewId}`) item.classList.add('active');
        });

        // Update View Display
        document.querySelectorAll('.view').forEach(view => {
            view.classList.remove('active');
            if (view.id === `view-${viewId}`) view.classList.add('active');
        });

        this.currentView = viewId;

        // Perform view-specific logic
        if (viewId === 'dashboard') {
            Screener.refresh();
        } else if (viewId === 'screener') {
            Screener.refresh();
        }
    },

    setupEventListeners() {
        // Global search (example: BBCA.JK)
        const searchInput = document.getElementById('global-search');
        if (searchInput) {
            searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    const symbol = searchInput.value.toUpperCase();
                    if (symbol) {
                        window.location.hash = '#analysis';
                        this.loadTicker(symbol);
                    }
                }
            });
        }
    },

    async loadSummary() {
        await Screener.updateStats();
    },

    async loadTicker(symbol, timeframe = '1d') {
        console.log(`Loading ticker detail: ${symbol}`);
        
        // If symbol doesn't have .JK and looks like Indonesian stock, add it
        if (!symbol.includes('.') && symbol.length === 4) {
            symbol += '.JK';
        }

        // Get ticker detail to show name etc
        const detail = await Utils.apiRequest(`/api/ticker/${symbol}`);
        if (detail && detail.info) {
            document.getElementById('chart-ticker-name').textContent = detail.info.name || 'Stock Detail';
            
            // Switch to analysis view if not there
            if (this.currentView !== 'analysis') {
                this.switchView('analysis');
                window.location.hash = '#analysis';
            }

            // Load chart
            Chart.loadData(symbol, timeframe);
            
            // Show notification
            Utils.showNotification(`Loaded ${symbol}: ${detail.info.name}`, 'success');
        } else {
            Utils.showNotification(`Ticker ${symbol} not found.`, 'warning');
        }
    }
};

// Start the app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    App.init();
});
