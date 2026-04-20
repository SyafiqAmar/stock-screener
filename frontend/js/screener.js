/**
 * Screener logic for managing the data table, filtering, and sorting.
 */

const Screener = {
    data: [],
    filters: {
        signal_type: '',
        timeframe: '',
        min_confidence: 0
    },

    async init() {
        this.setupEventListeners();
        await this.refresh();
    },

    setupEventListeners() {
        // Run scan button (Open Modal)
        const btnRunScan = document.getElementById('btn-run-scan');
        const modalScan = document.getElementById('modal-scan-config');
        const btnStartManual = document.getElementById('btn-start-manual-scan');
        const closeModalBtns = document.querySelectorAll('.close-modal');

        if (btnRunScan) {
            btnRunScan.addEventListener('click', () => {
                modalScan.classList.remove('hidden');
            });
        }

        if (closeModalBtns) {
            closeModalBtns.forEach(btn => {
                btn.addEventListener('click', () => {
                    modalScan.classList.add('hidden');
                });
            });
        }

        // Category warning logic
        const selectCategory = document.getElementById('scan-category');
        const categoryWarning = document.getElementById('category-warning');
        if (selectCategory) {
            selectCategory.addEventListener('change', (e) => {
                if (e.target.value === 'all_idx') {
                    categoryWarning.textContent = "⚠️ Large market (30-60 mins). High resource usage.";
                    categoryWarning.classList.add('warning');
                } else {
                    categoryWarning.textContent = "Small list (2-5 mins)";
                    categoryWarning.classList.remove('warning');
                }
            });
        }

        // Confidence range display
        const scanConf = document.getElementById('scan-confidence');
        const scanConfVal = document.getElementById('scan-confidence-val');
        if (scanConf) {
            scanConf.addEventListener('input', (e) => {
                scanConfVal.textContent = e.target.value + '%';
            });
        }

        if (btnStartManual) {
            btnStartManual.addEventListener('click', async () => {
                const category = selectCategory.value;
                const timeframes = Array.from(document.querySelectorAll('input[name="scan-tf"]:checked'))
                                       .map(cb => cb.value)
                                       .join(',');
                const minConfidence = parseInt(scanConf.value) / 100;

                if (!timeframes) {
                    Utils.showNotification('Please select at least one timeframe.', 'error');
                    return;
                }

                modalScan.classList.add('hidden');
                const overlay = document.getElementById('loading-overlay');
                const loadingText = document.getElementById('loading-text');
                overlay.classList.remove('hidden');
                loadingText.textContent = `Starting ${category.toUpperCase()} scan...`;
                
                const query = new URLSearchParams({
                    category: category,
                    timeframes: timeframes,
                    min_confidence: minConfidence
                });

                const result = await Utils.apiRequest(`/api/screener/run?${query.toString()}`, { method: 'POST' });
                if (result && result.status === 'started') {
                    Utils.showNotification(`Scan started for ${category}. Check status in Activity feed.`, 'success');
                }
                
                setTimeout(() => overlay.classList.add('hidden'), 5000);
            });
        }

        // Filter inputs
        const filterSignal = document.getElementById('filter-signal-type');
        const filterTF = document.getElementById('filter-timeframe');
        const filterConf = document.getElementById('filter-confidence');
        const confVal = document.getElementById('confidence-value');

        if (filterSignal) filterSignal.addEventListener('change', (e) => {
            this.filters.signal_type = e.target.value;
            this.refresh();
        });

        if (filterTF) filterTF.addEventListener('change', (e) => {
            this.filters.timeframe = e.target.value;
            this.refresh();
        });

        if (filterConf) filterConf.addEventListener('input', (e) => {
            const val = parseInt(e.target.value);
            confVal.textContent = val + '%';
            this.filters.min_confidence = val / 100;
        });

        if (filterConf) filterConf.addEventListener('change', () => this.refresh());

        const btnReset = document.getElementById('btn-reset-filters');
        if (btnReset) btnReset.addEventListener('click', () => {
            if (filterSignal) filterSignal.value = '';
            if (filterTF) filterTF.value = '';
            if (filterConf) {
                filterConf.value = 0;
                confVal.textContent = '0%';
            }
            this.filters = { signal_type: '', timeframe: '', min_confidence: 0 };
            this.refresh();
        });
    },

    async refresh() {
        const query = new URLSearchParams({
            min_confidence: this.filters.min_confidence,
            limit: 50
        });
        if (this.filters.signal_type) query.append('signal_type', this.filters.signal_type);
        if (this.filters.timeframe) query.append('timeframe', this.filters.timeframe);

        const response = await Utils.apiRequest(`/api/screener/results?${query.toString()}`);
        if (response && response.results) {
            this.data = response.results;
            this.renderTable('screener-table', this.data, true);
            
            // Also update dashboard table if empty
            if (document.getElementById('top-signals-table')) {
                this.renderTable('top-signals-table', this.data.slice(0, 10), false);
            }

            // Update stats
            this.updateStats();
        }
    },

    renderTable(tableId, data, isFull) {
        const table = document.getElementById(tableId);
        if (!table) return;
        
        const tbody = table.querySelector('tbody');
        tbody.innerHTML = '';

        if (data.length === 0) {
            tbody.innerHTML = `<tr><td colspan="8" class="text-center">No signals matching filters found.</td></tr>`;
            return;
        }

        data.forEach((row, idx) => {
            const tr = document.createElement('tr');
            tr.className = 'animate-up';
            tr.style.animationDelay = `${idx * 0.05}s`;

            const signalClass = Utils.getSignalClass(row.signal_type);
            const scorePercent = Math.round(row.confidence_score * 100);
            const setup = row.metadata?.trade_setup || {};

            if (isFull) {
                tr.innerHTML = `
                    <td>#${row.rank || idx + 1}</td>
                    <td><b>${row.symbol}</b></td>
                    <td class="text-dim">${row.ticker_name || '—'}</td>
                    <td><span class="badge-signal ${signalClass}">${Utils.formatSignalType(row.signal_type)}</span></td>
                    <td><span class="tf-badge">${row.timeframe}</span></td>
                    <td>
                        <div class="score-pill">
                            <div class="score-bar" style="width: ${scorePercent}%"></div>
                            <span>${scorePercent}%</span>
                        </div>
                    </td>
                    <td class="text-accent">${Utils.formatPrice(setup.entry || row.latest_price || 0)}</td>
                    <td class="text-green">${Utils.formatPrice(setup.target_1 || 0)}</td>
                    <td class="text-green">${Utils.formatPrice(setup.target_2 || 0)}</td>
                    <td class="text-red">${Utils.formatPrice(setup.stop_loss || 0)}</td>
                    <td>
                        <button class="btn-icon circle glass btn-view-chart" data-symbol="${row.symbol}" data-tf="${row.timeframe}">
                            <i class="fas fa-chart-line"></i>
                        </button>
                    </td>
                `;
            } else {
                tr.innerHTML = `
                    <td><b>${row.symbol}</b></td>
                    <td><span class="badge-signal ${signalClass}">${Utils.formatSignalType(row.signal_type)}</span></td>
                    <td><span class="tf-badge">${row.timeframe}</span></td>
                    <td>${scorePercent}%</td>
                    <td>${Utils.formatPrice(setup.entry || row.latest_price || 0)}</td>
                    <td class="text-green">${Utils.formatPrice(setup.target_1 || 0)}</td>
                    <td class="text-red">${Utils.formatPrice(setup.stop_loss || 0)}</td>
                    <td>
                        <button class="btn-icon circle glass btn-view-chart" data-symbol="${row.symbol}" data-tf="${row.timeframe}">
                            <i class="fas fa-eye"></i>
                        </button>
                    </td>
                `;
            }

            // Add click listener for view chart
            const btn = tr.querySelector('.btn-view-chart');
            btn.addEventListener('click', () => {
                window.location.hash = '#analysis';
                if (typeof App !== 'undefined') App.loadTicker(row.symbol, row.timeframe);
            });

            tbody.appendChild(tr);
        });
    },

    async updateStats() {
        const summary = await Utils.apiRequest('/api/screener/summary');
        if (summary) {
            document.getElementById('stat-total-signals').textContent = summary.total_signals;
            document.getElementById('stat-bullish-div').textContent = (summary.by_type?.bullish_divergence || 0) + (summary.by_type?.hidden_bullish_divergence || 0);
            document.getElementById('stat-abc-correction').textContent = summary.by_type?.abc_correction || 0;
            document.getElementById('stat-accumulation').textContent = summary.by_type?.accumulation || 0;
            
            // Note: HTML might have slight id differences, adjust if needed
        }
    }
};
