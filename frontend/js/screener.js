/**
 * Screener logic — manages table, filters, trade setup, and signal reasons.
 * Trade setup (Entry/SL/TP) di-fetch per ticker dari /api/ticker/{symbol}
 * karena /api/screener/results tidak menyertakan data tersebut secara langsung.
 */

const Screener = {
    data: [],
    tradeSetupCache: {},   // cache per symbol agar tidak double-fetch
    filters: {
        signal_type: '',
        timeframe: '',
        min_confidence: 0.5   // default 50%
    },

    async init() {
        this.setupEventListeners();
        await this.refresh();
    },

    setupEventListeners() {
        // Run scan button → open modal
        const btnRunScan = document.getElementById('btn-run-scan');
        const modalScan = document.getElementById('modal-scan-config');

        if (btnRunScan) btnRunScan.addEventListener('click', () => modalScan.classList.remove('hidden'));

        document.querySelectorAll('.close-modal').forEach(btn =>
            btn.addEventListener('click', () => modalScan.classList.add('hidden'))
        );

        // Category warning
        const selectCategory = document.getElementById('scan-category');
        const categoryWarning = document.getElementById('category-warning');
        if (selectCategory) {
            selectCategory.addEventListener('change', e => {
                if (e.target.value === 'all_idx') {
                    categoryWarning.textContent = '⚠️ Large market (30-60 mins). High resource usage.';
                    categoryWarning.classList.add('warning');
                } else {
                    categoryWarning.textContent = 'Small list (2-5 mins)';
                    categoryWarning.classList.remove('warning');
                }
            });
        }

        // Confidence slider (scan modal)
        const scanConf = document.getElementById('scan-confidence');
        const scanConfVal = document.getElementById('scan-confidence-val');
        if (scanConf) scanConf.addEventListener('input', e => scanConfVal.textContent = e.target.value + '%');

        // Start scan
        const btnStartManual = document.getElementById('btn-start-manual-scan');
        if (btnStartManual) {
            btnStartManual.addEventListener('click', async () => {
                const category = selectCategory.value;
                const timeframes = Array.from(document.querySelectorAll('input[name="scan-tf"]:checked'))
                    .map(cb => cb.value).join(',');
                const minConf = parseInt(scanConf.value) / 100;

                if (!timeframes) { Utils.showNotification('Pilih minimal satu timeframe.', 'error'); return; }

                modalScan.classList.add('hidden');
                document.getElementById('loading-overlay').classList.remove('hidden');
                document.getElementById('loading-text').textContent = `Memulai scan ${category.toUpperCase()}...`;

                const q = new URLSearchParams({ category, timeframes, min_confidence: minConf });
                const result = await Utils.apiRequest(`/api/screener/run?${q}`, { method: 'POST' });
                if (result?.status === 'started')
                    Utils.showNotification(`Scan dimulai untuk ${category}. Pantau di Activity feed.`, 'success');

                setTimeout(() => document.getElementById('loading-overlay').classList.add('hidden'), 5000);
            });
        }

        // Filter controls
        const filterSignal = document.getElementById('filter-signal-type');
        const filterTF = document.getElementById('filter-timeframe');
        const filterConf = document.getElementById('filter-confidence');
        const confVal = document.getElementById('confidence-value');

        if (filterSignal) filterSignal.addEventListener('change', e => { this.filters.signal_type = e.target.value; this.refresh(); });
        if (filterTF) filterTF.addEventListener('change', e => { this.filters.timeframe = e.target.value; this.refresh(); });
        if (filterConf) {
            filterConf.addEventListener('input', e => confVal.textContent = e.target.value + '%');
            filterConf.addEventListener('change', e => { this.filters.min_confidence = parseInt(e.target.value) / 100; this.refresh(); });
        }

        const btnReset = document.getElementById('btn-reset-filters');
        if (btnReset) btnReset.addEventListener('click', () => {
            if (filterSignal) filterSignal.value = '';
            if (filterTF) filterTF.value = '';
            if (filterConf) { filterConf.value = 50; confVal.textContent = '50%'; }
            this.filters = { signal_type: '', timeframe: '', min_confidence: 0.5 };
            this.refresh();
        });
    },

    async refresh() {
        const q = new URLSearchParams({ min_confidence: this.filters.min_confidence, limit: 50 });
        if (this.filters.signal_type) q.append('signal_type', this.filters.signal_type);
        if (this.filters.timeframe) q.append('timeframe', this.filters.timeframe);

        const response = await Utils.apiRequest(`/api/screener/results?${q}`);
        if (!response?.results) return;

        this.data = response.results;

        // Fetch trade setup untuk setiap ticker (batch, cached)
        await this._prefetchTradeSetups(this.data);

        this.renderTable('screener-table', this.data, true);
        this.renderTable('top-signals-table', this.data.slice(0, 10), false);
        this.updateStats();
    },

    /** Fetch /api/ticker/{symbol} untuk semua ticker yang belum di-cache */
    async _prefetchTradeSetups(data) {
        const symbols = [...new Set(data.map(r => r.symbol))].filter(s => !this.tradeSetupCache[s]);
        await Promise.all(symbols.map(async symbol => {
            const detail = await Utils.apiRequest(`/api/ticker/${symbol}`);
            if (detail?.signals?.length) {
                // Simpan semua sinyal per simbol agar bisa di-match per timeframe
                this.tradeSetupCache[symbol] = detail.signals;
            } else {
                this.tradeSetupCache[symbol] = [];
            }
        }));
    },

    /** Ambil trade setup terbaik untuk simbol + timeframe tertentu */
    _getSetup(symbol, timeframe) {
        const sigs = this.tradeSetupCache[symbol] || [];
        // Cocokkan timeframe, ambil confidence tertinggi
        const match = sigs
            .filter(s => s.timeframe === timeframe)
            .sort((a, b) => (b.confidence_score || 0) - (a.confidence_score || 0))[0];
        return match || sigs[0] || null;   // fallback ke sinyal apapun
    },

    /** Bangun teks alasan sinyal dari metadata indikator */
    _buildReason(row, setup) {
        const reasons = [];
        const meta = row.metadata || {};

        // Divergence info
        if (row.signal_type?.includes('divergence')) {
            const ind = row.indicator || meta.indicator || '';
            const p1 = meta.ind_pivot_1_val ?? row.ind_pivot_1_val;
            const p2 = meta.ind_pivot_2_val ?? row.ind_pivot_2_val;
            if (ind && p1 != null && p2 != null) {
                const dir = row.signal_type === 'bullish_divergence' ? 'naik' : 'turun';
                reasons.push(`${ind.toUpperCase()} ${dir} (${p1.toFixed(1)} → ${p2.toFixed(1)})`);
            }
            const str = row.divergence_strength ?? meta.divergence_strength;
            if (str != null) reasons.push(`Kekuatan: ${Math.round(str * 100)}%`);
        }

        // ABC info
        if (row.signal_type === 'abc_correction') {
            const b = meta.b_retracement ?? row.b_retracement;
            const c = meta.c_extension ?? row.c_extension;
            if (b != null) reasons.push(`Wave B retrace ${(b * 100).toFixed(1)}%`);
            if (c != null) reasons.push(`Wave C ext ${(c * 100).toFixed(1)}%`);
            const fib = meta.fibonacci_precision ?? row.fibonacci_precision;
            if (fib != null) reasons.push(`Fib presisi: ${Math.round(fib * 100)}%`);
        }

        // Accumulation info
        if (row.signal_type === 'accumulation') {
            const m = meta.metadata || meta;
            if (m.adl_trend) reasons.push(`ADL ${m.adl_trend}`);
            if (m.obv_trend) reasons.push(`OBV ${m.obv_trend}`);
            if (m.mfi_level) reasons.push(`MFI ${m.mfi_level}`);
            if (m.volume_ratio) reasons.push(`Vol ratio ${m.volume_ratio}x`);
        }

        // Multi-TF confirmation
        if (row.multi_tf_confirmed) {
            const tfs = (row.confirmed_timeframes || []).join(', ');
            reasons.push(`<i class="fas fa-layer-group"></i> Multi-TF: ${tfs}`);
        }

        if (row.signal_type === 'accumulation' || row.signal_type === 'distribution') {
            return `<i class="fas fa-user-secret"></i> Bandar Analysis: ${reasons.join(' · ')}`;
        }

        return reasons.length ? reasons.join(' · ') : 'Technical Signal Detected';
    },

    renderTable(tableId, data, isFull) {
        const table = document.getElementById(tableId);
        if (!table) return;

        const tbody = table.querySelector('tbody');
        tbody.innerHTML = '';

        if (!data.length) {
            const cols = isFull ? 11 : 8;
            tbody.innerHTML = `<tr><td colspan="${cols}" class="text-center text-dim" style="padding:24px">
                Tidak ada sinyal yang memenuhi filter. Coba turunkan min. confidence atau jalankan scan baru.
            </td></tr>`;
            return;
        }

        data.forEach((row, idx) => {
            const tr = document.createElement('tr');
            tr.className = 'animate-up';
            tr.style.animationDelay = `${idx * 0.04}s`;

            const signalClass = Utils.getSignalClass(row.signal_type);
            const scorePercent = Math.round((row.confidence_score || 0) * 100);
            const setup = this._getSetup(row.symbol, row.timeframe);
            const reason = this._buildReason(row, setup);

            if (scorePercent >= 85) tr.classList.add('premium-signal');

            // Priority: use flattened top-level fields (row.entry) OR the prefetched setup object
            const entry = row.entry || setup?.entry || row.latest_price || null;
            const sl = row.stop_loss || setup?.stop_loss || null;
            const tp1 = row.target_1 || setup?.target_1 || null;
            const tp2 = row.target_2 || setup?.target_2 || null;
            const rr = row.risk_reward_1 || setup?.risk_reward_1 || null;
            
            // Percentage distance metrics
            const sl_pct = row.sl_pct || setup?.sl_pct || null;
            const tp1_pct = row.tp1_pct || setup?.tp1_pct || null;

            if (isFull) {
                tr.innerHTML = `
                    <td class="text-dim">#${row.rank || idx + 1}</td>
                    <td>
                        <div class="ticker-cell">
                            <b class="ticker-symbol">${row.symbol}</b>
                            <span class="ticker-name text-dim">${row.ticker_name || '—'}</span>
                        </div>
                    </td>
                    <td>
                        <span class="badge-signal ${signalClass}">${Utils.formatSignalType(row.signal_type).replace('Accumulation', 'Bandar Accum').replace('Distribution', 'Bandar Dist')}</span>
                        <div class="reason-text text-dim">${reason}</div>
                    </td>
                    <td><span class="tf-badge">${row.timeframe}</span></td>
                    <td>
                        <div class="score-pill">
                            <div class="score-bar" style="width:${scorePercent}%"></div>
                            <span>${scorePercent}%</span>
                        </div>
                    </td>
                    <td class="text-accent price-cell">
                        ${entry ? Utils.formatPrice(entry) : '<span class="text-dim">—</span>'}
                    </td>
                    <td class="price-cell">
                        ${tp1 ? `<span class="text-green">${Utils.formatPrice(tp1)}</span>
                                 ${tp1_pct ? `<span class="pct-tag green">+${tp1_pct}%</span>` : ''}` : '<span class="text-dim">—</span>'}
                    </td>
                    <td class="price-cell">
                        ${tp2 ? `<span class="text-green">${Utils.formatPrice(tp2)}</span>` : '<span class="text-dim">—</span>'}
                    </td>
                    <td class="text-red price-cell">
                        ${sl ? `<span class="text-red">${Utils.formatPrice(sl)}</span>
                                ${sl_pct ? `<span class="pct-tag red">-${sl_pct}%</span>` : ''}` : '<span class="text-dim">—</span>'}
                    </td>
                    <td class="text-dim">${rr ? `${rr}:1` : '—'}</td>
                    <td>
                        <div class="row-actions">
                            <button class="btn-view-chart btn-icon-small glass" data-symbol="${row.symbol}" data-tf="${row.timeframe}" title="Lihat chart">
                                <i class="fas fa-chart-line"></i>
                            </button>
                            ${scorePercent >= 80 ? '<span class="hot-badge">PRO</span>' : ''}
                        </div>
                    </td>
                `;
            } else {
                tr.innerHTML = `
                    <td><b>${row.symbol}</b></td>
                    <td><span class="badge-signal ${signalClass}">${Utils.formatSignalType(row.signal_type)}</span></td>
                    <td><span class="tf-badge">${row.timeframe}</span></td>
                    <td>${scorePercent}%</td>
                    <td class="text-accent">${entry ? Utils.formatPrice(entry) : '—'}</td>
                    <td class="text-green">${tp1 ? Utils.formatPrice(tp1) : '—'}</td>
                    <td class="text-red">${sl ? Utils.formatPrice(sl) : '—'}</td>
                    <td>
                        <button class="btn-view-chart btn-icon-small glass" data-symbol="${row.symbol}" data-tf="${row.timeframe}">
                            <i class="fas fa-eye"></i>
                        </button>
                    </td>
                `;
            }

            tr.querySelector('.btn-view-chart').addEventListener('click', () => {
                window.location.hash = '#analysis';
                App.loadTicker(row.symbol, row.timeframe);
            });

            tbody.appendChild(tr);
        });
    },

    async updateStats() {
        const summary = await Utils.apiRequest('/api/screener/summary');
        if (!summary) return;
        const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
        set('stat-total-signals', summary.total_signals || 0);
        set('stat-bullish-div', (summary.by_type?.bullish_divergence || 0) + (summary.by_type?.hidden_bullish_divergence || 0));
        set('stat-abc-correction', summary.by_type?.abc_correction || 0);
        set('stat-accumulation', summary.by_type?.accumulation || 0);
    }
};