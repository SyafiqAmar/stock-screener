/**
 * Charting & Algo Analysis logic.
 * Menampilkan chart, signal detail lengkap, trade setup (Entry/SL/TP),
 * reason indikator, dan accumulation trend.
 */

const Chart = {
    charts: {},
    series: {},
    currentSymbol: null,
    currentTimeframe: '1d',
    currentDetail: null,   // data lengkap dari /api/ticker/{symbol}

    init() {
        this.createCharts();
        this.setupEventListeners();
        console.log('📊 Chart Engine initialized');
    },

    // Helper untuk memvalidasi data sebelum dikirim ke Lightweight Charts
    _validateData(data, requiredFields = []) {
        if (!Array.isArray(data)) return [];
        return data.filter(item => {
            if (!item || item.time == null) return false;
            for (const field of requiredFields) {
                if (item[field] == null || isNaN(item[field])) return false;
            }
            return true;
        });
    },

    createCharts() {
        const commonOptions = {
            layout: { background: { color: 'transparent' }, textColor: '#94a3b8' },
            grid: {
                vertLines: { color: 'rgba(255,255,255,0.04)' },
                horzLines: { color: 'rgba(255,255,255,0.04)' },
            },
            timeScale: { borderColor: 'rgba(255,255,255,0.1)', timeVisible: true },
            rightPriceScale: { borderColor: 'rgba(255,255,255,0.1)' },
            crosshair: { mode: 1 },
        };

        const mainContainer = document.getElementById('main-chart-container');
        if (!mainContainer) return;

        this.charts.main = LightweightCharts.createChart(mainContainer, { ...commonOptions, height: 400 });

        this.series.candle = this.charts.main.addCandlestickSeries({
            upColor: '#00d4aa', downColor: '#ef4444',
            borderVisible: false,
            wickUpColor: '#00d4aa', wickDownColor: '#ef4444',
        });

        this.series.volume = this.charts.main.addHistogramSeries({
            color: 'rgba(0,212,170,0.25)',
            priceFormat: { type: 'volume' },
            priceScaleId: 'volume-scale', // Gunakan ID unik
        });

        this.charts.main.priceScale('volume-scale').applyOptions({
            scaleMargins: { top: 0.8, bottom: 0 },
            visible: false, // Sembunyikan angka scale-nya
        });

        // RSI sub-chart
        const rsiContainer = document.getElementById('rsi-chart-container');
        if (rsiContainer) {
            this.charts.rsi = LightweightCharts.createChart(rsiContainer, { ...commonOptions, height: 120 });
            this.series.rsi = this.charts.rsi.addLineSeries({ color: '#9d50bb', lineWidth: 2, title: 'RSI 14' });

            // Static OB/OS lines — pakai nilai ekstrem agar tidak error time
            const obLine = this.charts.rsi.addLineSeries({ color: 'rgba(239,68,68,0.3)', lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false });
            const osLine = this.charts.rsi.addLineSeries({ color: 'rgba(0,212,170,0.3)', lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false });
            // set setelah data candle dimuat — lihat _drawRsiLevels()
            this._obLine = obLine;
            this._osLine = osLine;
        }

        // Sync timescale main ↔ RSI
        if (this.charts.rsi) {
            let syncing = false;
            
            const syncHandler = (fromChart, toChart) => {
                const range = fromChart.timeScale().getVisibleRange();
                const toRange = toChart.timeScale().getVisibleRange();
                
                // Hanya sync jika range valid dan berbeda
                if (!range || syncing) return;
                if (toRange && toRange.from === range.from && toRange.to === range.to) return;
                
                syncing = true;
                try {
                    toChart.timeScale().setVisibleRange(range);
                } catch (e) {
                    // Abaikan jika chart tujuan belum punya data
                }
                syncing = false;
            };

            this.charts.main.timeScale().subscribeVisibleTimeRangeChange(() => syncHandler(this.charts.main, this.charts.rsi));
            this.charts.rsi.timeScale().subscribeVisibleTimeRangeChange(() => syncHandler(this.charts.rsi, this.charts.main));
        }
    },

    setupEventListeners() {
        document.querySelectorAll('.tf-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.currentTimeframe = btn.dataset.tf;
                if (this.currentSymbol) this.loadData(this.currentSymbol, this.currentTimeframe);
            });
        });

        window.addEventListener('resize', () => {
            const mc = document.getElementById('main-chart-container');
            if (mc && this.charts.main) this.charts.main.resize(mc.clientWidth, 400);
            const rc = document.getElementById('rsi-chart-container');
            if (rc && this.charts.rsi) this.charts.rsi.resize(rc.clientWidth, 120);
        });
    },

    async loadData(symbol, timeframe) {
        this.currentSymbol = symbol;
        this.currentTimeframe = timeframe;

        document.getElementById('chart-ticker-symbol').textContent = symbol;
        document.getElementById('chart-ticker-name').textContent = 'Memuat data...';

        // Fetch OHLCV
        const ohlcv = await Utils.apiRequest(`/api/chart/${symbol}?timeframe=${timeframe}&limit=500`);
        if (ohlcv?.data?.length) {
            const candleData = this._validateData(ohlcv.data, ['open', 'high', 'low', 'close']);
            if (candleData.length) {
                try {
                    this.series.candle.setData(candleData);
                    
                    const volData = candleData.map(d => ({
                        time: d.time,
                        value: Number(d.volume) || 0,
                        color: (d.close >= d.open) ? 'rgba(0,212,170,0.25)' : 'rgba(239,68,68,0.25)'
                    }));
                    
                    this.series.volume.setData(volData);
                    this._drawRsiLevels(candleData);
                } catch (e) {
                    console.error('LWC Main Data error:', e);
                }
            }
        }

        // Fetch Indicators
        const ind = await Utils.apiRequest(`/api/chart/${symbol}/indicators?timeframe=${timeframe}&limit=500`);
        if (ind?.indicators && this.series.rsi) {
            const rsiData = ind.indicators
                .filter(i => i.time != null && i.rsi_14 != null && !isNaN(i.rsi_14))
                .map(i => ({ time: i.time, value: i.rsi_14 }));
            
            if (rsiData.length) {
                this.series.rsi.setData(rsiData);
            } else {
                this.series.rsi.setData([]); // Bisikan empty array jika tidak ada data
            }
        }

        // Fetch full detail (signals + trade setup + accum)
        const detail = await Utils.apiRequest(`/api/ticker/${symbol}`);
        this.currentDetail = detail;

        if (detail?.info) {
            document.getElementById('chart-ticker-name').textContent =
                `${detail.info.name || symbol} · ${detail.info.sector || ''}`;
        }

        // Render signal markers on chart
        const signalRes = await Utils.apiRequest(`/api/chart/${symbol}/signals`);
        if (signalRes?.signals) {
            this.renderMarkers(signalRes.signals.filter(s => s.timeframe === timeframe));
        }

        // Render detail panels
        if (detail) {
            this.renderSignalDetails(detail.signals || []);
            this.renderAccumulation(detail.accumulation_history || []);
        }
    },

    _drawRsiLevels(candleData) {
        if (!this._obLine || !this._osLine || !candleData || candleData.length < 2) return;
        
        try {
            const first = candleData[0].time;
            const last = candleData[candleData.length - 1].time;
            
            if (first == null || last == null) return;

            this._obLine.setData([{ time: first, value: 70 }, { time: last, value: 70 }]);
            this._osLine.setData([{ time: first, value: 30 }, { time: last, value: 30 }]);
        } catch (e) {
            console.warn('Non-critical: RSI Levels draw skipped', e.message);
        }
    },

    renderMarkers(signals) {
        if (!this.series.candle || !signals || !Array.isArray(signals)) return;
        
        const markers = signals.map(s => {
            if (!s.detected_at) return null;
            const ts = new Date(s.detected_at).getTime();
            if (isNaN(ts) || ts <= 0) return null;
            
            const isBullish = s.signal_type?.includes('bullish') || s.signal_type === 'abc_correction';
            return {
                time: Math.floor(ts / 1000),
                position: 'belowBar',
                color: isBullish ? '#00d4aa' : '#9d50bb',
                shape: 'arrowUp',
                text: Utils.formatSignalType(s.signal_type).split(' ')[0],
            };
        }).filter(m => m !== null).sort((a, b) => a.time - b.time);

        // Pastikan tidak ada duplikat 'time' dalam markers karena LWC bisa error
        const uniqueMarkers = [];
        const seenTimes = new Set();
        for (const m of markers) {
            if (!seenTimes.has(m.time)) {
                uniqueMarkers.push(m);
                seenTimes.add(m.time);
            }
        }

        try {
            this.series.candle.setMarkers(uniqueMarkers);
        } catch (e) {
            console.warn('Non-critical: Markers draw failed', e.message);
        }
    },

    renderSignalDetails(signals) {
        const list = document.getElementById('ticker-signal-list');
        if (!list) return;

        list.innerHTML = '';
        if (!signals.length) {
            list.innerHTML = '<div class="empty-state">Tidak ada sinyal aktif untuk ticker ini.</div>';
            return;
        }

        signals.forEach((s, idx) => {
            const signalClass = Utils.getSignalClass(s.signal_type);
            const scorePercent = Math.round((s.confidence_score || 0) * 100);
            const reason = this._buildReason(s);

            // Trade setup — sudah di-flatten di routes_ticker.py
            const entry = s.entry;
            const sl = s.stop_loss;
            const tp1 = s.target_1;
            const tp2 = s.target_2;
            const rr = s.risk_reward_1;
            const slPct = s.sl_pct;
            const tp1Pct = s.tp1_pct;
            const tp2Pct = s.tp2_pct;

            const hasSetup = entry && sl && tp1;

            const item = document.createElement('div');
            item.className = 'signal-detail-card glass animate-up';
            item.style.animationDelay = `${idx * 0.1}s`;

            item.innerHTML = `
                <div class="sdc-header">
                    <span class="badge-signal ${signalClass}">${Utils.formatSignalType(s.signal_type)}</span>
                    <div class="sdc-meta">
                        <span class="tf-badge">${s.timeframe}</span>
                        <span class="score-badge" style="background:${this._scoreColor(scorePercent)}">${scorePercent}%</span>
                    </div>
                </div>

                <div class="sdc-reason">${reason}</div>

                ${hasSetup ? `
                <div class="trade-setup-grid">
                    <div class="setup-cell entry">
                        <span class="setup-label">Entry</span>
                        <span class="setup-price">${Utils.formatPrice(entry)}</span>
                    </div>
                    <div class="setup-cell tp1">
                        <span class="setup-label">Target 1</span>
                        <span class="setup-price text-green">${Utils.formatPrice(tp1)}</span>
                        ${tp1Pct ? `<span class="setup-pct green">+${tp1Pct}%</span>` : ''}
                    </div>
                    <div class="setup-cell tp2">
                        <span class="setup-label">Target 2</span>
                        <span class="setup-price text-green">${Utils.formatPrice(tp2)}</span>
                        ${tp2Pct ? `<span class="setup-pct green">+${tp2Pct}%</span>` : ''}
                    </div>
                    <div class="setup-cell sl">
                        <span class="setup-label">Stop Loss</span>
                        <span class="setup-price text-red">${Utils.formatPrice(sl)}</span>
                        ${slPct ? `<span class="setup-pct red">-${slPct}%</span>` : ''}
                    </div>
                    ${rr ? `<div class="setup-cell rr">
                        <span class="setup-label">Risk/Reward</span>
                        <span class="setup-price text-accent">${rr}:1</span>
                    </div>` : ''}
                </div>
                ` : `<div class="setup-missing">Trade setup belum tersedia. Jalankan scan ulang.</div>`}

                <div class="sdc-footer">
                    <span class="text-dim">Terdeteksi: ${Utils.formatDate(s.detected_at, true)}</span>
                    ${s.multi_tf_confirmed ? `<span class="multi-tf-badge">Multi-TF ✓</span>` : ''}
                </div>
            `;

            list.appendChild(item);
        });
    },

    renderAccumulation(history) {
        const container = document.getElementById('accumulation-chart');
        if (!container) return;

        container.innerHTML = '';
        if (!history.length) {
            container.innerHTML = '<div class="empty-state">Tidak ada data akumulasi.</div>';
            return;
        }

        const latest = history[0];
        const phase = latest.phase || 'neutral';
        const phaseColor = phase === 'accumulation' ? '#00d4aa' : phase === 'distribution' ? '#ef4444' : '#94a3b8';
        const phaseIcon = phase === 'accumulation' ? '↑' : phase === 'distribution' ? '↓' : '→';

        // Trend 5 hari terakhir
        const trend = history.slice(0, 5).map(h => h.phase);
        const trendHtml = trend.map(p => {
            const c = p === 'accumulation' ? '#00d4aa' : p === 'distribution' ? '#ef4444' : '#64748b';
            const sym = p === 'accumulation' ? '▲' : p === 'distribution' ? '▼' : '●';
            return `<span style="color:${c};font-size:16px">${sym}</span>`;
        }).join('');

        container.innerHTML = `
            <div class="accum-phase-badge" style="color:${phaseColor};border-color:${phaseColor}">
                ${phaseIcon} ${phase.charAt(0).toUpperCase() + phase.slice(1)}
            </div>

            <div class="accum-metrics">
                <div class="accum-metric">
                    <span class="metric-label">ADL Trend</span>
                    <span class="metric-val ${latest.adl_value > 0 ? 'text-green' : 'text-red'}">
                        ${latest.adl_value ? Utils.formatNumber(Math.round(latest.adl_value)) : '—'}
                    </span>
                </div>
                <div class="accum-metric">
                    <span class="metric-label">OBV</span>
                    <span class="metric-val ${latest.obv_value > 0 ? 'text-green' : 'text-red'}">
                        ${latest.obv_value ? Utils.formatNumber(Math.round(latest.obv_value)) : '—'}
                    </span>
                </div>
                <div class="accum-metric">
                    <span class="metric-label">MFI</span>
                    <span class="metric-val">${latest.mfi_value ? latest.mfi_value.toFixed(1) : '—'}</span>
                </div>
                <div class="accum-metric">
                    <span class="metric-label">Vol Ratio</span>
                    <span class="metric-val ${(latest.volume_ratio || 0) > 1.5 ? 'text-green' : ''}">
                        ${latest.volume_ratio ? latest.volume_ratio.toFixed(2) + 'x' : '—'}
                    </span>
                </div>
            </div>

            <div class="accum-trend">
                <span class="text-dim" style="font-size:12px">Tren 5 hari:</span>
                <div class="trend-dots">${trendHtml}</div>
            </div>
        `;
    },

    _buildReason(signal) {
        const reasons = [];
        const meta = signal.metadata || {};

        if (signal.signal_type?.includes('divergence')) {
            const ind = meta.indicator || signal.indicator || '';
            const p1 = meta.ind_pivot_1_val;
            const p2 = meta.ind_pivot_2_val;
            if (ind) {
                const dir = signal.signal_type === 'bullish_divergence'
                    ? 'Higher Low' : 'Lower Low';
                reasons.push(`${ind.toUpperCase()}: ${dir}`);
                if (p1 != null && p2 != null) reasons.push(`(${p1.toFixed(1)} → ${p2.toFixed(1)})`);
            }
            const str = meta.divergence_strength;
            if (str != null) reasons.push(`Strength: ${Math.round(str * 100)}%`);
        }

        if (signal.signal_type === 'abc_correction') {
            const b = meta.b_retracement;
            const c = meta.c_extension;
            const fib = meta.fibonacci_precision;
            if (b != null) reasons.push(`Wave B: ${(b * 100).toFixed(1)}% retrace`);
            if (c != null) reasons.push(`Wave C: ${(c * 100).toFixed(1)}% ext`);
            if (fib != null) reasons.push(`Fib: ${Math.round(fib * 100)}%`);
            if (meta.correction_complete) reasons.push('Koreksi selesai ✓');
        }

        if (signal.signal_type === 'accumulation') {
            const m = meta.metadata || meta;
            const parts = [];
            if (m.adl_trend && m.adl_trend !== 'unknown') parts.push(`ADL ${m.adl_trend}`);
            if (m.obv_trend && m.obv_trend !== 'unknown') parts.push(`OBV ${m.obv_trend}`);
            if (m.mfi_level && m.mfi_level !== 'unknown') parts.push(`MFI ${m.mfi_level}`);
            if (m.volume_ratio > 1.5) parts.push(`Vol ${m.volume_ratio.toFixed(1)}x avg`);
            if (parts.length) reasons.push(parts.join(', '));
        }

        if (signal.multi_tf_confirmed) {
            const tfs = (signal.confirmed_timeframes || []).join('+');
            reasons.push(`Multi-TF: ${tfs}`);
        }

        return reasons.length
            ? reasons.join(' · ')
            : 'Sinyal teknikal terdeteksi';
    },

    _scoreColor(pct) {
        if (pct >= 75) return 'rgba(0,212,170,0.2)';
        if (pct >= 60) return 'rgba(245,158,11,0.2)';
        return 'rgba(148,163,184,0.1)';
    }
};