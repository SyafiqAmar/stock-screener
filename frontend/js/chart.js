/**
 * Charting logic using TradingView Lightweight Charts.
 * Handles candlestick charts, indicators, and signal markers.
 */

const Chart = {
    charts: {}, // { main, rsi, macd }
    series: {}, // { candle, volume, rsi, macd, macd_signal, macd_hist }
    currentSymbol: null,
    currentTimeframe: '1d',

    init() {
        this.createCharts();
        this.setupEventListeners();
    },

    createCharts() {
        const commonOptions = {
            layout: {
                background: { color: 'transparent' },
                textColor: '#94a3b8',
            },
            grid: {
                vertLines: { color: 'rgba(255, 255, 255, 0.05)' },
                horzLines: { color: 'rgba(255, 255, 255, 0.05)' },
            },
            timeScale: {
                borderColor: 'rgba(255, 255, 255, 0.1)',
                timeVisible: true,
            },
            rightPriceScale: {
                borderColor: 'rgba(255, 255, 255, 0.1)',
            },
        };

        // Main Chart
        const mainContainer = document.getElementById('main-chart-container');
        if (!mainContainer) return;
        
        this.charts.main = LightweightCharts.createChart(mainContainer, {
            ...commonOptions,
            height: 400,
        });

        this.series.candle = this.charts.main.addCandlestickSeries({
            upColor: '#00d4aa',
            downColor: '#ef4444',
            borderVisible: false,
            wickUpColor: '#00d4aa',
            wickDownColor: '#ef4444',
        });

        this.series.volume = this.charts.main.addHistogramSeries({
            color: 'rgba(0, 212, 170, 0.3)',
            priceFormat: { type: 'volume' },
            priceScaleId: '', // overlay
        });

        this.series.volume.priceScale().applyOptions({
            scaleMargins: { top: 0.8, bottom: 0 },
        });

        // RSI Chart
        const rsiContainer = document.getElementById('rsi-chart-container');
        if (rsiContainer) {
            this.charts.rsi = LightweightCharts.createChart(rsiContainer, {
                ...commonOptions,
                height: 120,
            });
            this.series.rsi = this.charts.rsi.addLineSeries({
                color: '#9d50bb',
                lineWidth: 2,
                title: 'RSI (14)',
            });
            // Overbought/Oversold lines
            this.charts.rsi.addLineSeries({ color: 'rgba(255, 255, 255, 0.1)', lineStyle: 2 }).setData([{time:0, value:70}, {time:9999999999, value:70}]);
            this.charts.rsi.addLineSeries({ color: 'rgba(255, 255, 255, 0.1)', lineStyle: 2 }).setData([{time:0, value:30}, {time:9999999999, value:30}]);
        }

        // Sync timescales
        if (this.charts.rsi) {
            let isSyncing = false;
            this.charts.main.timeScale().subscribeVisibleTimeRangeChange((range) => {
                if (!range || isSyncing) return;
                isSyncing = true;
                this.charts.rsi.timeScale().setVisibleRange(range);
                isSyncing = false;
            });
            this.charts.rsi.timeScale().subscribeVisibleTimeRangeChange((range) => {
                if (!range || isSyncing) return;
                isSyncing = true;
                this.charts.main.timeScale().setVisibleRange(range);
                isSyncing = false;
            });
        }
    },

    setupEventListeners() {
        const tfBtns = document.querySelectorAll('.tf-btn');
        tfBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                tfBtns.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.currentTimeframe = btn.dataset.tf;
                if (this.currentSymbol) this.loadData(this.currentSymbol, this.currentTimeframe);
            });
        });

        window.addEventListener('resize', () => {
            if (this.charts.main) {
                const container = document.getElementById('main-chart-container');
                if (container) this.charts.main.resize(container.clientWidth, 400);
            }
            if (this.charts.rsi) {
                const container = document.getElementById('rsi-chart-container');
                if (container) this.charts.rsi.resize(container.clientWidth, 120);
            }
        });
    },

    async loadData(symbol, timeframe) {
        this.currentSymbol = symbol;
        this.currentTimeframe = timeframe;

        document.getElementById('chart-ticker-symbol').textContent = symbol;
        
        // Fetch OHLCV
        const ohlcv = await Utils.apiRequest(`/api/chart/${symbol}?timeframe=${timeframe}&limit=500`);
        if (ohlcv && ohlcv.data) {
            this.series.candle.setData(ohlcv.data);
            this.series.volume.setData(ohlcv.data.map(d => ({
                time: d.time,
                value: d.volume,
                color: d.close >= d.open ? 'rgba(0, 212, 170, 0.3)' : 'rgba(239, 68, 68, 0.3)'
            })));
        }

        // Fetch Indicators
        const indicators = await Utils.apiRequest(`/api/chart/${symbol}/indicators?timeframe=${timeframe}&limit=500`);
        if (indicators && indicators.indicators && this.series.rsi) {
            const rsiData = indicators.indicators
                .filter(i => i.rsi_14 !== null)
                .map(i => ({ time: i.time, value: i.rsi_14 }));
            this.series.rsi.setData(rsiData);
        }

        // Fetch Signals for markers
        const signalData = await Utils.apiRequest(`/api/chart/${symbol}/signals`);
        if (signalData && signalData.signals) {
            this.renderMarkers(signalData.signals.filter(s => s.timeframe === timeframe));
            this.renderAnalysisDetails(signalData.signals);
        }
    },

    renderMarkers(signals) {
        if (!this.series.candle) return;
        
        const markers = signals.map(s => {
            const isBullish = s.signal_type.includes('bullish') || s.signal_type === 'abc_correction';
            return {
                time: s.date, // Note: backend dates need to match chart time format (Unix ts or ISO)
                position: isBullish ? 'belowBar' : 'aboveBar',
                color: isBullish ? '#00d4aa' : '#ef4444',
                shape: isBullish ? 'arrowUp' : 'arrowDown',
                text: Utils.formatSignalType(s.signal_type).split(' ')[0],
            };
        });

        this.series.candle.setMarkers(markers);
    },

    renderAnalysisDetails(signals) {
        const list = document.getElementById('ticker-signal-list');
        if (!list) return;
        
        list.innerHTML = '';
        if (signals.length === 0) {
            list.innerHTML = '<div class="empty-state">No signals found for this ticker.</div>';
            return;
        }

        signals.forEach((s, idx) => {
            const item = document.createElement('div');
            item.className = 'signal-item glass animate-up';
            item.style.animationDelay = `${idx * 0.1}s`;
            
            const signalClass = Utils.getSignalClass(s.signal_type);
            const scorePercent = Math.round(s.confidence_score * 100);

            item.innerHTML = `
                <div class="signal-item-header">
                    <span class="badge-signal ${signalClass}">${Utils.formatSignalType(s.signal_type)}</span>
                    <span class="score">${scorePercent}%</span>
                </div>
                <div class="signal-item-body">
                    <div class="meta-row">
                        <span>Timeframe:</span>
                        <span>${s.timeframe}</span>
                    </div>
                    <div class="meta-row">
                        <span>Detected:</span>
                        <span>${Utils.formatDate(s.detected_at)}</span>
                    </div>
                </div>
            `;
            list.appendChild(item);
        });
    }
};
