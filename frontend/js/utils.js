/**
 * Utility functions for the Stock Screener frontend.
 */

const Utils = {
    /**
     * Format large numbers (unbound volume/market cap)
     */
    formatNumber(num) {
        if (num >= 1000000000) return (num / 1000000000).toFixed(2) + 'B';
        if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
        if (num >= 1000) return (num / 1000).toFixed(2) + 'K';
        return num.toString();
    },

    /**
     * Format currency/price with IDR standard or custom decimals
     */
    formatPrice(price, decimals = 2) {
        if (price === null || price === undefined) return '—';
        return new Intl.NumberFormat('id-ID', {
            style: 'decimal',
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        }).format(price);
    },

    /**
     * Format date to human readable string
     */
    formatDate(dateStr, showTime = false) {
        if (!dateStr) return '—';
        const date = new Date(dateStr);
        const options = { year: 'numeric', month: 'short', day: 'numeric' };
        if (showTime) {
            options.hour = '2-digit';
            options.minute = '2-digit';
        }
        return date.toLocaleDateString('en-US', options);
    },

    /**
     * Get CSS class for signal type
     */
    getSignalClass(type) {
        const classes = {
            'bullish_divergence': 'bullish',
            'hidden_bullish_divergence': 'hidden',
            'abc_correction': 'abc',
            'accumulation': 'accum',
            'distribution': 'dist'
        };
        return classes[type] || '';
    },

    /**
     * Format signal type name for display
     */
    formatSignalType(type) {
        return type.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
    },

    /**
     * Show a toast notification
     */
    showNotification(message, type = 'info') {
        const container = document.getElementById('notification-container');
        const toast = document.createElement('div');
        toast.className = `toast glass animate-up ${type}`;
        
        const icon = type === 'success' ? 'check-circle' : 
                     type === 'error' ? 'exclamation-circle' : 
                     type === 'warning' ? 'exclamation-triangle' : 'info-circle';
        
        toast.innerHTML = `
            <i class="fas fa-${icon}"></i>
            <span>${message}</span>
        `;
        
        container.appendChild(toast);
        
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(20px)';
            setTimeout(() => toast.remove(), 500);
        }, 4000);
    },

    /**
     * API Request helper
     */
    async apiRequest(endpoint, options = {}) {
        try {
            const response = await fetch(endpoint, options);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            return await response.json();
        } catch (error) {
            console.error(`API Request failed for ${endpoint}:`, error);
            this.showNotification(`Error: ${error.message}`, 'error');
            return null;
        }
    }
};

// Add standard toast styling dynamically if needed or keep in CSS
