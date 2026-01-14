/**
 * LL Analytics - Main Application JavaScript
 */

// API helper functions
const API = {
    baseUrl: '/api',

    async get(endpoint) {
        const response = await fetch(`${this.baseUrl}${endpoint}`);
        if (!response.ok) {
            throw new Error(`API error: ${response.status}`);
        }
        return response.json();
    },

    async post(endpoint, data) {
        const response = await fetch(`${this.baseUrl}${endpoint}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data),
        });
        if (!response.ok) {
            throw new Error(`API error: ${response.status}`);
        }
        return response.json();
    },

    // Specific endpoints
    async getMetrics() {
        return this.get('/metrics');
    },

    async getMetric(metricId) {
        return this.get(`/metrics/${metricId}`);
    },

    async calculateMetric(metricId, scope, params) {
        const queryParams = new URLSearchParams(params).toString();
        return this.get(`/metrics/${metricId}/${scope}?${queryParams}`);
    },

    async getPlayers(search = '', limit = 100) {
        const params = new URLSearchParams({ limit });
        if (search) params.append('search', search);
        return this.get(`/players?${params}`);
    },

    async getPlayer(username) {
        return this.get(`/players/${username}`);
    },

    async getSeasons() {
        return this.get('/seasons');
    },

    async getSeason(seasonNumber) {
        return this.get(`/seasons/${seasonNumber}`);
    },
};

// Utility functions
const Utils = {
    // Format a number with sign
    formatSigned(num, decimals = 2) {
        if (num === null || num === undefined) return '-';
        const formatted = num.toFixed(decimals);
        return num >= 0 ? `+${formatted}` : formatted;
    },

    // Format percentage
    formatPercent(num, decimals = 1) {
        if (num === null || num === undefined) return '-';
        return `${(num * 100).toFixed(decimals)}%`;
    },

    // Debounce function
    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },

    // Get URL parameters
    getUrlParams() {
        return Object.fromEntries(new URLSearchParams(window.location.search));
    },

    // Set URL parameters without reload
    setUrlParams(params) {
        const url = new URL(window.location);
        Object.entries(params).forEach(([key, value]) => {
            if (value) {
                url.searchParams.set(key, value);
            } else {
                url.searchParams.delete(key);
            }
        });
        window.history.replaceState({}, '', url);
    },
};

// Alpine.js store for global state
document.addEventListener('alpine:init', () => {
    Alpine.store('app', {
        metrics: [],
        seasons: [],
        loading: false,

        async init() {
            this.loading = true;
            try {
                const [metricsData, seasonsData] = await Promise.all([
                    API.getMetrics(),
                    API.getSeasons(),
                ]);
                this.metrics = metricsData.metrics;
                this.seasons = seasonsData.seasons;
            } catch (err) {
                console.error('Failed to initialize app:', err);
            } finally {
                this.loading = false;
            }
        },

        getMetricById(id) {
            return this.metrics.find(m => m.id === id);
        },

        getSeasonByNumber(num) {
            return this.seasons.find(s => s.season_number === num);
        },
    });
});

// Initialize app store when page loads
document.addEventListener('DOMContentLoaded', () => {
    if (window.Alpine) {
        Alpine.store('app').init();
    }
});

// Common Alpine.js components
window.LLComponents = {
    // Player search component
    playerSearch() {
        return {
            query: '',
            results: [],
            loading: false,
            showResults: false,

            async search() {
                if (this.query.length < 2) {
                    this.results = [];
                    return;
                }

                this.loading = true;
                try {
                    const data = await API.getPlayers(this.query, 10);
                    this.results = data.players;
                    this.showResults = true;
                } catch (err) {
                    console.error('Search failed:', err);
                } finally {
                    this.loading = false;
                }
            },

            selectPlayer(username) {
                window.location.href = `/players/${username}`;
            },

            debouncedSearch: Utils.debounce(function() {
                this.search();
            }, 300),
        };
    },

    // Metric selector component
    metricSelector() {
        return {
            selectedMetric: '',
            metrics: [],

            init() {
                this.metrics = Alpine.store('app').metrics;

                // Check URL for initial selection
                const params = Utils.getUrlParams();
                if (params.metric) {
                    this.selectedMetric = params.metric;
                }
            },

            onChange() {
                Utils.setUrlParams({ metric: this.selectedMetric });
                this.$dispatch('metric-changed', { metric: this.selectedMetric });
            },
        };
    },

    // Season selector component
    seasonSelector() {
        return {
            selectedSeason: '',
            seasons: [],

            init() {
                this.seasons = Alpine.store('app').seasons;

                // Check URL for initial selection
                const params = Utils.getUrlParams();
                if (params.season) {
                    this.selectedSeason = params.season;
                }
            },

            onChange() {
                Utils.setUrlParams({ season: this.selectedSeason });
                this.$dispatch('season-changed', { season: this.selectedSeason });
            },
        };
    },
};

// Global player search component for navbar
function playerSearch() {
    return {
        query: '',
        results: [],
        loading: false,
        showResults: false,
        selectedIndex: -1,

        async search() {
            if (this.query.length < 1) {
                this.results = [];
                this.showResults = false;
                return;
            }

            this.loading = true;
            try {
                const response = await fetch(`/api/players/search/autocomplete?q=${encodeURIComponent(this.query)}`);
                const data = await response.json();
                this.results = data.results;
                this.showResults = this.results.length > 0;
                this.selectedIndex = -1;
            } catch (err) {
                console.error('Search failed:', err);
            } finally {
                this.loading = false;
            }
        },

        goToFirst() {
            if (this.results.length > 0) {
                const target = this.selectedIndex >= 0 ? this.results[this.selectedIndex] : this.results[0];
                window.location.href = `/player/${target}`;
            }
        },

        selectNext() {
            if (this.results.length > 0) {
                this.selectedIndex = (this.selectedIndex + 1) % this.results.length;
            }
        },

        selectPrev() {
            if (this.results.length > 0) {
                this.selectedIndex = this.selectedIndex <= 0 ? this.results.length - 1 : this.selectedIndex - 1;
            }
        }
    };
}

// Export for use in templates
window.API = API;
window.Utils = Utils;
window.playerSearch = playerSearch;
