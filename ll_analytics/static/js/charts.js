/**
 * Chart.js helpers for LL Analytics
 */

// Default chart colors (emerald primary for dark theme)
const CHART_COLORS = {
    primary: 'rgb(52, 211, 153)',      // Emerald-400
    secondary: 'rgb(96, 165, 250)',    // Blue-400
    tertiary: 'rgb(250, 204, 21)',     // Yellow-400
    danger: 'rgb(248, 113, 113)',      // Red-400
    gray: 'rgb(148, 163, 184)',        // Slate-400
};

// Color palette for multiple series
const COLOR_PALETTE = [
    CHART_COLORS.primary,
    CHART_COLORS.secondary,
    CHART_COLORS.tertiary,
    CHART_COLORS.danger,
    'rgb(167, 139, 250)',  // Purple-400
    'rgb(244, 114, 182)',  // Pink-400
    'rgb(34, 211, 238)',   // Cyan-400
    'rgb(251, 146, 60)',   // Orange-400
];

// Set Chart.js defaults for dark backgrounds
Chart.defaults.color = '#94a3b8';           // slate-400 for text
Chart.defaults.borderColor = '#1e293b';     // slate-800 for grid lines
Chart.defaults.backgroundColor = 'transparent';
Chart.defaults.plugins.legend.labels.color = '#cbd5e1'; // slate-300
Chart.defaults.plugins.title.color = '#e2e8f0';         // slate-200
Chart.defaults.scale.grid = {
    color: 'rgba(51, 65, 85, 0.5)',  // slate-700 at 50%
};
Chart.defaults.scale.ticks = {
    color: '#94a3b8', // slate-400
};

/**
 * Create a line chart for time series data
 */
function createLineChart(ctx, data, options = {}) {
    const config = {
        type: 'line',
        data: {
            labels: data.labels,
            datasets: data.datasets.map((ds, i) => ({
                ...ds,
                borderColor: ds.borderColor || COLOR_PALETTE[i % COLOR_PALETTE.length],
                backgroundColor: ds.backgroundColor || 'transparent',
                tension: 0.1,
                pointRadius: 3,
                pointHoverRadius: 5,
            })),
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: data.datasets.length > 1,
                },
                title: {
                    display: !!options.title,
                    text: options.title,
                },
            },
            scales: {
                x: {
                    title: {
                        display: !!options.xAxisLabel,
                        text: options.xAxisLabel,
                    },
                },
                y: {
                    title: {
                        display: !!options.yAxisLabel,
                        text: options.yAxisLabel,
                    },
                },
            },
            ...options,
        },
    };

    return new Chart(ctx, config);
}

/**
 * Create a bar chart for comparisons
 */
function createBarChart(ctx, data, options = {}) {
    const config = {
        type: 'bar',
        data: {
            labels: data.labels,
            datasets: data.datasets.map((ds, i) => ({
                ...ds,
                backgroundColor: ds.backgroundColor || COLOR_PALETTE[i % COLOR_PALETTE.length],
            })),
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: data.datasets.length > 1,
                },
                title: {
                    display: !!options.title,
                    text: options.title,
                },
            },
            ...options,
        },
    };

    return new Chart(ctx, config);
}

/**
 * Create a scatter plot for correlations
 */
function createScatterChart(ctx, data, options = {}) {
    const config = {
        type: 'scatter',
        data: {
            datasets: data.datasets.map((ds, i) => ({
                ...ds,
                backgroundColor: ds.backgroundColor || COLOR_PALETTE[i % COLOR_PALETTE.length],
                pointRadius: 5,
                pointHoverRadius: 7,
            })),
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: !!options.title,
                    text: options.title,
                },
            },
            scales: {
                x: {
                    title: {
                        display: !!options.xAxisLabel,
                        text: options.xAxisLabel,
                    },
                },
                y: {
                    title: {
                        display: !!options.yAxisLabel,
                        text: options.yAxisLabel,
                    },
                },
            },
            ...options,
        },
    };

    return new Chart(ctx, config);
}

/**
 * Create a histogram for distributions
 */
function createHistogram(ctx, data, options = {}) {
    // Calculate histogram bins
    const values = data.values;
    const binCount = options.bins || 20;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const binWidth = (max - min) / binCount;

    const bins = new Array(binCount).fill(0);
    const binLabels = [];

    for (let i = 0; i < binCount; i++) {
        const binStart = min + i * binWidth;
        binLabels.push(binStart.toFixed(2));
    }

    values.forEach(v => {
        const binIndex = Math.min(Math.floor((v - min) / binWidth), binCount - 1);
        bins[binIndex]++;
    });

    return createBarChart(ctx, {
        labels: binLabels,
        datasets: [{
            label: data.label || 'Distribution',
            data: bins,
        }],
    }, options);
}

/**
 * Render a metric result based on its visualization type
 */
function renderMetricChart(containerId, result) {
    const container = document.getElementById(containerId);
    if (!container) return null;

    // Clear existing content
    container.innerHTML = '<canvas></canvas>';
    const ctx = container.querySelector('canvas').getContext('2d');

    const chartConfig = result.chart_config || {};

    switch (result.visualization) {
        case 'line':
            if (result.data?.details) {
                return createLineChart(ctx, {
                    labels: result.data.details.map(d => `Day ${d.match_day}`),
                    datasets: [{
                        label: 'Cumulative Surprise',
                        data: result.data.details.map(d => d.cumulative),
                    }],
                }, {
                    title: chartConfig.title,
                    yAxisLabel: chartConfig.yAxisLabel,
                });
            }
            break;

        case 'bar':
            if (result.data?.early && result.data?.late) {
                return createBarChart(ctx, {
                    labels: ['Early Season', 'Late Season'],
                    datasets: [{
                        label: 'Average Surprise',
                        data: [result.data.early.avg_surprise, result.data.late.avg_surprise],
                        backgroundColor: [CHART_COLORS.primary, CHART_COLORS.secondary],
                    }],
                }, {
                    title: chartConfig.title || 'Early vs Late Season Performance',
                });
            }
            break;

        case 'histogram':
            if (Array.isArray(result.data)) {
                const values = result.data.map(d => d.total_surprise || d.value || 0);
                return createHistogram(ctx, {
                    values,
                    label: 'Distribution',
                }, {
                    title: chartConfig.title || 'Distribution',
                });
            }
            break;

        default:
            // For leaderboard and other non-chart types, don't render a chart
            container.innerHTML = '';
            return null;
    }

    return null;
}

/**
 * Create a radar chart for category comparisons
 */
function createRadarChart(ctx, data, options = {}) {
    const config = {
        type: 'radar',
        data: {
            labels: data.labels,
            datasets: data.datasets.map((ds, i) => ({
                ...ds,
                borderColor: ds.borderColor || COLOR_PALETTE[i % COLOR_PALETTE.length],
                backgroundColor: ds.backgroundColor || (COLOR_PALETTE[i % COLOR_PALETTE.length].replace('rgb', 'rgba').replace(')', ', 0.15)')),
                pointRadius: 3,
                pointHoverRadius: 5,
            })),
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: data.datasets.length > 1,
                },
                title: {
                    display: !!options.title,
                    text: options.title,
                },
            },
            scales: {
                r: {
                    min: 0,
                    max: options.max || 100,
                    ticks: {
                        stepSize: options.stepSize || 20,
                        color: '#94a3b8',
                        backdropColor: 'transparent',
                    },
                    grid: {
                        color: 'rgba(51, 65, 85, 0.5)',
                    },
                    angleLines: {
                        color: 'rgba(51, 65, 85, 0.5)',
                    },
                    pointLabels: {
                        color: '#cbd5e1',
                    },
                },
            },
            ...options,
        },
    };

    return new Chart(ctx, config);
}

// Export functions for use in other scripts
window.LLCharts = {
    createLineChart,
    createBarChart,
    createScatterChart,
    createHistogram,
    createRadarChart,
    renderMetricChart,
    CHART_COLORS,
    COLOR_PALETTE,
};
