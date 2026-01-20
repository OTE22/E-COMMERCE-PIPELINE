/**
 * Chart Utilities for E-Commerce Analytics
 */

Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.color = '#a0aec0';

const chartColors = {
    primary: '#6366f1',
    success: '#10b981',
    warning: '#f59e0b',
    danger: '#ef4444',
    info: '#3b82f6',
    palette: ['#6366f1', '#8b5cf6', '#10b981', '#f59e0b', '#ef4444', '#3b82f6']
};

const charts = {};

function createRevenueTrendChart(canvasId, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    if (charts[canvasId]) charts[canvasId].destroy();

    const gradient = ctx.getContext('2d').createLinearGradient(0, 0, 0, 300);
    gradient.addColorStop(0, 'rgba(99, 102, 241, 0.4)');
    gradient.addColorStop(1, 'rgba(99, 102, 241, 0)');

    charts[canvasId] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => new Date(d.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })),
            datasets: [{
                label: 'Revenue',
                data: data.map(d => d.revenue),
                borderColor: chartColors.primary,
                backgroundColor: gradient,
                borderWidth: 3,
                fill: true,
                tension: 0.4,
                pointRadius: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { display: false } },
                y: { grid: { color: 'rgba(255,255,255,0.05)' } }
            }
        }
    });
    return charts[canvasId];
}

function createCategoryChart(canvasId, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    if (charts[canvasId]) charts[canvasId].destroy();

    charts[canvasId] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: data.map(d => d.category),
            datasets: [{ data: data.map(d => d.revenue), backgroundColor: chartColors.palette, borderWidth: 0 }]
        },
        options: { responsive: true, maintainAspectRatio: false, cutout: '65%' }
    });
    return charts[canvasId];
}

function createHourlyChart(canvasId, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    if (charts[canvasId]) charts[canvasId].destroy();

    charts[canvasId] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => `${d.hour}:00`),
            datasets: [{ label: 'Orders', data: data.map(d => d.orders), backgroundColor: chartColors.info, borderRadius: 4 }]
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
    });
    return charts[canvasId];
}

function createSegmentChart(canvasId, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    if (charts[canvasId]) charts[canvasId].destroy();

    charts[canvasId] = new Chart(ctx, {
        type: 'polarArea',
        data: {
            labels: data.map(d => d.segment),
            datasets: [{ data: data.map(d => d.count), backgroundColor: chartColors.palette, borderWidth: 0 }]
        },
        options: { responsive: true, maintainAspectRatio: false }
    });
    return charts[canvasId];
}

function formatCurrency(value) {
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(value);
}

window.charts = charts;
window.createRevenueTrendChart = createRevenueTrendChart;
window.createCategoryChart = createCategoryChart;
window.createHourlyChart = createHourlyChart;
window.createSegmentChart = createSegmentChart;
window.formatCurrency = formatCurrency;
