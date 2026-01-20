/**
 * E-Commerce Analytics Dashboard App
 */

class DashboardApp {
    constructor() {
        this.currentPage = 'dashboard';
        this.dateRange = 30;
        this.init();
    }

    init() {
        this.bindEvents();
        this.loadDashboard();
        this.checkApiStatus();
    }

    bindEvents() {
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', (e) => {
                e.preventDefault();
                this.navigateTo(item.dataset.page);
            });
        });

        document.getElementById('dateRange')?.addEventListener('change', (e) => {
            this.dateRange = parseInt(e.target.value);
            this.loadDashboard();
        });

        document.getElementById('refreshBtn')?.addEventListener('click', () => this.loadDashboard());
    }

    navigateTo(page) {
        document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
        document.querySelector(`[data-page="${page}"]`)?.classList.add('active');

        document.querySelectorAll('.dashboard-content, .page-content').forEach(p => p.classList.add('hidden'));
        document.getElementById(`${page}Page`)?.classList.remove('hidden');

        document.querySelector('.page-title').textContent = page.charAt(0).toUpperCase() + page.slice(1);
        this.currentPage = page;

        if (page === 'orders') this.loadOrders();
        else if (page === 'customers') this.loadCustomers();
        else if (page === 'products') this.loadProducts();
    }

    async checkApiStatus() {
        try {
            await api.checkHealth();
            document.querySelector('.status-dot').classList.add('online');
            document.querySelector('.status-text').textContent = 'API Connected';
        } catch {
            document.querySelector('.status-dot').classList.remove('online');
            document.querySelector('.status-text').textContent = 'API Offline';
        }
    }

    async loadDashboard() {
        try {
            const data = mockData;

            document.getElementById('totalRevenue').textContent = formatCurrency(data.salesOverview.total_revenue);
            document.getElementById('totalOrders').textContent = data.salesOverview.total_orders.toLocaleString();
            document.getElementById('totalCustomers').textContent = data.salesOverview.total_customers.toLocaleString();
            document.getElementById('avgOrderValue').textContent = formatCurrency(data.salesOverview.avg_order_value);

            document.getElementById('revenueChange').textContent = `+${data.salesOverview.revenue_growth}%`;
            document.getElementById('ordersChange').textContent = `+${data.salesOverview.orders_growth}%`;

            createRevenueTrendChart('revenueTrendChart', data.salesTrend.data);
            createCategoryChart('categoryChart', data.categoryData);
            createHourlyChart('hourlyChart', data.hourlyData);
            createSegmentChart('segmentChart', data.segments);

            this.renderTopProducts(data.topProducts);
            this.renderRecentOrders(data.recentOrders);
        } catch (error) {
            this.showToast('Failed to load dashboard data', 'error');
        }
    }

    renderTopProducts(products) {
        const tbody = document.getElementById('topProductsTable');
        tbody.innerHTML = products.map(p => `
            <tr>
                <td>${p.name}</td>
                <td>${p.category}</td>
                <td>${p.units_sold.toLocaleString()}</td>
                <td>${formatCurrency(p.revenue)}</td>
            </tr>
        `).join('');
    }

    renderRecentOrders(orders) {
        const tbody = document.getElementById('recentOrdersTable');
        tbody.innerHTML = orders.map(o => `
            <tr>
                <td>${o.order_number}</td>
                <td>${o.customer}</td>
                <td>${formatCurrency(o.amount)}</td>
                <td><span class="status-badge ${o.status}">${o.status}</span></td>
            </tr>
        `).join('');
    }

    async loadOrders() {
        const tbody = document.getElementById('ordersTableBody');
        tbody.innerHTML = '<tr><td colspan="7">Loading orders...</td></tr>';

        try {
            const response = await api.getOrders(1, 20);
            tbody.innerHTML = response.items.map(o => `
                <tr>
                    <td>${o.order_number}</td>
                    <td>${new Date(o.order_timestamp).toLocaleDateString()}</td>
                    <td>${o.customer_id.slice(0, 8)}...</td>
                    <td>${o.item_count}</td>
                    <td>${formatCurrency(o.total_amount)}</td>
                    <td><span class="status-badge ${o.status}">${o.status}</span></td>
                    <td><button class="btn btn-sm">View</button></td>
                </tr>
            `).join('');
        } catch {
            tbody.innerHTML = '<tr><td colspan="7">Using demo data</td></tr>';
        }
    }

    async loadCustomers() {
        try {
            const metrics = mockData.segments;
            document.getElementById('vipCount').textContent = metrics.find(s => s.segment === 'VIP')?.count || 0;
            document.getElementById('atRiskCount').textContent = metrics.find(s => s.segment === 'At Risk')?.count || 0;
            document.getElementById('avgLtv').textContent = formatCurrency(450);
        } catch (error) {
            console.error('Failed to load customers:', error);
        }
    }

    async loadProducts() {
        const tbody = document.getElementById('productsTableBody');
        tbody.innerHTML = '<tr><td colspan="7">Loading products...</td></tr>';

        try {
            const response = await api.getProducts(1, 20);
            tbody.innerHTML = response.items.map(p => `
                <tr>
                    <td>${p.sku}</td>
                    <td>${p.name}</td>
                    <td>${p.category}</td>
                    <td>${formatCurrency(p.unit_price)}</td>
                    <td>${p.stock_quantity}</td>
                    <td>${p.avg_rating?.toFixed(1) || 'N/A'}</td>
                    <td><span class="stock-badge ${p.is_in_stock ? 'in-stock' : 'out-of-stock'}">${p.is_in_stock ? 'In Stock' : 'Out'}</span></td>
                </tr>
            `).join('');
        } catch {
            tbody.innerHTML = '<tr><td colspan="7">Using demo data - API offline</td></tr>';
        }
    }

    showToast(message, type = 'info') {
        const container = document.getElementById('toastContainer');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        container.appendChild(toast);
        setTimeout(() => toast.remove(), 5000);
    }
}

document.addEventListener('DOMContentLoaded', () => new DashboardApp());
