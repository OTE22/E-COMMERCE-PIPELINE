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

        // Initialize page based on current URL
        const path = window.location.pathname;
        const page = path === '/' ? 'dashboard' : path.slice(1);
        const validPages = ['dashboard', 'orders', 'customers', 'products', 'analytics'];

        console.log('[DashboardApp] Initializing with path:', path, 'page:', page);

        if (validPages.includes(page)) {
            console.log('[DashboardApp] Rendering page:', page);
            this.renderPage(page);
        } else {
            console.log('[DashboardApp] Invalid page, falling back to dashboard');
            this.renderPage('dashboard');
        }

        this.checkApiStatus();
    }

    bindEvents() {
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', (e) => {
                e.preventDefault();
                const page = item.dataset.page;
                this.navigateTo(page);
            });
        });

        // Handle Back/Forward browser buttons
        window.addEventListener('popstate', (e) => {
            if (e.state && e.state.page) {
                this.renderPage(e.state.page);
            } else {
                this.renderPage('dashboard');
            }
        });

        // Dashboard Date Range
        document.getElementById('dateRange')?.addEventListener('change', (e) => {
            this.dateRange = parseInt(e.target.value);
            this.loadDashboard();
        });

        // Dashboard Refresh
        document.getElementById('refreshBtn')?.addEventListener('click', () => {
            if (this.currentPage === 'dashboard') this.loadDashboard();
            else if (this.currentPage === 'analytics') this.loadAnalytics();
            else this.navigateTo(this.currentPage);
        });

        // Orders Filters
        document.getElementById('orderStatusFilter')?.addEventListener('change', () => this.loadOrders());
        document.getElementById('orderSearch')?.addEventListener('input', this.debounce(() => this.loadOrders(), 500));

        // Products Filters
        document.getElementById('categoryFilter')?.addEventListener('change', () => this.loadProducts());
        document.getElementById('productSearch')?.addEventListener('input', this.debounce(() => this.loadProducts(), 500));
    }

    navigateTo(page) {
        if (this.currentPage === page) return;

        // Update URL
        const url = page === 'dashboard' ? '/' : `/${page}`;
        window.history.pushState({ page }, '', url);

        this.renderPage(page);
    }

    renderPage(page) {
        document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
        document.querySelector(`[data-page="${page}"]`)?.classList.add('active');

        document.querySelectorAll('.dashboard-content, .page-content').forEach(p => p.classList.add('hidden'));
        document.getElementById(`${page}Page`)?.classList.remove('hidden');

        document.querySelector('.page-title').textContent = page.charAt(0).toUpperCase() + page.slice(1);
        this.currentPage = page;

        if (page === 'dashboard') this.loadDashboard();
        else if (page === 'orders') this.loadOrders();
        else if (page === 'customers') this.loadCustomers();
        else if (page === 'products') this.loadProducts();
        else if (page === 'analytics') this.loadAnalytics();
    }

    getDateRangeParams() {
        const end = new Date();
        const start = new Date();
        start.setDate(end.getDate() - this.dateRange);
        return {
            startDate: start.toISOString().split('T')[0],
            endDate: end.toISOString().split('T')[0]
        };
    }

    async loadDashboard() {
        try {
            const { startDate, endDate } = this.getDateRangeParams();

            // Parallel data fetching
            const [overview, trend, category, hourly, segments, products, orders] = await Promise.all([
                api.getSalesOverview(startDate, endDate),
                api.getSalesTrend(startDate, endDate),
                api.getSalesByCategory(startDate, endDate),
                api.getHourlyDistribution(startDate, endDate),
                api.getSegmentDistribution(),
                api.getTopSellingProducts(5, startDate, endDate),
                api.getOrders(1, 5) // Recent orders
            ]);

            // Update KPI Cards
            document.getElementById('totalRevenue').textContent = formatCurrency(overview.total_revenue);
            document.getElementById('totalOrders').textContent = overview.total_orders.toLocaleString();
            document.getElementById('totalCustomers').textContent = overview.total_customers.toLocaleString();
            document.getElementById('avgOrderValue').textContent = formatCurrency(overview.avg_order_value);

            document.getElementById('revenueChange').textContent = `${overview.revenue_growth >= 0 ? '+' : ''}${overview.revenue_growth}%`;
            document.getElementById('ordersChange').textContent = `${overview.orders_growth >= 0 ? '+' : ''}${overview.orders_growth}%`;

            // Update Charts
            createRevenueTrendChart('revenueTrendChart', trend.data || trend);
            createCategoryChart('categoryChart', category);
            createHourlyChart('hourlyChart', hourly);
            createSegmentChart('segmentChart', segments);

            // Update Tables
            this.renderTopProducts(products);
            this.renderRecentOrders(orders.items || orders);

        } catch (error) {
            console.error(error);
            this.showToast('Failed to load dashboard data', 'error');
            // Fallback to mock data if API fails completely
            this.loadDashboardMock();
        }
    }

    // Fallback method
    loadDashboardMock() {
        const data = mockData;
        createRevenueTrendChart('revenueTrendChart', data.salesTrend.data);
        createCategoryChart('categoryChart', data.categoryData);
        // ... rest of mock loading triggers ...
    }

    renderTopProducts(products) {
        const tbody = document.getElementById('topProductsTable');
        if (!tbody) return;
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
        if (!tbody) return;
        tbody.innerHTML = orders.map(o => `
            <tr>
                <td>${o.order_number}</td>
                <td>${o.customer || o.customer_id}</td>
                <td>${formatCurrency(o.amount || o.total_amount)}</td>
                <td><span class="status-badge ${o.status}">${o.status}</span></td>
            </tr>
        `).join('');
    }

    async loadOrders() {
        const tbody = document.getElementById('ordersTableBody');
        tbody.innerHTML = '<tr><td colspan="7">Loading orders...</td></tr>';

        const status = document.getElementById('orderStatusFilter')?.value;
        const search = document.getElementById('orderSearch')?.value;

        try {
            const response = await api.getOrders(1, 20, { status, search });
            if (response.items.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7">No orders found</td></tr>';
                return;
            }
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
            tbody.innerHTML = '<tr><td colspan="7">Error loading orders</td></tr>';
        }
    }

    async loadAnalytics() {
        try {
            const { startDate, endDate } = this.getDateRangeParams();
            const trend = await api.getSalesTrend(startDate, endDate);

            // Dual Axis Chart (Revenue vs Orders)
            createDualAxisChart('dualAxisChart', trend.data || trend);

            // Mock Day of Week Data (derived or static for now)
            const dowData = [150, 230, 180, 220, 290, 310, 250];
            createDoWChart('dowChart', dowData);

            // Mock Payment Methods
            const paymentData = [
                { method: 'Credit Card', percentage: 65 },
                { method: 'PayPal', percentage: 20 },
                { method: 'Apple Pay', percentage: 10 },
                { method: 'Crypto', percentage: 5 }
            ];
            createPaymentChart('paymentChart', paymentData);

        } catch (error) {
            console.error('Failed to load analytics', error);
        }
    }

    async loadCustomers(page = 1) {
        const tbody = document.getElementById('customersTableBody');
        tbody.innerHTML = '<tr><td colspan="7" class="loading">Loading customers...</td></tr>';

        const segment = document.getElementById('customerSegmentFilter')?.value;
        const search = document.getElementById('customerSearch')?.value;

        try {
            const response = await api.getCustomers(page, 15, { segment, search }); // Fetch 15 per page

            // Render Metrics (Mocked for now, or fetch from separate endpoint)
            // Ideally backend provides summarized metrics
            const metrics = await api.getSegmentDistribution();
            const vip = metrics.find(s => s.segment === 'vip')?.count || 0;
            const atRisk = metrics.find(s => s.segment === 'at_risk')?.count || 0;

            document.getElementById('vipCount').innerText = vip.toLocaleString();
            document.getElementById('atRiskCount').innerText = atRisk.toLocaleString();

            // Calculate weighted average LTV from all segments
            const totalCustomers = metrics.reduce((sum, s) => sum + s.count, 0);
            const totalLtvValue = metrics.reduce((sum, s) => sum + (s.avg_ltv * s.count), 0);
            const avgLtv = totalCustomers > 0 ? totalLtvValue / totalCustomers : 0;
            document.getElementById('avgLtv').innerText = formatCurrency(avgLtv);

            if (response.items.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" class="loading">No customers found</td></tr>';
                return;
            }

            tbody.innerHTML = response.items.map(c => `
                <tr>
                    <td>
                        <div style="font-weight: 500">${c.first_name_masked || 'Unknown'} ${c.last_name_masked || ''}</div>
                        <div style="font-size: 12px; color: var(--text-muted)">${c.customer_key}</div>
                    </td>
                    <td>${(c.email_hash || '').substring(0, 10)}...</td>
                    <td><span class="status-badge ${(c.segment || 'new').toLowerCase().replace(' ', '_')}">${c.segment || 'New'}</span></td>
                    <td>${c.total_orders}</td>
                    <td>${formatCurrency(c.lifetime_value)}</td>
                    <td>${c.last_order_date ? new Date(c.last_order_date).toLocaleDateString() : 'N/A'}</td>
                    <td>
                        <button class="btn btn-sm" onclick="alert('View customer ${c.customer_id}')">View</button>
                    </td>
                </tr>
            `).join('');

            // Render Pagination
            this.renderPagination('customersPagination', response.page, response.pages, (p) => this.loadCustomers(p));

            // Render Cohort Chart (re-use existing logic)
            createSegmentChart('cohortChart', metrics);

            // Mock LTV Chart Data
            const ltvData = {
                labels: ['0-100', '100-500', '500-1000', '1000+'],
                data: [40, 30, 20, 10] // percentages
            };
            createPieChart('ltvChart', ltvData, 'LTV Distribution');

        } catch (error) {
            console.error('Failed to load customers:', error);
            tbody.innerHTML = '<tr><td colspan="7" class="loading">Error loading customer data</td></tr>';
        }
    }

    renderPagination(elementId, currentPage, totalPages, onPageChange) {
        const container = document.getElementById(elementId);
        if (!container) return;

        container.innerHTML = '';

        if (totalPages <= 1) return;

        // Prev Button
        const prevBtn = document.createElement('button');
        prevBtn.innerText = '←';
        prevBtn.disabled = currentPage === 1;
        prevBtn.onclick = () => onPageChange(currentPage - 1);
        container.appendChild(prevBtn);

        // Page Numbers (Simplified)
        for (let i = Math.max(1, currentPage - 2); i <= Math.min(totalPages, currentPage + 2); i++) {
            const btn = document.createElement('button');
            btn.innerText = i;
            if (i === currentPage) btn.classList.add('active');
            btn.onclick = () => onPageChange(i);
            container.appendChild(btn);
        }

        // Next Button
        const nextBtn = document.createElement('button');
        nextBtn.innerText = '→';
        nextBtn.disabled = currentPage === totalPages;
        nextBtn.onclick = () => onPageChange(currentPage + 1);
        container.appendChild(nextBtn);
    }

    async loadProducts() {
        const tbody = document.getElementById('productsTableBody');
        tbody.innerHTML = '<tr><td colspan="7">Loading products...</td></tr>';

        const category = document.getElementById('categoryFilter')?.value;
        const search = document.getElementById('productSearch')?.value;

        try {
            const response = await api.getProducts(1, 20, { category, search });
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
            tbody.innerHTML = '<tr><td colspan="7">Error loading products</td></tr>';
        }
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

    showToast(message, type = 'info') {
        const container = document.getElementById('toastContainer');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        container.appendChild(toast);
        setTimeout(() => toast.remove(), 5000);
    }

    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func.apply(this, args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
}

document.addEventListener('DOMContentLoaded', () => new DashboardApp());


