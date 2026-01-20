/**
 * E-Commerce Analytics API Client
 * 
 * Handles all API communication with the FastAPI backend
 */

class ApiClient {
    constructor(baseUrl = 'http://localhost:8000/api/v1') {
        this.baseUrl = baseUrl;
        this.headers = {
            'Content-Type': 'application/json',
        };
    }

    async request(endpoint, options = {}) {
        const url = `${this.baseUrl}${endpoint}`;

        try {
            const response = await fetch(url, {
                ...options,
                headers: {
                    ...this.headers,
                    ...options.headers,
                },
            });

            if (!response.ok) {
                throw new Error(`API Error: ${response.status} ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            console.error(`API Request failed: ${endpoint}`, error);
            throw error;
        }
    }

    // Health Check
    async checkHealth() {
        return this.request('/health');
    }

    // ==========================================
    // Analytics Endpoints
    // ==========================================

    async getSalesOverview(startDate, endDate) {
        const params = new URLSearchParams();
        if (startDate) params.append('start_date', startDate);
        if (endDate) params.append('end_date', endDate);
        return this.request(`/analytics/sales/overview?${params}`);
    }

    async getSalesTrend(startDate, endDate, granularity = 'day') {
        const params = new URLSearchParams({
            granularity,
        });
        if (startDate) params.append('start_date', startDate);
        if (endDate) params.append('end_date', endDate);
        return this.request(`/analytics/sales/trend?${params}`);
    }

    async getSalesByCategory(startDate, endDate) {
        const params = new URLSearchParams();
        if (startDate) params.append('start_date', startDate);
        if (endDate) params.append('end_date', endDate);
        return this.request(`/analytics/sales/by-category?${params}`);
    }

    async getHourlyDistribution(startDate, endDate) {
        const params = new URLSearchParams();
        if (startDate) params.append('start_date', startDate);
        if (endDate) params.append('end_date', endDate);
        return this.request(`/analytics/sales/hourly?${params}`);
    }

    async getCustomerCohorts() {
        return this.request('/analytics/customers/cohorts');
    }

    async getTopSellingProducts(limit = 10, startDate, endDate) {
        const params = new URLSearchParams({ limit });
        if (startDate) params.append('start_date', startDate);
        if (endDate) params.append('end_date', endDate);
        return this.request(`/analytics/products/top-selling?${params}`);
    }

    // ==========================================
    // Orders Endpoints
    // ==========================================

    async getOrders(page = 1, pageSize = 20, filters = {}) {
        const params = new URLSearchParams({
            page,
            page_size: pageSize,
        });

        if (filters.status) params.append('status', filters.status);
        if (filters.startDate) params.append('start_date', filters.startDate);
        if (filters.endDate) params.append('end_date', filters.endDate);
        if (filters.customerId) params.append('customer_id', filters.customerId);

        return this.request(`/orders?${params}`);
    }

    async getOrder(orderId) {
        return this.request(`/orders/${orderId}`);
    }

    async getOrderMetrics(startDate, endDate) {
        const params = new URLSearchParams();
        if (startDate) params.append('start_date', startDate);
        if (endDate) params.append('end_date', endDate);
        return this.request(`/orders/metrics?${params}`);
    }

    // ==========================================
    // Customers Endpoints
    // ==========================================

    async getCustomers(page = 1, pageSize = 20, filters = {}) {
        const params = new URLSearchParams({
            page,
            page_size: pageSize,
        });

        if (filters.segment) params.append('segment', filters.segment);
        if (filters.country) params.append('country', filters.country);
        if (filters.minLtv) params.append('min_ltv', filters.minLtv);

        return this.request(`/customers?${params}`);
    }

    async getCustomer(customerId) {
        return this.request(`/customers/${customerId}`);
    }

    async getCustomerMetrics() {
        return this.request('/customers/metrics');
    }

    async getSegmentDistribution() {
        return this.request('/customers/segments');
    }

    async getTopSpenders(limit = 10) {
        return this.request(`/customers/top-spenders?limit=${limit}`);
    }

    async getAtRiskCustomers() {
        return this.request('/customers/at-risk');
    }

    // ==========================================
    // Products Endpoints
    // ==========================================

    async getProducts(page = 1, pageSize = 20, filters = {}) {
        const params = new URLSearchParams({
            page,
            page_size: pageSize,
        });

        if (filters.category) params.append('category', filters.category);
        if (filters.brand) params.append('brand', filters.brand);
        if (filters.inStockOnly) params.append('in_stock_only', 'true');
        if (filters.search) params.append('search', filters.search);

        return this.request(`/products?${params}`);
    }

    async getProduct(productId) {
        return this.request(`/products/${productId}`);
    }

    async getProductMetrics() {
        return this.request('/products/metrics');
    }

    async getCategories() {
        return this.request('/products/categories');
    }

    async getLowStockProducts(threshold = 10) {
        return this.request(`/products/low-stock?threshold=${threshold}`);
    }
}

// Create global API instance
const api = new ApiClient();

// Mock data for demo (when API is not available)
const mockData = {
    salesOverview: {
        total_revenue: 1547893.45,
        total_orders: 12847,
        avg_order_value: 120.47,
        total_customers: 8234,
        revenue_growth: 12.5,
        orders_growth: 8.3,
    },

    salesTrend: {
        data: Array.from({ length: 30 }, (_, i) => ({
            date: new Date(Date.now() - (29 - i) * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
            revenue: 40000 + Math.random() * 20000,
            orders: 350 + Math.floor(Math.random() * 150),
            customers: 200 + Math.floor(Math.random() * 100),
        })),
    },

    categoryData: [
        { category: 'Electronics', revenue: 425000, orders: 3200, percentage: 35 },
        { category: 'Clothing', revenue: 312000, orders: 4500, percentage: 25 },
        { category: 'Home & Garden', revenue: 198000, orders: 1800, percentage: 16 },
        { category: 'Sports', revenue: 156000, orders: 1400, percentage: 12 },
        { category: 'Beauty', revenue: 89000, orders: 1200, percentage: 7 },
        { category: 'Books', revenue: 62000, orders: 800, percentage: 5 },
    ],

    hourlyData: Array.from({ length: 24 }, (_, i) => ({
        hour: i,
        orders: Math.floor(50 + Math.sin(i / 3) * 40 + Math.random() * 20),
        revenue: Math.floor(5000 + Math.sin(i / 3) * 3000 + Math.random() * 1500),
    })),

    segments: [
        { segment: 'VIP', count: 823, avg_ltv: 2500 },
        { segment: 'Returning', count: 3294, avg_ltv: 450 },
        { segment: 'New', count: 2468, avg_ltv: 85 },
        { segment: 'At Risk', count: 1234, avg_ltv: 320 },
        { segment: 'Churned', count: 415, avg_ltv: 150 },
    ],

    topProducts: [
        { name: 'Wireless Pro Headphones', category: 'Electronics', units_sold: 2847, revenue: 284700 },
        { name: 'Smart Fitness Watch', category: 'Electronics', units_sold: 2156, revenue: 215600 },
        { name: 'Premium Yoga Mat', category: 'Sports', units_sold: 1893, revenue: 56790 },
        { name: 'Organic Face Serum', category: 'Beauty', units_sold: 1654, revenue: 82700 },
        { name: 'Cotton Basics Tee', category: 'Clothing', units_sold: 1432, revenue: 35800 },
    ],

    recentOrders: [
        { order_number: 'ORD-1847293', customer: 'John Smith', amount: 234.50, status: 'delivered' },
        { order_number: 'ORD-1847292', customer: 'Sarah Johnson', amount: 89.99, status: 'shipped' },
        { order_number: 'ORD-1847291', customer: 'Mike Davis', amount: 456.00, status: 'pending' },
        { order_number: 'ORD-1847290', customer: 'Emily Brown', amount: 123.45, status: 'confirmed' },
        { order_number: 'ORD-1847289', customer: 'Chris Wilson', amount: 78.50, status: 'delivered' },
    ],
};

// Export for use
window.api = api;
window.mockData = mockData;
