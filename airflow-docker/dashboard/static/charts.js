// charts.js - Simple and clean
document.addEventListener('DOMContentLoaded', function() {
    // Set last update time
    document.getElementById('lastUpdate').textContent = new Date().toLocaleString();
    
    // Fetch data from API
    fetch('/api/data')
        .then(response => response.json())
        .then(data => {
            createProductChart(data.top_products);
            createMonthlyChart(data.monthly_revenue);
        })
        .catch(error => {
            console.error('Error loading data:', error);
        });
});

function createProductChart(products) {
    const ctx = document.getElementById('productsChart').getContext('2d');
    const labels = products.map(item => item[0]);
    const values = products.map(item => item[1]);
    
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Revenue',
                data: values,
                backgroundColor: ['#667eea', '#764ba2', '#f093fb', '#f5576c', '#4facfe']
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

function createMonthlyChart(monthly) {
    const ctx = document.getElementById('monthlyChart').getContext('2d');
    const months = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const labels = monthly.map(item => months[item[0]]);
    const values = monthly.map(item => item[1]);
    
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Revenue',
                data: values,
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

// Auto-refresh every 5 minutes
setTimeout(() => location.reload(), 300000);