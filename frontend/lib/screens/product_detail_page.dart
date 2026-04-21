import 'package:flutter/material.dart';
import 'package:fl_chart/fl_chart.dart';

class ProductDetailPage extends StatelessWidget {
  const ProductDetailPage({super.key});

  @override
  Widget build(BuildContext context) {
    final product = {
      "id": "1",
      "name": "Premium Arabica Coffee",
      "category": "Beverages",
      "description":
          "100% Arabica coffee beans sourced from Colombia. Rich, smooth flavor with notes of chocolate and caramel.",
      "price": 12.99,
      "cost": 6.5,
      "stock": 245,
      "margin": 50,
      "sold": 342,
      "image": "☕",
    };

    return Scaffold(
      backgroundColor: const Color(0xFFF5F6FA),

      /// APP BAR
      appBar: AppBar(
        backgroundColor: Colors.white,
        elevation: 1,
        leading: IconButton(
          icon: const Icon(Icons.arrow_back),
          onPressed: () => Navigator.pop(context),
        ),
        title: const Text("Product Detail"),
      ),

      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [

            /// 📦 PRODUCT HEADER CARD
            _productHeader(product),

            const SizedBox(height: 16),

            /// 📊 METRICS GRID
            _metricsGrid(product),

            const SizedBox(height: 16),

            /// 📈 PRICE HISTORY CHART
            _priceHistoryChart(),

            const SizedBox(height: 16),

            /// 🤖 AI BUTTON
            _aiButton(context),

            const SizedBox(height: 16),

            /// 📄 DETAILS
            _productDetails(product),
          ],
        ),
      ),
    );
  }

  /// 📦 HEADER
  Widget _productHeader(Map product) {
    return Card(
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Row(
          children: [

            /// IMAGE
            Container(
              width: 70,
              height: 70,
              decoration: BoxDecoration(
                color: Colors.grey[200],
                borderRadius: BorderRadius.circular(16),
              ),
              child: Center(
                child: Text(
                  product["image"],
                  style: const TextStyle(fontSize: 32),
                ),
              ),
            ),

            const SizedBox(width: 12),

            /// TEXT
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Container(
                    padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
                    decoration: BoxDecoration(
                      color: Colors.blue[100],
                      borderRadius: BorderRadius.circular(20),
                    ),
                    child: Text(
                      product["category"],
                      style: const TextStyle(
                        color: Colors.blue,
                        fontSize: 12,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ),

                  const SizedBox(height: 6),

                  Text(
                    product["name"],
                    style: const TextStyle(
                      fontSize: 18,
                      fontWeight: FontWeight.bold,
                    ),
                  ),

                  const SizedBox(height: 4),

                  Text(
                    product["description"],
                    style: const TextStyle(fontSize: 12, color: Colors.grey),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  /// 📊 METRICS
  Widget _metricsGrid(Map product) {
    return GridView.count(
      crossAxisCount: 2,
      shrinkWrap: true,
      physics: const NeverScrollableScrollPhysics(),
      childAspectRatio: 1.6,
      children: [

        _metricCard("Price", "\$${product["price"]}", Icons.attach_money, Colors.green),
        _metricCard("Stock", "${product["stock"]}", Icons.inventory, Colors.blue),
        _metricCard("Cost", "\$${product["cost"]}", Icons.trending_up, Colors.orange),
        _metricCard("Sold", "${product["sold"]}", Icons.history, Colors.purple),
      ],
    );
  }

  Widget _metricCard(String title, String value, IconData icon, Color color) {
    return Card(
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Icon(icon, color: color),
            const Spacer(),
            Text(title, style: const TextStyle(fontSize: 12, color: Colors.grey)),
            Text(
              value,
              style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
          ],
        ),
      ),
    );
  }

  /// 📈 CHART (SIMPLIFIED)
  Widget _priceHistoryChart() {
    return Card(
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: const [

            Text(
              "Price & Cost Trend",
              style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
            ),

            SizedBox(height: 150),

            /// NOTE:
            /// Replace this with fl_chart LineChart if needed
            Center(child: Text("📈 Chart Area (Use fl_chart)")),
          ],
        ),
      ),
    );
  }

  /// 🤖 AI BUTTON
  Widget _aiButton(BuildContext context) {
    return Container(
      width: double.infinity,
      decoration: BoxDecoration(
        gradient: const LinearGradient(
          colors: [Color(0xFF2ECC71), Color(0xFF27AE60)],
        ),
        borderRadius: BorderRadius.circular(16),
      ),
      child: TextButton(
        onPressed: () {
          Navigator.pushNamed(context, "/ai");
        },
        child: const Padding(
          padding: EdgeInsets.all(16),
          child: Column(
            children: [
              Icon(Icons.auto_awesome, color: Colors.white),
              SizedBox(height: 6),
              Text(
                "Get AI Recommendation",
                style: TextStyle(color: Colors.white, fontSize: 16),
              ),
              Text(
                "Z.AI GLM optimization",
                style: TextStyle(color: Colors.white70, fontSize: 12),
              ),
            ],
          ),
        ),
      ),
    );
  }

  /// 📄 DETAILS
  Widget _productDetails(Map product) {
    return Card(
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [

            _detailRow("SKU", "PRD-00001"),
            _detailRow("Category", product["category"]),
            _detailRow("Margin", "${product["margin"]}%"),
            _detailRow("Status", "In Stock"),
          ],
        ),
      ),
    );
  }

  Widget _detailRow(String title, String value) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(title, style: const TextStyle(color: Colors.grey)),
          Text(value, style: const TextStyle(fontWeight: FontWeight.bold)),
        ],
      ),
    );
  }
}