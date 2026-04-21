import 'package:flutter/material.dart';

class Analytics extends StatelessWidget {
  const Analytics({super.key});

  // 🤖 AI OUTPUT (replace later with GLM API)
  final String recommendation =
      "Increase price by 5% on high-demand products and introduce bundle promotions for slow-moving items.";

  final String explanation =
      "Cost increased due to inflation and supplier price adjustment. Demand remains stable but price sensitivity is increasing in customers.";

  final String tradeOff =
      "Price vs Demand:\n"
      "- Higher price → lower demand but higher margin\n"
      "- Lower price → higher demand but lower margin\n\n"
      "Cost vs Profit:\n"
      "- Increasing cost reduces profit margin\n"
      "- Optimizing pricing can recover 12–18% profit";

  final String forecast =
      "📈 Next 7 Days Forecast:\n"
      "• Revenue: +15% if price adjusted\n"
      "• Demand: +10% during weekend\n"
      "• Profit: +18% improvement potential";

  final String impact =
      "💥 AI Impact Summary\n\n"
      "💰 Revenue\nRM20,000 → RM24,000\n+20% ↑\n\n"
      "💵 Profit\nRM5,000 → RM10,500\n+110% ↑\n\n"
      "💸 Cost\nRM15,000 → RM13,500\n-10% ↓\n\n"
      "⏱️ Time Saved\n5 hrs/week → 30 mins/week\n-90% ↓";

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xfff5f6f8),

      appBar: AppBar(
        title: const Text("AI Decision Engine"),
        backgroundColor: Colors.white,
        foregroundColor: Colors.black,
        elevation: 0,
      ),

      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text(
              "Z.AI GLM Intelligence System",
              style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
            ),

            const SizedBox(height: 16),

            _card(
              title: "AI Recommendation",
              content: recommendation,
              icon: Icons.lightbulb,
              color: Colors.green,
            ),

            const SizedBox(height: 12),

            _card(
              title: "Trade-Off Analysis",
              content: tradeOff,
              icon: Icons.balance,
              color: Colors.orange,
            ),

            const SizedBox(height: 12),

            _card(
              title: "Explanation (GLM Reasoning)",
              content: explanation,
              icon: Icons.psychology,
              color: Colors.blue,
            ),

            const SizedBox(height: 12),

            _card(
              title: "Forecast",
              content: forecast,
              icon: Icons.trending_up,
              color: Colors.purple,
            ),

            const SizedBox(height: 12),

            // 💥 IMPACT ANALYSIS (SAME STYLE AS FORECAST)
            _card(
              title: "AI Impact Analysis",
              content:
                  "💰 Revenue\nRM20,000 → RM24,000 (+20%)\n\n"
                  "💵 Profit\nRM5,000 → RM10,500 (+110%)\n\n"
                  "💸 Cost\nRM15,000 → RM13,500 (-10%)\n\n"
                  "⏱️ Time Saved\n5 hrs/week → 30 mins/week (-90%)",
              icon: Icons.insights,
              color: Colors.redAccent,
            ),
          ],
        ),
      ),
    );
  }

  // 🧠 REUSABLE CARD
  Widget _card({
    required String title,
    required String content,
    required IconData icon,
    required Color color,
  }) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(color: Colors.black.withOpacity(0.05), blurRadius: 10),
        ],
      ),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Container(
            padding: const EdgeInsets.all(10),
            decoration: BoxDecoration(
              color: color.withOpacity(0.15),
              borderRadius: BorderRadius.circular(12),
            ),
            child: Icon(icon, color: color),
          ),

          const SizedBox(width: 12),

          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  title,
                  style: const TextStyle(
                    fontSize: 15,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                const SizedBox(height: 6),
                Text(
                  content,
                  style: const TextStyle(
                    fontSize: 13,
                    color: Colors.black87,
                    height: 1.4,
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}
