import 'package:flutter/material.dart';

class Analytics extends StatelessWidget {
  const Analytics({super.key});

  // ===================== MOCK DATA =====================
  final double revenueBefore = 20000;
  final double revenueAfter = 24000;

  final double profitBefore = 5000;
  final double profitAfter = 10500;

  final double costBefore = 15000;
  final double costAfter = 13500;

  final double timeBefore = 5; // hours per week
  final double timeAfter = 0.5; // 30 mins

  // ===================== CALCULATIONS =====================
  double get revenueGrowth =>
      ((revenueAfter - revenueBefore) / revenueBefore) * 100;

  double get profitGrowth =>
      ((profitAfter - profitBefore) / profitBefore) * 100;

  double get costReduction =>
      ((costBefore - costAfter) / costBefore) * 100;

  double get timeSaved =>
      ((timeBefore - timeAfter) / timeBefore) * 100;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xfff5f6f8),

      appBar: AppBar(
        title: const Text("Analytics"),
        backgroundColor: Colors.white,
        foregroundColor: Colors.black,
        elevation: 0,
      ),

      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [

            /// ================= AI RECOMMENDATION =================
            const Text(
              "🧠 AI Recommendation",
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),

            const SizedBox(height: 10),

            _card(
              icon: Icons.lightbulb,
              color: Colors.green,
              title: "Pricing Strategy",
              desc:
                  "Increase high-demand product prices by 3–5% to maximize profit.\n\n"
                  "Restock fast-moving items to avoid stockouts.",
            ),

            const SizedBox(height: 20),

            /// ================= TRADE OFF =================
            const Text(
              "⚖️ Trade-Off Analysis",
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),

            const SizedBox(height: 10),

            Row(
              children: [
                Expanded(
                  child: _smallCard(
                    title: "Price vs Demand",
                    value: "Balanced ↑",
                    color: Colors.blue,
                  ),
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: _smallCard(
                    title: "Cost vs Profit",
                    value: "Optimized ↑",
                    color: Colors.green,
                  ),
                ),
              ],
            ),

            const SizedBox(height: 20),

            /// ================= EXPLANATION =================
            const Text(
              "📉 GLM Reasoning",
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),

            const SizedBox(height: 10),

            _card(
              icon: Icons.psychology,
              color: Colors.blue,
              title: "AI Explanation",
              desc:
                  "Cost increased due to inflation, but demand remains strong.\n"
                  "Recommended price adjustment: +8% to maintain margin stability.",
            ),

            const SizedBox(height: 20),

            /// ================= FORECAST =================
            const Text(
              "📈 Forecast",
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),

            const SizedBox(height: 10),

            Row(
              children: [
                Expanded(
                  child: _smallCard(
                    title: "Revenue",
                    value: "+20% ↑",
                    color: Colors.green,
                  ),
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: _smallCard(
                    title: "Demand",
                    value: "+15% ↑",
                    color: Colors.blue,
                  ),
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: _smallCard(
                    title: "Profit",
                    value: "+110% ↑",
                    color: Colors.purple,
                  ),
                ),
              ],
            ),

            const SizedBox(height: 20),

            /// ================= IMPACT ANALYSIS =================
            const Text(
              "📈 Impact Analysis",
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),

            const SizedBox(height: 10),

            _impactCard(
              title: "💥 AI Impact Summary",
              items: [
                _impactItem("💰 Revenue", revenueBefore, revenueAfter,
                    "+${revenueGrowth.toStringAsFixed(0)}%"),
                _impactItem("💵 Profit", profitBefore, profitAfter,
                    "+${profitGrowth.toStringAsFixed(0)}%"),
                _impactItem("💸 Cost", costBefore, costAfter,
                    "-${costReduction.toStringAsFixed(0)}%"),
                _impactItem("⏱ Time Saved", timeBefore, timeAfter,
                    "-${timeSaved.toStringAsFixed(0)}%"),
              ],
            ),
          ],
        ),
      ),
    );
  }

  // ================= CARD =================
  Widget _card({
    required IconData icon,
    required Color color,
    required String title,
    required String desc,
  }) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 10,
          )
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
                Text(title,
                    style: const TextStyle(
                        fontWeight: FontWeight.bold, fontSize: 14)),
                const SizedBox(height: 6),
                Text(desc,
                    style: const TextStyle(
                        color: Colors.black87, fontSize: 13, height: 1.4)),
              ],
            ),
          )
        ],
      ),
    );
  }

  // ================= SMALL CARD =================
  Widget _smallCard({
    required String title,
    required String value,
    required Color color,
  }) {
    return Container(
      padding: const EdgeInsets.all(14),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(title,
              style: const TextStyle(fontSize: 12, color: Colors.grey)),
          const SizedBox(height: 6),
          Text(
            value,
            style: TextStyle(
              fontSize: 16,
              fontWeight: FontWeight.bold,
              color: color,
            ),
          ),
        ],
      ),
    );
  }

  // ================= IMPACT CARD =================
  Widget _impactCard({
    required String title,
    required List<Widget> items,
  }) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        gradient: const LinearGradient(
          colors: [Color(0xFFE0F2FE), Color(0xFFEEF2FF)],
        ),
        borderRadius: BorderRadius.circular(16),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(title,
              style: const TextStyle(
                  fontWeight: FontWeight.bold, fontSize: 14)),
          const SizedBox(height: 10),
          ...items,
        ],
      ),
    );
  }

  Widget _impactItem(
    String label,
    double before,
    double after,
    String change,
  ) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(label),
          Text(
            "$before → $after  ($change)",
            style: const TextStyle(fontWeight: FontWeight.bold),
          ),
        ],
      ),
    );
  }
}

