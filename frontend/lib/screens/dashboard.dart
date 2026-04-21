import 'package:flutter/material.dart';

class Dashboard extends StatelessWidget {
  const Dashboard({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xfff5f6f8),

      body: SafeArea(
        child: SingleChildScrollView(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [

              /// 🧠 TITLE
              const Text(
                "Business Overview",
                style: TextStyle(
                  fontSize: 22,
                  fontWeight: FontWeight.bold,
                ),
              ),

              const SizedBox(height: 6),

              const Text(
                "Your business performance at a glance",
                style: TextStyle(color: Colors.grey),
              ),

              const SizedBox(height: 20),

              /// 💰 MAIN KPI CARD (REVENUE)
              _kpiCard(
                title: "Total Revenue",
                value: "RM 328,000",
                icon: Icons.attach_money,
                color: Colors.green,
                trend: "12.5% from last month",
                isPositive: true,
              ),

              const SizedBox(height: 12),

              /// 📊 COST + PROFIT
              Row(
                children: [
                  Expanded(
                    child: _kpiCard(
                      title: "Total Cost",
                      value: "RM 198,000",
                      icon: Icons.trending_down,
                      color: Colors.red,
                      trend: "6.2%",
                      isPositive: false,
                    ),
                  ),
                  const SizedBox(width: 12),
                  Expanded(
                    child: _kpiCard(
                      title: "Net Profit",
                      value: "RM 130,000",
                      icon: Icons.trending_up,
                      color: Colors.blue,
                      trend: "18.3%",
                      isPositive: true,
                    ),
                  ),
                ],
              ),

              const SizedBox(height: 25),

              /// 🤖 AI INSIGHTS TITLE
              const Text(
                "AI Insights",
                style: TextStyle(
                  fontSize: 18,
                  fontWeight: FontWeight.bold,
                ),
              ),

              const SizedBox(height: 12),

              /// ⚠️ AI INSIGHT 1
              _aiCard(
                icon: Icons.warning_amber_rounded,
                iconBg: Colors.orange.shade100,
                iconColor: Colors.orange,
                title: "Cost Alert: Inflation Impact",
                desc:
                    "Your costs increased by 6% due to inflation over the past quarter.",
                action: "View detailed analysis →",
              ),

              /// 📈 AI INSIGHT 2
              _aiCard(
                icon: Icons.trending_up,
                iconBg: Colors.green.shade100,
                iconColor: Colors.green,
                title: "Demand Forecast",
                desc:
                    "Demand expected to rise by 15% next week due to seasonal trends.",
                action: "View forecast →",
              ),

              /// 📉 AI INSIGHT 3
              _aiCard(
                icon: Icons.trending_down,
                iconBg: Colors.amber.shade100,
                iconColor: Colors.amber.shade700,
                title: "Pricing Alert",
                desc:
                    "You are priced 12% higher than market average. This may impact sales.",
                action: "View comparison →",
              ),
            ],
          ),
        ),
      ),
    );
  }

  /// 📊 KPI CARD
  Widget _kpiCard({
    required String title,
    required String value,
    required IconData icon,
    required Color color,
    required String trend,
    required bool isPositive,
  }) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(18),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [

          /// ICON + TREND
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Icon(icon, color: color, size: 28),
              Text(
                trend,
                style: TextStyle(
                  fontSize: 12,
                  color: isPositive ? Colors.green : Colors.red,
                ),
              ),
            ],
          ),

          const SizedBox(height: 10),

          /// TITLE
          Text(
            title,
            style: const TextStyle(
              color: Colors.grey,
              fontSize: 13,
            ),
          ),

          const SizedBox(height: 5),

          /// VALUE
          Text(
            value,
            style: const TextStyle(
              fontSize: 22,
              fontWeight: FontWeight.bold,
            ),
          ),
        ],
      ),
    );
  }

  /// 🤖 AI CARD
  Widget _aiCard({
    required IconData icon,
    required Color iconBg,
    required Color iconColor,
    required String title,
    required String desc,
    required String action,
  }) {
    return Container(
      margin: const EdgeInsets.only(bottom: 12),
      padding: const EdgeInsets.all(14),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [

          /// ICON
          Container(
            padding: const EdgeInsets.all(8),
            decoration: BoxDecoration(
              color: iconBg,
              borderRadius: BorderRadius.circular(10),
            ),
            child: Icon(icon, color: iconColor),
          ),

          const SizedBox(width: 12),

          /// TEXT
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [

                Text(
                  title,
                  style: const TextStyle(
                    fontWeight: FontWeight.bold,
                  ),
                ),

                const SizedBox(height: 5),

                Text(
                  desc,
                  style: const TextStyle(
                    color: Colors.black87,
                    fontSize: 13,
                  ),
                ),

                const SizedBox(height: 8),

                Text(
                  action,
                  style: const TextStyle(
                    color: Colors.green,
                    fontSize: 12,
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ],
            ),
          )
        ],
      ),
    );
  }
}