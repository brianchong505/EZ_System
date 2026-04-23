import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../services/api_service.dart';

class Dashboard extends StatefulWidget {
  const Dashboard({super.key});

  @override
  State<Dashboard> createState() => _DashboardState();
}

class _DashboardState extends State<Dashboard> {
  late Future<List<dynamic>> combinedData;

  @override
  void initState() {
    super.initState();
    _initLoad();
  }

  // 初始化加载
  void _initLoad() {
    combinedData = _loadAllData();
  }

  // 刷新逻辑：用于手动刷新按钮和下拉刷新
  Future<void> _handleRefresh() async {
    setState(() {
      combinedData = _loadAllData();
    });
    await combinedData; // 确保等待请求完成
  }

  Future<List<dynamic>> _loadAllData() async {
    final prefs = await SharedPreferences.getInstance();
    // 使用 getString 确保拿到的是最新的字符串 ID
    final String? userId = prefs.getString('user_id');
    
    if (userId == null || userId.isEmpty) {
      throw Exception("User session expired. Please login again.");
    }

    try {
      // 并行请求：获取 Dashboard 统计数据 和 AI 预测数据
      return await Future.wait([
        ApiService.getDashboard(userId),
        ApiService.getForecast(),
      ]);
    } catch (e) {
      throw Exception("Connection Error: $e");
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xfff5f6f8),
      appBar: AppBar(
        title: const Text("Business Overview", 
          style: TextStyle(color: Colors.black, fontWeight: FontWeight.bold)),
        backgroundColor: Colors.white,
        elevation: 0,
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh, color: Colors.green),
            onPressed: _handleRefresh,
          )
        ],
      ),
      // ✅ 增加下拉刷新功能
      body: RefreshIndicator(
        onRefresh: _handleRefresh,
        color: Colors.green,
        child: FutureBuilder<List<dynamic>>(
          future: combinedData,
          builder: (context, snapshot) {
            if (snapshot.connectionState == ConnectionState.waiting) {
              return const Center(child: CircularProgressIndicator());
            } else if (snapshot.hasError) {
              return _buildErrorState(snapshot.error.toString());
            } else if (snapshot.hasData) {
              final salesData = snapshot.data![0] as Map<String, dynamic>;
              final forecastData = snapshot.data![1] as Map<String, dynamic>;
              return _buildContent(salesData, forecastData);
            }
            return const Center(child: Text("No data available."));
          },
        ),
      ),
    );
  }

  // 错误展示界面
  Widget _buildErrorState(String error) {
    return ListView( // 使用 ListView 确保在报错时也能触发下拉刷新
      children: [
        SizedBox(height: MediaQuery.of(context).size.height * 0.3),
        const Icon(Icons.cloud_off, size: 60, color: Colors.grey),
        const SizedBox(height: 16),
        Center(child: Text("Error: $error", textAlign: TextAlign.center)),
        TextButton(onPressed: _handleRefresh, child: const Text("Try Again")),
      ],
    );
  }

  Widget _buildContent(Map<String, dynamic> sales, Map<String, dynamic> forecast) {
    // ✅ 安全的数据转换逻辑
    final double revenue = (sales['revenue'] ?? 0).toDouble();
    final double cost = (sales['cost'] ?? 0).toDouble();
    final double profit = revenue - cost;

    final int predictedUnits = (forecast['forecast'] ?? 0).toInt();
    final String trend = forecast['trend']?.toString() ?? "stable";
    final String explanation = forecast['explanation']?.toString() ?? "Generating insights...";

    return SingleChildScrollView(
      physics: const AlwaysScrollableScrollPhysics(), // 确保内容不满也能下拉刷新
      padding: const EdgeInsets.all(16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // 主 KPI 卡片
          _kpiCard(
            title: "Total Revenue",
            value: "RM ${revenue.toStringAsFixed(2)}",
            icon: Icons.payments,
            color: Colors.green,
          ),
          const SizedBox(height: 12),
          Row(
            children: [
              Expanded(child: _kpiCard(title: "Net Profit", value: "RM ${profit.toStringAsFixed(2)}", icon: Icons.trending_up, color: Colors.blue)),
              const SizedBox(width: 12),
              Expanded(child: _kpiCard(title: "Total Cost", value: "RM ${cost.toStringAsFixed(2)}", icon: Icons.shopping_bag, color: Colors.orange)),
            ],
          ),
          const SizedBox(height: 24),
          const Text("AI Business Insights", style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
          const SizedBox(height: 12),
          
          // AI 预测卡片
          _aiCard(
            icon: Icons.psychology,
            iconBg: Colors.purple.shade50,
            iconColor: Colors.purple,
            title: "Demand Forecast",
            desc: "Next cycle: $predictedUnits units ($trend). $explanation",
            action: "Adjust Inventory →",
          ),
          
          // 智能建议卡片
          _aiCard(
            icon: Icons.tips_and_updates,
            iconBg: Colors.amber.shade50,
            iconColor: Colors.amber,
            title: "Smart Tip",
            desc: profit > 0 ? "Profit margins look healthy! Ready to scale?" : "Cost exceeds revenue. Review your pricing strategy.",
            action: "Full Report →",
          ),
        ],
      ),
    );
  }

  Widget _kpiCard({required String title, required String value, required IconData icon, required Color color}) {
    return Container(
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: Colors.white, 
        borderRadius: BorderRadius.circular(20),
        boxShadow: [BoxShadow(color: Colors.black.withOpacity(0.05), blurRadius: 10, offset: const Offset(0, 4))],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          CircleAvatar(backgroundColor: color.withOpacity(0.1), radius: 20, child: Icon(icon, color: color, size: 20)),
          const SizedBox(height: 15),
          Text(title, style: const TextStyle(color: Colors.grey, fontSize: 14)),
          const SizedBox(height: 4),
          Text(value, style: const TextStyle(fontSize: 24, fontWeight: FontWeight.bold)),
        ],
      ),
    );
  }

  Widget _aiCard({required IconData icon, required Color iconBg, required Color iconColor, required String title, required String desc, required String action}) {
    return Container(
      margin: const EdgeInsets.only(bottom: 12),
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Colors.white, 
        borderRadius: BorderRadius.circular(18),
        border: Border.all(color: Colors.grey.shade100),
      ),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Container(
            padding: const EdgeInsets.all(10),
            decoration: BoxDecoration(color: iconBg, borderRadius: BorderRadius.circular(12)),
            child: Icon(icon, color: iconColor),
          ),
          const SizedBox(width: 16),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(title, style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 15)),
                const SizedBox(height: 6),
                Text(desc, style: const TextStyle(color: Colors.black54, fontSize: 13, height: 1.4)),
                const SizedBox(height: 10),
                Text(action, style: const TextStyle(color: Colors.green, fontSize: 13, fontWeight: FontWeight.w600)),
              ],
            ),
          ),
        ],
      ),
    );
  }
}