import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../services/api_service.dart';

class Alerts extends StatefulWidget {
  const Alerts({super.key});

  @override
  State<Alerts> createState() => _AlertsState();
}

class _AlertsState extends State<Alerts> {
  late Future<List<dynamic>> _alertsFuture;

  @override
  void initState() {
    super.initState();
    _alertsFuture = _fetchAlerts();
  }

  Future<List<dynamic>> _fetchAlerts() async {
    final prefs = await SharedPreferences.getInstance();
    final String userId = prefs.getString('user_id') ?? "";
    return await ApiService.getAlerts(userId);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xfff5f6f8),
      appBar: AppBar(
        title: const Text("Business Alerts", style: TextStyle(fontWeight: FontWeight.bold)),
        backgroundColor: Colors.white,
        foregroundColor: Colors.black,
        elevation: 0,
      ),
      body: FutureBuilder<List<dynamic>>(
        future: _alertsFuture,
        builder: (context, snapshot) {
          if (snapshot.connectionState == ConnectionState.waiting) {
            return const Center(child: CircularProgressIndicator());
          }
          
          if (snapshot.hasError || !snapshot.hasData || snapshot.data!.isEmpty) {
            return _buildEmptyState();
          }

          return RefreshIndicator(
            onRefresh: () async {
              setState(() { _alertsFuture = _fetchAlerts(); });
            },
            child: ListView.builder(
              padding: const EdgeInsets.all(16),
              itemCount: snapshot.data!.length,
              itemBuilder: (context, index) {
                final alert = snapshot.data![index];
                // 假设后端返回: { "type": "critical", "title": "...", "message": "..." }
                return _alert(
                  alert['title'] ?? "Alert", 
                  alert['message'] ?? "", 
                  alert['type'] == 'critical'
                );
              },
            ),
          );
        },
      ),
    );
  }

  Widget _buildEmptyState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(Icons.check_circle_outline, size: 60, color: Colors.green.shade200),
          const SizedBox(height: 16),
          const Text("All systems clear! No alerts.", style: TextStyle(color: Colors.grey)),
        ],
      ),
    );
  }

  Widget _alert(String title, String desc, bool high) {
  return Container(
    margin: const EdgeInsets.only(bottom: 12),
    padding: const EdgeInsets.all(16),
    decoration: BoxDecoration(
      // 根据高危(high)还是普通提醒切换颜色
      color: high ? Colors.red.shade50 : Colors.blue.shade50,
      borderRadius: BorderRadius.circular(15),
      border: Border.all(color: high ? Colors.red.shade100 : Colors.blue.shade100),
    ),
    child: Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Icon(
          high ? Icons.warning_amber_rounded : Icons.info_outline_rounded,
          color: high ? Colors.red : Colors.blue,
        ),
        const SizedBox(width: 12),
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(title, style: TextStyle(
                fontWeight: FontWeight.bold, 
                fontSize: 16,
                color: high ? Colors.red.shade900 : Colors.blue.shade900
              )),
              const SizedBox(height: 6),
              Text(desc, style: const TextStyle(fontSize: 14, color: Colors.black87)),
            ]
            ),
          ),
        ],
      ),
    );
  }
}