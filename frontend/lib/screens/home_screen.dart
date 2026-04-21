import 'package:flutter/material.dart';

import 'dashboard.dart';
import 'products.dart';
import 'analytics.dart';
import 'alerts.dart';
import 'profile.dart';

class HomeScreen extends StatefulWidget {
  final VoidCallback onLogout;

  const HomeScreen({super.key, required this.onLogout});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  int index = 0;

  late final List<Widget> pages;

  @override
  void initState() {
    super.initState();

    /// ✅ SAFE INITIALISATION (BEST PRACTICE)
    pages = [
      Dashboard(),
      ProductPage(),
      Analytics(),
      Alerts(),
      ProfilePage(onLogout: widget.onLogout),
    ];
  }

  void onTabSelected(int i) {
    setState(() {
      index = i;
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xfff5f6f8),

      /// 📄 PAGE CONTENT
      body: IndexedStack(
        index: index,
        children: pages,
      ),

      /// 📱 BOTTOM NAVIGATION
      bottomNavigationBar: Container(
        margin: const EdgeInsets.fromLTRB(16, 0, 16, 16),
        padding: const EdgeInsets.symmetric(vertical: 10),
        decoration: BoxDecoration(
          color: Colors.white,
          borderRadius: BorderRadius.circular(24),
          boxShadow: [
            BoxShadow(
              color: Colors.black.withOpacity(0.08),
              blurRadius: 20,
            ),
          ],
        ),

        child: Row(
          mainAxisAlignment: MainAxisAlignment.spaceAround,
          children: [
            _navItem(Icons.dashboard, "Home", 0),
            _navItem(Icons.inventory_2, "Products", 1),
            _navItem(Icons.analytics, "Analytics", 2),
            _navItem(Icons.warning, "Alerts", 3),
            _navItem(Icons.person, "Profile", 4),
          ],
        ),
      ),
    );
  }

  /// 📌 NAV ITEM WIDGET (CLEAN + REUSABLE)
  Widget _navItem(IconData icon, String label, int i) {
    final selected = index == i;

    return GestureDetector(
      onTap: () => onTabSelected(i),
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 200),
        padding: const EdgeInsets.symmetric(vertical: 6, horizontal: 10),
        decoration: BoxDecoration(
          color: selected ? Colors.green.withOpacity(0.1) : Colors.transparent,
          borderRadius: BorderRadius.circular(12),
        ),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(
              icon,
              color: selected ? Colors.green : Colors.grey,
            ),
            const SizedBox(height: 2),
            Text(
              label,
              style: TextStyle(
                color: selected ? Colors.green : Colors.grey,
                fontSize: 12,
                fontWeight: selected ? FontWeight.w600 : FontWeight.normal,
              ),
            ),
          ],
        ),
      ),
    );
  }
}