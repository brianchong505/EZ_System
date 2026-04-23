import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../services/user_service.dart';
import 'edit_profile.dart';
import 'package:frontend/services/api_service.dart'; 

class ProfilePage extends StatefulWidget {
  final VoidCallback onLogout;

  const ProfilePage({super.key, required this.onLogout});

  @override
  State<ProfilePage> createState() => _ProfilePageState();
}

// --- 核心修改部分 ---

class _ProfilePageState extends State<ProfilePage> {
  String name = "Loading...";
  String email = "";
  // ❌ 删掉 category 和 location 变量

  @override
  void initState() {
    super.initState();
    loadProfile();
  }

  Future<void> loadProfile() async {
    final prefs = await SharedPreferences.getInstance();
    final userId = prefs.getString("user_id");

    setState(() {
      name = prefs.getString("name") ?? "User";
      email = prefs.getString("email") ?? "";
    });

    if (userId != null && userId.isNotEmpty) {
      try {
        final data = await ApiService.getUserProfile(userId);
        setState(() {
          name = data['name'] ?? "No Name Set";
          email = data['email'] ?? email;
        });
        await prefs.setString("name", name);
        await prefs.setString("email", email);
      } catch (e) {
        print("Profile Sync Error: $e");
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xfff5f6f8),
      appBar: AppBar(
        title: const Text("Business Profile", style: TextStyle(fontWeight: FontWeight.bold)),
        centerTitle: true, // 居中标题显得更平衡
        backgroundColor: Colors.white,
        elevation: 0,
        foregroundColor: Colors.black,
      ),
      body: SingleChildScrollView( // 防止溢出，增加层次感
        padding: const EdgeInsets.all(20),
        child: Column(
          children: [
            _profileHeader(), // 升级后的头部
            const SizedBox(height: 24),
            _menuSection(),   // 增加菜单项填充空白
            const SizedBox(height: 40),
            _logoutButton(),
          ],
        ),
      ),
    );
  }

  // 👤 升级后的个人头部：不再显得空旷
  Widget _profileHeader() {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.symmetric(vertical: 30, horizontal: 20),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(24),
        boxShadow: [BoxShadow(color: Colors.black.withOpacity(0.05), blurRadius: 15)],
      ),
      child: Column(
        children: [
          Stack( // 增加头像装饰感
            alignment: Alignment.bottomRight,
            children: [
              CircleAvatar(
                radius: 45,
                backgroundColor: Colors.green.shade100,
                child: const Icon(Icons.business, size: 45, color: Colors.green),
              ),
              CircleAvatar(
                radius: 14,
                backgroundColor: Colors.blue.shade600,
                child: const Icon(Icons.verified, size: 16, color: Colors.white),
              ),
            ],
          ),
          const SizedBox(height: 16),
          Text(name, style: const TextStyle(fontSize: 22, fontWeight: FontWeight.bold)),
          const SizedBox(height: 4),
          Text(email, style: TextStyle(color: Colors.grey.shade600, fontSize: 14)),
          const SizedBox(height: 20),
          const Divider(),
          const SizedBox(height: 10),
          const Text(
            "“ Empowering SMEs with AI intelligence ”", // 增加slogan填充空间
            style: TextStyle(fontStyle: FontStyle.italic, color: Colors.green),
          ),
        ],
      ),
    );
  }

  // 🛠️ 增加功能菜单项：让页面看起来功能丰富
  Widget _menuSection() {
    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(20),
      ),
      child: Column(
        children: [
          _menuTile(Icons.edit_note, "Edit Profile", Colors.blue, () async {
            final result = await Navigator.push(
              context,
              MaterialPageRoute(builder: (_) => const EditProfilePage()),
            );
            if (result == true) loadProfile();
          }),
          _menuTile(Icons.security, "Security Settings", Colors.orange, () {}),
          _menuTile(Icons.help_outline, "Help & Support", Colors.purple, () {}),
        ],
      ),
    );
  }

  Widget _menuTile(IconData icon, String title, Color color, VoidCallback onTap) {
    return ListTile(
      leading: Container(
        padding: const EdgeInsets.all(8),
        decoration: BoxDecoration(color: color.withOpacity(0.1), borderRadius: BorderRadius.circular(10)),
        child: Icon(icon, color: color, size: 20),
      ),
      title: Text(title, style: const TextStyle(fontWeight: FontWeight.w500)),
      trailing: const Icon(Icons.chevron_right, size: 18),
      onTap: onTap,
    );
  }

  // 🔴 退出登录按钮
  Widget _logoutButton() {
    return TextButton.icon(
      style: TextButton.styleFrom(foregroundColor: Colors.red),
      onPressed: () async {
        await UserService.clearUser();
        widget.onLogout();
      },
      icon: const Icon(Icons.logout),
      label: const Text("Sign Out of EZ System", style: TextStyle(fontWeight: FontWeight.bold)),
    );
  }
}