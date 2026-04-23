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

class _ProfilePageState extends State<ProfilePage> {
  String name = "Loading...";
  String category = "";
  String location = "";
  String email = "";

  @override
  void initState() {
    super.initState();
    loadProfile();
  }

  // --- profile.dart ---

Future<void> loadProfile() async {
  final prefs = await SharedPreferences.getInstance();
  final userId = prefs.getString("user_id");

  // 1. 尝试从本地缓存读取（如果有的话）
  setState(() {
    name = prefs.getString("name") ?? "Loading...";
    email = prefs.getString("email") ?? "";
    category = prefs.getString("category") ?? "N/A";
    location = prefs.getString("location") ?? "N/A";
  });

  if (userId != null && userId.isNotEmpty) {
    try {
      // 2. 调用 API 获取最新数据
      final data = await ApiService.getUserProfile(userId);
      
      setState(() {
        // ✅ 这里的 Key 必须和 main.py 返回的一模一样
        name = data['name'] ?? "No Name Set";
        email = data['email'] ?? email;
        category = data['category'] ?? "N/A";
        location = data['location'] ?? "Unknown";
      });

      // 3. 将新数据存入本地，下次打开 App 瞬间就能显示
      await prefs.setString("name", name);
      await prefs.setString("email", email);
      await prefs.setString("category", category);
      await prefs.setString("location", location);

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
        title: const Text("Profile"),
        backgroundColor: Colors.white,
        elevation: 0,
        foregroundColor: Colors.black,
      ),

      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [
            _profileCard(),

            const SizedBox(height: 20),

            _editButton(),

            const Spacer(),

            _logoutButton(),
          ],
        ),
      ),
    );
  }

  // 👤 PROFILE CARD
  Widget _profileCard() {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Column(
        children: [
          const CircleAvatar(
            radius: 30,
            backgroundColor: Colors.green,
            child: Icon(Icons.person, color: Colors.white),
          ),

          const SizedBox(height: 12),

          Text(
            name,
            style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
          ),

          const SizedBox(height: 6),

          Text(email, style: const TextStyle(color: Colors.grey)),

          const SizedBox(height: 12),

          _info("Category", category),
          _info("Location", location),
        ],
      ),
    );
  }

  Widget _info(String title, String value) {
    return Padding(
      padding: const EdgeInsets.only(top: 8),
      child: Row(
        children: [
          Text("$title: ", style: const TextStyle(fontWeight: FontWeight.bold)),
          Text(value),
        ],
      ),
    );
  }

  // ✏️ EDIT PROFILE BUTTON
  Widget _editButton() {
    return SizedBox(
      width: double.infinity,
      height: 50,
      child: ElevatedButton.icon(
        style: ElevatedButton.styleFrom(
          backgroundColor: Colors.blue,
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(12),
          ),
        ),
        icon: const Icon(Icons.edit),
        label: const Text("Edit Profile"),
        onPressed: () async {
          final result = await Navigator.push(
            context,
            MaterialPageRoute(builder: (_) => const EditProfilePage()),
          );

          if (result == true) {
            loadProfile(); // refresh after edit
          }
        },
      ),
    );
  }

  // 🔴 LOGOUT
  Widget _logoutButton() {
    return SizedBox(
      width: double.infinity,
      height: 50,
      child: ElevatedButton.icon(
        style: ElevatedButton.styleFrom(
          backgroundColor: Colors.red,
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(12),
          ),
        ),
        icon: const Icon(Icons.logout),
        label: const Text("Logout"),
        onPressed: () async {
          await UserService.clearUser();
          widget.onLogout();
        },
      ),
    );
  }
}
