import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../services/api_service.dart';

// ✅ 1. 必须有这个类，ProfilePage 才能识别 "EditProfilePage" 这个类型
class EditProfilePage extends StatefulWidget {
  const EditProfilePage({super.key});

  @override
  State<EditProfilePage> createState() => _EditProfilePageState();
}

// 2. 这是对应的状态类（你刚才贴出的部分）
class _EditProfilePageState extends State<EditProfilePage> {
  final nameController = TextEditingController();
  bool isLoading = false;

  @override
  void initState() {
    super.initState();
    _loadCurrentData();
  }

  void _loadCurrentData() async {
    final prefs = await SharedPreferences.getInstance();
    setState(() {
      nameController.text = prefs.getString("name") ?? "";
    });
  }

  Future<void> _saveProfile() async {
    if (nameController.text.trim().isEmpty) return;

    setState(() => isLoading = true);
    
    final prefs = await SharedPreferences.getInstance();
    final userId = prefs.getString("user_id");

    if (userId != null) {
      bool success = await ApiService.updateUserProfile(userId, {
        "name": nameController.text.trim(),
      });

      if (success) {
        await prefs.setString("name", nameController.text.trim());
        if (mounted) {
          Navigator.pop(context, true); 
        }
      } else {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text("Update failed, please try again.")),
        );
      }
    }
    if (mounted) setState(() => isLoading = false);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text("Edit Profile"),
        backgroundColor: Colors.white,
        foregroundColor: Colors.black,
        elevation: 0,
      ),
      body: Padding(
        padding: const EdgeInsets.all(20.0),
        child: Column(
          children: [
            TextField(
              controller: nameController,
              decoration: InputDecoration(
                labelText: "Full Name",
                border: OutlineInputBorder(borderRadius: BorderRadius.circular(12)),
                prefixIcon: const Icon(Icons.person),
              ),
            ),
            const SizedBox(height: 30),
            SizedBox(
              width: double.infinity,
              height: 50,
              child: isLoading 
                ? const Center(child: CircularProgressIndicator())
                : ElevatedButton(
                    style: ElevatedButton.styleFrom(
                      backgroundColor: Colors.blue,
                      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(12)),
                    ),
                    onPressed: _saveProfile, 
                    child: const Text("Save Changes", style: TextStyle(color: Colors.white)),
                  ),
            ),
          ],
        ),
      ),
    );
  }
}