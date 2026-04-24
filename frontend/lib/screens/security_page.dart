import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../services/api_service.dart'; // 确保这个路径指向你的 ApiService
import 'auth_screen.dart';

class SecurityPage extends StatefulWidget {
  const SecurityPage({super.key});

  @override
  State<SecurityPage> createState() => _SecurityPageState();
}

class _SecurityPageState extends State<SecurityPage> {
  // 1. 定义控制器来获取输入框的值
  final TextEditingController _oldPwdController = TextEditingController();
  final TextEditingController _newPwdController = TextEditingController();
  final TextEditingController _confirmPwdController = TextEditingController();

  final TextEditingController _emailPwdController = TextEditingController();
  final TextEditingController _newEmailController = TextEditingController();

  @override
  void dispose() {
    // 页面关闭时销毁控制器
    _oldPwdController.dispose();
    _newPwdController.dispose();
    _confirmPwdController.dispose();
    _emailPwdController.dispose();
    _newEmailController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text("Security Settings")),
      body: ListView(
        children: [
          ListTile(
            leading: const Icon(Icons.lock_outline),
            title: const Text("Change Password"),
            trailing: const Icon(Icons.chevron_right),
            onTap: () => _showChangePasswordDialog(),
          ),
          ListTile(
            leading: const Icon(Icons.email_outlined),
            title: const Text("Change Email"),
            trailing: const Icon(Icons.chevron_right),
            onTap: () => _showChangeEmailDialog(),
          ),
          const Divider(),
          ListTile(
            leading: const Icon(Icons.delete_forever, color: Colors.red),
            title: const Text(
              "Delete Account",
              style: TextStyle(color: Colors.red, fontWeight: FontWeight.bold),
            ),
            onTap: () => _showDeleteAccountDialog(),
          ),
        ],
      ),
    );
  }

  // --- 修改密码对话框 ---
  void _showChangePasswordDialog() {
    // 每次打开清空上次的输入
    _oldPwdController.clear();
    _newPwdController.clear();
    _confirmPwdController.clear();

    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text("Change Password"),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: _oldPwdController,
              decoration: const InputDecoration(labelText: "Current Password"),
              obscureText: true,
            ),
            TextField(
              controller: _newPwdController,
              decoration: const InputDecoration(labelText: "New Password"),
              obscureText: true,
            ),
            TextField(
              controller: _confirmPwdController,
              decoration: const InputDecoration(
                labelText: "Confirm New Password",
              ),
              obscureText: true,
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text("Cancel"),
          ),
          // ✅ 按钮点击关联到下面的 _handleUpdatePassword 方法
          ElevatedButton(
            onPressed: _handleUpdatePassword,
            child: const Text("Update"),
          ),
        ],
      ),
    );
  }

  // --- 修改邮箱对话框 ---
  void _showChangeEmailDialog() {
    _emailPwdController.clear();
    _newEmailController.clear();

    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text("Change Email"),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: _emailPwdController,
              decoration: const InputDecoration(labelText: "Password"),
              obscureText: true,
            ),
            TextField(
              controller: _newEmailController,
              decoration: const InputDecoration(labelText: "New Email"),
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text("Cancel"),
          ),
          // ✅ 按钮点击关联到下面的 _handleUpdateEmail 方法
          ElevatedButton(
            onPressed: _handleUpdateEmail,
            child: const Text("Update"),
          ),
        ],
      ),
    );
  }

  void _showDeleteAccountDialog() {
    final TextEditingController _confirmPwdController = TextEditingController();

    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text("Delete Account?"),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            const Text(
              "This action is permanent and cannot be undone. All your business data will be lost.",
              style: TextStyle(color: Colors.grey, fontSize: 13),
            ),
            const SizedBox(height: 16),
            TextField(
              controller: _confirmPwdController,
              decoration: const InputDecoration(
                labelText: "Enter Password to Confirm",
                border: OutlineInputBorder(),
              ),
              obscureText: true,
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text("Cancel"),
          ),
          ElevatedButton(
            style: ElevatedButton.styleFrom(backgroundColor: Colors.red),
            onPressed: () async {
              final prefs = await SharedPreferences.getInstance();
              final userId = prefs.getString("user_id") ?? "";

              final result = await ApiService.deleteAccount(
                userId,
                _confirmPwdController.text,
              );

              if (result['status'] == 'success') {
                // 1. 清除本地所有登录数据
                await prefs.clear();

                // 2. 关闭确认对话框
                if (!mounted) return;
                Navigator.pop(context);

                // 3. 🚀 关键修改：直接跳转到 AuthScreen
                // 修改 SecurityPage.dart 里的跳转部分
                Navigator.pushAndRemoveUntil(
                  context,
                  MaterialPageRoute(
                    builder: (context) => AuthScreen(
                      onLogin: () {}, // ✅ 在这里加上这行，解决 "required" 报错
                    ),
                  ),
                  (route) => false,
                );

                _showSnackBar("Account deleted successfully.", Colors.black);
              }
            },
            child: const Text(
              "Confirm Delete",
              style: TextStyle(color: Colors.white),
            ),
          ),
        ],
      ),
    );
  }

  // ================= 核心逻辑方法 (之前报错就在这里) =================

  Future<void> _handleUpdatePassword() async {
    // 1. 简单校验
    if (_newPwdController.text != _confirmPwdController.text) {
      _showSnackBar("Passwords do not match!", Colors.red);
      return;
    }

    // 2. 获取 UserId
    final prefs = await SharedPreferences.getInstance();
    final userId = prefs.getString("user_id") ?? "";

    print("🚀 发送修改密码请求: $userId");

    // 3. 调用 API (方法名必须和 ApiService 里一致)
    final result = await ApiService.changePassword(
      userId,
      _oldPwdController.text,
      _newPwdController.text,
    );

    if (!mounted) return;

    if (result['status'] == 'success') {
      Navigator.pop(context); // 关闭对话框
      _showSnackBar("Password updated successfully!", Colors.green);
    } else {
      _showSnackBar(result['message'] ?? "Error occurred", Colors.red);
    }
  }

  Future<void> _handleUpdateEmail() async {
    final prefs = await SharedPreferences.getInstance();
    final userId = prefs.getString("user_id") ?? "";

    final result = await ApiService.changeEmail(
      userId,
      _emailPwdController.text,
      _newEmailController.text,
    );

    if (!mounted) return;

    if (result['status'] == 'success') {
      await prefs.setString("email", _newEmailController.text); // 更新本地缓存
      Navigator.pop(context);
      _showSnackBar("Email updated successfully!", Colors.green);
    } else {
      _showSnackBar(result['message'] ?? "Error occurred", Colors.red);
    }
  }

  void _showSnackBar(String msg, Color color) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(msg),
        backgroundColor: color,
        behavior: SnackBarBehavior.floating,
      ),
    );
  }
}
