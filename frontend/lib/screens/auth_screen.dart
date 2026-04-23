import 'package:flutter/material.dart';
import '../services/api_service.dart';
import 'dart:async'; // 必须有这个才能使用 TimeoutException
import 'home_screen.dart';// 路径根据你的实际情况修改

class AuthScreen extends StatefulWidget {
  final VoidCallback onLogin;
  const AuthScreen({super.key, required this.onLogin});

  @override
  State<AuthScreen> createState() => _AuthScreenState();
}

class _AuthScreenState extends State<AuthScreen> {
  bool isSignUp = false;
  bool isLoading = false;

  // 只保留 email, password 和 business(仅前端展示用) 的 Controller
  final emailController = TextEditingController();
  final passwordController = TextEditingController();
  final businessController = TextEditingController(); // 仅前端填写，暂不传 API

  Future<void> handleAuth() async {
  final email = emailController.text.trim();
  final password = passwordController.text.trim();

  // 1. 基础验证（保持不变）
  bool isEmailValid = RegExp(
    r"^[a-zA-Z0-9.a-zA-Z0-9.!#$%&'*+-/=?^_`{|}~]+@[a-zA-Z0-9]+\.[a-zA-Z]+"
  ).hasMatch(email);

  if (!isEmailValid) {
    _showError("Email format is invalid");
    return;
  }
  if (password.length < 8) {
    _showError("Password must be at least 8 characters");
    return;
  }
  if (isSignUp && businessController.text.isEmpty) {
    _showError("Please enter your Business Name");
    return;
  }

  setState(() => isLoading = true);

  try {
    bool success = false;
    if (isSignUp) {
      // 如果 ApiService 抛出 Exception("Email already registered")
      // 它会直接跳到下面的 catch 块
      success = await ApiService.register(
        email, 
        password, 
        businessController.text.trim() 
      ).timeout(const Duration(seconds: 5));
    } else {
      success = await ApiService.login(email, password)
          .timeout(const Duration(seconds: 5));
    }

    if (success) {
      // 1. 触发 AuthGate 的 login() 方法，改变 isLoggedIn 状态
      widget.onLogin();

      // 2. 🚀 双重保障：如果 AuthGate 没有自动切换，我们手动推一把
      if (mounted) {
        Navigator.pushAndRemoveUntil(
          context,
          MaterialPageRoute(builder: (context) => HomeScreen(onLogout: () {
            // 这里传一个简单的退出逻辑，或者留空
            Navigator.pushNamedAndRemoveUntil(context, '/', (route) => false);
          })),
          (route) => false,
        );
      }
    }
  } catch (e) {
      debugPrint("Auth Error: $e");
      
      String errorMessage;

      // 1. 精准处理超时错误 (需要 import 'dart:async')
      if (e is TimeoutException) {
        errorMessage = "Server request timed out. Please check your connection.";
      } 
      // 2. 处理网络连接被拒绝 (服务器没开)
      else if (e.toString().contains("Connection refused") || e.toString().contains("SocketException")) {
        errorMessage = "Cannot connect to server. Is your backend running?";
      }
      // 3. 处理业务逻辑错误 (例如: Email already registered)
      else {
        // 去掉 "Exception: " 前缀，只保留核心消息
        errorMessage = e.toString().replaceAll("Exception: ", "");
      }

      _showError(errorMessage);
    } finally {
      if (mounted) setState(() => isLoading = false);
    }
  }

  void _showError(String message) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(message), backgroundColor: Colors.redAccent),
    );
  }

  @override
  void dispose() {
    emailController.dispose();
    passwordController.dispose();
    businessController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: Container(
        decoration: const BoxDecoration(
          gradient: LinearGradient(
            colors: [Color(0xFFE8F5E9), Colors.white],
            begin: Alignment.topLeft,
            end: Alignment.bottomRight,
          ),
        ),
        child: Center(
          child: SingleChildScrollView(
            padding: const EdgeInsets.all(24),
            child: Column(
              children: [
                const Icon(Icons.business_center, size: 80, color: Colors.green),
                const SizedBox(height: 10),
                const Text("EZ SYSTEM", style: TextStyle(fontSize: 28, fontWeight: FontWeight.bold)),
                const SizedBox(height: 5),
                const Text("AI-powered Business Intelligence", style: TextStyle(color: Colors.grey)),
                const SizedBox(height: 30),
                Container(
                  padding: const EdgeInsets.all(20),
                  decoration: BoxDecoration(
                    color: Colors.white,
                    borderRadius: BorderRadius.circular(20),
                    boxShadow: const [BoxShadow(color: Colors.black12, blurRadius: 20)],
                  ),
                  child: Column(
                    children: [
                      // LOGIN / REGISTER 切换
                      Row(
                        children: [
                          _toggleButton("Login", !isSignUp, () => setState(() => isSignUp = false)),
                          _toggleButton("Register", isSignUp, () => setState(() => isSignUp = true)),
                        ],
                      ),
                      const SizedBox(height: 20),

                      // 动态输入框
                      if (isSignUp) ...[
                        _buildTextField(businessController, "Business Name", Icons.store),
                        const SizedBox(height: 10),
                      ],
                      _buildTextField(emailController, "Email", Icons.email),
                      const SizedBox(height: 10),
                      _buildTextField(passwordController, "Password", Icons.lock, obscure: true),
                      
                      const SizedBox(height: 20),

                      // 提交按钮
                      SizedBox(
                        width: double.infinity,
                        height: 50,
                        child: ElevatedButton(
                          style: ElevatedButton.styleFrom(backgroundColor: Colors.green),
                          onPressed: isLoading ? null : handleAuth,
                          child: isLoading 
                              ? const CircularProgressIndicator(color: Colors.white)
                              : Text(isSignUp ? "Create Account" : "Login", style: const TextStyle(color: Colors.white)),
                        ),
                      ),
                    ],
                  ),
                )
              ],
            ),
          ),
        ),
      ),
    );
  }

  Widget _toggleButton(String title, bool active, VoidCallback onPressed) {
    return Expanded(
      child: TextButton(
        onPressed: isLoading ? null : onPressed,
        child: Text(title, style: TextStyle(color: active ? Colors.green : Colors.grey, fontWeight: active ? FontWeight.bold : FontWeight.normal)),
      ),
    );
  }

  Widget _buildTextField(TextEditingController controller, String label, IconData icon, {bool obscure = false}) {
    return TextField(
      controller: controller,
      obscureText: obscure,
      decoration: InputDecoration(
        prefixIcon: Icon(icon, size: 20),
        labelText: label,
        border: const OutlineInputBorder(),
      ),
    );
  }
}