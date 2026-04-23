import 'package:flutter/material.dart';
import 'package:frontend/screens/auth_screen.dart';
import 'package:frontend/screens/home_screen.dart';
import 'package:frontend/services/user_service.dart'; // 必须导入 UserService

void main() {
  runApp(const EZSystemApp());
}

class EZSystemApp extends StatelessWidget {
  const EZSystemApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'EZ System',
      // 移除 const，因为 AuthGate 内部有异步初始化逻辑
      home: AuthGate(), 
    );
  }
}

/// 1. 定义 StatefulWidget 类 (这是你之前代码里漏掉的部分)
class AuthGate extends StatefulWidget {
  const AuthGate({super.key});

  @override
  State<AuthGate> createState() => _AuthGateState();
}

/// 2. 定义 State 类
class _AuthGateState extends State<AuthGate> {
  bool isLoggedIn = false;
  bool isLoading = true; 

  @override
  void initState() {
    super.initState();
    checkLoginStatus();
  }

  // 检查本地是否有存储 user_id
  Future<void> checkLoginStatus() async {
    final userId = await UserService.getUserId();
    setState(() {
      isLoggedIn = userId != null;
      isLoading = false;
    });
  }

  void login() {
    setState(() {
      isLoggedIn = true;
    });
  }

  void logout() {
    setState(() {
      isLoggedIn = false;
    });
  }

  @override
  Widget build(BuildContext context) {
    // 如果还在检查登录状态，显示加载圈
    if (isLoading) {
      return const Scaffold(
        body: Center(child: CircularProgressIndicator()),
      );
    }
  
    // 根据登录状态跳转页面
    return isLoggedIn
        ? HomeScreen(onLogout: logout)
        : AuthScreen(onLogin: login);
  }
}