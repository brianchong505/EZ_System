import 'package:flutter/material.dart';
import 'package:frontend/screens/auth_screen.dart';
import 'package:frontend/screens/home_screen.dart';

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
      home: const AuthGate(),
    );
  }
}

/// THIS CONTROLS LOGIN FLOW
class AuthGate extends StatefulWidget {
  const AuthGate({super.key});

  @override
  State<AuthGate> createState() => _AuthGateState();
}

class _AuthGateState extends State<AuthGate> {
  bool isLoggedIn = false; // later replace with JWT check

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
    return isLoggedIn
        ? HomeScreen(onLogout: logout)
        : AuthScreen(onLogin: login);
  }
}