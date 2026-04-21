import 'package:flutter/material.dart';
import 'dart:convert';
import 'package:http/http.dart' as http;

class AuthScreen extends StatefulWidget {
  final VoidCallback onLogin;

  const AuthScreen({super.key, required this.onLogin});

  @override
  State<AuthScreen> createState() => _AuthScreenState();
}

class _AuthScreenState extends State<AuthScreen> {
  bool isSignUp = false;

  final emailController = TextEditingController();
  final passwordController = TextEditingController();

  void handleAuth() async {
  final res = await http.post(
    Uri.parse("http://10.0.2.2:8000/auth/login"),
    headers: {"Content-Type": "application/json"},
    body: jsonEncode({
      "email": emailController.text,
      "password": passwordController.text,
    }),
  );

  final data = jsonDecode(res.body);

  if (data["error"] == null) {
    widget.onLogin();
  }
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
                const Icon(Icons.business_center,
                    size: 80, color: Colors.green),

                const SizedBox(height: 10),

                const Text(
                  "EZ SYSTEM",
                  style: TextStyle(fontSize: 28, fontWeight: FontWeight.bold),
                ),

                const SizedBox(height: 5),

                const Text(
                  "AI-powered Business Intelligence",
                  style: TextStyle(color: Colors.grey),
                ),

                const SizedBox(height: 30),

                Container(
                  padding: const EdgeInsets.all(20),
                  decoration: BoxDecoration(
                    color: Colors.white,
                    borderRadius: BorderRadius.circular(20),
                    boxShadow: [
                      BoxShadow(
                        color: Colors.black.withOpacity(0.08),
                        blurRadius: 20,
                      )
                    ],
                  ),
                  child: Column(
                    children: [
                      // Toggle
                      Row(
                        children: [
                          Expanded(
                            child: TextButton(
                              onPressed: () => setState(() => isSignUp = false),
                              child: Text(
                                "Login",
                                style: TextStyle(
                                  color: !isSignUp
                                      ? Colors.green
                                      : Colors.grey,
                                ),
                              ),
                            ),
                          ),
                          Expanded(
                            child: TextButton(
                              onPressed: () => setState(() => isSignUp = true),
                              child: Text(
                                "Sign Up",
                                style: TextStyle(
                                  color:
                                      isSignUp ? Colors.green : Colors.grey,
                                ),
                              ),
                            ),
                          ),
                        ],
                      ),

                      const SizedBox(height: 10),

                      TextField(
                        controller: emailController,
                        decoration: const InputDecoration(
                          labelText: "Email",
                          border: OutlineInputBorder(),
                        ),
                      ),

                      const SizedBox(height: 10),

                      TextField(
                        controller: passwordController,
                        obscureText: true,
                        decoration: const InputDecoration(
                          labelText: "Password",
                          border: OutlineInputBorder(),
                        ),
                      ),

                      const SizedBox(height: 20),

                      SizedBox(
                        width: double.infinity,
                        height: 50,
                        child: ElevatedButton(
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.green,
                          ),
                          onPressed: handleAuth,
                          child: Text(
                            isSignUp ? "Create Account" : "Login",
                          ),
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
}