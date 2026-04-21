import 'package:flutter/material.dart';

class AuthScreen extends StatefulWidget {
  final VoidCallback onLogin;

  const AuthScreen({super.key, required this.onLogin});

  @override
  State<AuthScreen> createState() => _AuthScreenState();
}

class _AuthScreenState extends State<AuthScreen> {
  bool isSignUp = false;

  final nameController = TextEditingController();
  final emailController = TextEditingController();
  final passwordController = TextEditingController();
  final businessController = TextEditingController();
  final regionController = TextEditingController();

  void handleAuth() {
    if (isSignUp) {
      // 🔥 REGISTER LOGIC (later API call)
      print("Registering user:");
      print("Name: ${nameController.text}");
      print("Email: ${emailController.text}");
      print("Business: ${businessController.text}");
      print("Region: ${regionController.text}");
    } else {
      // 🔥 LOGIN LOGIC (later API call)
      print("Logging in user");
    }

    widget.onLogin();
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
                        color: Colors.black12,
                        blurRadius: 20,
                      )
                    ],
                  ),
                  child: Column(
                    children: [
                      // TOGGLE
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
                                "Register",
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

                      // NAME (REGISTER ONLY)
                      if (isSignUp)
                        Column(
                          children: [
                            TextField(
                              controller: nameController,
                              decoration: const InputDecoration(
                                labelText: "Full Name",
                                border: OutlineInputBorder(),
                              ),
                            ),
                            const SizedBox(height: 10),
                          ],
                        ),

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

                      // BUSINESS NAME (REGISTER ONLY)
                      if (isSignUp) ...[
                        const SizedBox(height: 10),
                        TextField(
                          controller: businessController,
                          decoration: const InputDecoration(
                            labelText: "Business Name",
                            border: OutlineInputBorder(),
                          ),
                        ),

                        const SizedBox(height: 10),

                        TextField(
                          controller: regionController,
                          decoration: const InputDecoration(
                            labelText: "Region",
                            border: OutlineInputBorder(),
                          ),
                        ),
                      ],

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