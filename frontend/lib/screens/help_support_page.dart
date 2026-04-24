import 'package:flutter/material.dart';
// 如果你想实现点击号码直接拨号，可以添加 url_launcher 插件

class HelpSupportPage extends StatelessWidget {
  const HelpSupportPage({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text("Help & Support"),
        backgroundColor: Colors.white,
        foregroundColor: Colors.black,
        elevation: 0,
      ),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          const Text(
            "Contact Us",
            style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
          ),
          const SizedBox(height: 10),
          _buildContactCard(
            Icons.phone,
            "Customer Service",
            "+60 12-345 6789",
            () { /* 这里可以写拨号逻辑 */ },
          ),
          _buildContactCard(
            Icons.email,
            "Technical Support",
            "support@ezsystem.com",
            () { /* 这里可以写邮件逻辑 */ },
          ),
          const SizedBox(height: 24),
          const Text(
            "Frequently Asked Questions",
            style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
          ),
          const SizedBox(height: 10),
          const ExpansionTile(
            title: Text("How to reset password?"),
            children: [
              Padding(
                padding: EdgeInsets.all(16),
                child: Text("You can change your password in the Security Settings under your profile."),
              )
            ],
          ),
          const ExpansionTile(
            title: Text("How to add a new product?"),
            children: [
              Padding(
                padding: EdgeInsets.all(16),
                child: Text("Go to the Inventory tab and click the '+' button at the top right."),
              )
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildContactCard(IconData icon, String title, String subtitle, VoidCallback onTap) {
    return Card(
      elevation: 2,
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(12)),
      child: ListTile(
        leading: CircleAvatar(
          backgroundColor: Colors.blue.shade50,
          child: Icon(icon, color: Colors.blue),
        ),
        title: Text(title),
        subtitle: Text(subtitle),
        trailing: const Icon(Icons.arrow_forward_ios, size: 16),
        onTap: onTap,
      ),
    );
  }
}