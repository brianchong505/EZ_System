import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:http/http.dart' as http;
import 'package:shared_preferences/shared_preferences.dart';
import '../models/product.dart'; // 请确保路径正确

class ApiService {
  // 模拟器访问电脑本地后端的固定 IP
  static const String baseUrl = "http://10.0.2.2:8000";

  // ==========================================
  // 1. 用户认证 (Auth)
  // ==========================================

  /// 登录
  static Future<bool> login(String email, String password) async {
    try {
      final response = await http.post(
        Uri.parse('$baseUrl/auth/login'),
        headers: {'Content-Type': 'application/json'},
        body: jsonEncode({'email': email, 'password': password}),
      );

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        final prefs = await SharedPreferences.getInstance();
        if (data['user_id'] != null) {
          await prefs.setString('user_id', data['user_id'].toString());
          return true;
        }
      }
      return false;
    } catch (e) {
      debugPrint("Login Error: $e");
      rethrow;
    }
  }

  /// 注册
  static Future<bool> register(String email, String password, String businessName) async {
    try {
      final String generatedUserId = "U${DateTime.now().millisecondsSinceEpoch}";
      final response = await http.post(
        Uri.parse("$baseUrl/auth/register"),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode({
          "user_id": generatedUserId,
          "email": email,
          "password": password,
          "business_name": businessName,
        }),
      );

      if (response.statusCode == 200 || response.statusCode == 201) {
        final prefs = await SharedPreferences.getInstance();
        await prefs.setString('user_id', generatedUserId);
        await prefs.setString('business_name', businessName);
        return true;
      } else {
        final errorData = jsonDecode(response.body);
        throw Exception(errorData['detail'] ?? "Registration failed");
      }
    } catch (e) {
      debugPrint("Register Error: $e");
      rethrow;
    }
  }

  // ==========================================
  // 2. 安全与设置 (Security & Settings)
  // ==========================================

  /// 修改密码
  static Future<Map<String, dynamic>> changePassword(String userId, String oldPwd, String newPwd) async {
    try {
      final response = await http.post(
        Uri.parse("$baseUrl/users/change-password"),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode({
          "user_id": userId,
          "old_password": oldPwd,
          "new_password": newPwd,
        }),
      );
      return jsonDecode(response.body);
    } catch (e) {
      return {"status": "error", "message": "Network error: $e"};
    }
  }

  /// 修改邮箱
  static Future<Map<String, dynamic>> changeEmail(String userId, String password, String newEmail) async {
    try {
      final response = await http.post(
        Uri.parse("$baseUrl/users/change-email"),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode({
          "user_id": userId,
          "password": password,
          "new_email": newEmail,
        }),
      );
      return jsonDecode(response.body);
    } catch (e) {
      return {"status": "error", "message": "Network error: $e"};
    }
  }

  /// 获取用户资料
  static Future<Map<String, dynamic>> getUserProfile(String userId) async {
    final response = await http.get(Uri.parse("$baseUrl/users/$userId"));
    if (response.statusCode == 200) {
      return jsonDecode(response.body);
    } else {
      throw Exception("Failed to load profile");
    }
  }

  /// 更新资料 (姓名/描述等)
  static Future<bool> updateUserProfile(String userId, Map<String, dynamic> data) async {
    try {
      final response = await http.put(
        Uri.parse("$baseUrl/users/$userId"),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode(data),
      );
      return response.statusCode == 200;
    } catch (e) {
      debugPrint("Update Profile Error: $e");
      return false;
    }
  }

  // ==========================================
  // 3. 业务功能 (Products, Stock, Sales)
  // ==========================================

  /// 获取产品列表
  static Future<List<Product>> getProducts(String userId) async {
    try {
      final response = await http.get(Uri.parse("$baseUrl/products/$userId"));
      if (response.statusCode == 200) {
        List data = jsonDecode(response.body);
        return data.map((item) => Product.fromJson(item)).toList();
      }
      return [];
    } catch (e) {
      debugPrint("Fetch Products Error: $e");
      return [];
    }
  }

  /// 添加产品
  static Future<bool> addProduct(Map<String, dynamic> data) async {
    try {
      final response = await http.post(
        Uri.parse("$baseUrl/products/"),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode(data),
      );
      return response.statusCode == 200;
    } catch (e) {
      debugPrint("Add Product Error: $e");
      return false;
    }
  }

  /// 更新产品信息
  static Future<bool> updateProduct(Map<String, dynamic> productData) async {
    try {
      final String productId = productData['product_id'];
      final response = await http.put(
        Uri.parse("$baseUrl/products/$productId"),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode(productData),
      );
      return response.statusCode == 200;
    } catch (e) {
      debugPrint("Update Product Error: $e");
      return false;
    }
  }

  /// 更新库存 (Restock)
  static Future<bool> updateStock(Map<String, dynamic> data) async {
    try {
      final response = await http.post(
        Uri.parse("$baseUrl/products/restock"),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode(data),
      );
      return response.statusCode == 200;
    } catch (e) {
      debugPrint("Restock Error: $e");
      return false;
    }
  }

  /// 删除产品
  static Future<bool> deleteProduct(String productId) async {
    final response = await http.delete(
      Uri.parse("$baseUrl/products/$productId"),
    );
    return response.statusCode == 200;
  }

  /// 记录销售记录
  static Future<bool> recordSale(Map<String, dynamic> data) async {
    try {
      final response = await http.post(
        Uri.parse("$baseUrl/sales/record"),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode(data),
      );
      return response.statusCode == 200;
    } catch (e) {
      debugPrint("Record Sale Error: $e");
      return false;
    }
  }

  // ==========================================
  // 4. 数据看板与预警 (Dashboard & Alerts)
  // ==========================================

  /// 获取看板数据
  static Future<Map<String, dynamic>> getDashboard(String userId) async {
    try {
      final response = await http.get(Uri.parse("$baseUrl/dashboard/$userId"));
      if (response.statusCode == 200) {
        return jsonDecode(response.body);
      }
      return {"revenue": 0.0, "cost": 0.0};
    } catch (e) {
      debugPrint("Dashboard API Error: $e");
      return {"revenue": 0.0, "cost": 0.0};
    }
  }

  /// 获取 AI 预测数据
  static Future<Map<String, dynamic>> getForecast() async {
    try {
      final response = await http.get(Uri.parse("$baseUrl/dashboard/demand-forecast"));
      if (response.statusCode == 200) {
        return jsonDecode(response.body);
      }
      return {"forecast": 0, "trend": "unknown", "explanation": "No data."};
    } catch (e) {
      debugPrint("Forecast API Error: $e");
      return {"forecast": 0, "trend": "error", "explanation": "Connection failed."};
    }
  }

  /// 获取库存预警
  static Future<List<dynamic>> getAlerts(String userId) async {
    try {
      final response = await http.get(Uri.parse("$baseUrl/alerts/$userId"));
      if (response.statusCode == 200) {
        return jsonDecode(response.body);
      }
      return [];
    } catch (e) {
      debugPrint("Alerts API Error: $e");
      return [];
    }
  }
}