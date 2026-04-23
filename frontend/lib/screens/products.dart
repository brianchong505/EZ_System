import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../services/api_service.dart';
import '../models/product.dart'; // ✅ Ensure this path is correct

class ProductPage extends StatefulWidget {
  const ProductPage({super.key});
  @override
  State<ProductPage> createState() => _ProductPageState();
}

class _ProductPageState extends State<ProductPage> {
  late Future<List<Product>> _productsFuture;
  
  // Controllers for the Form
  final nameController = TextEditingController();
  final priceController = TextEditingController();
  final stockController = TextEditingController();
  
  // Controller and variable for Search
  final searchController = TextEditingController();
  String searchQuery = ""; 

  String? editingId;
  bool showForm = false;

  @override
  void initState() {
    super.initState();
    _refresh();
  }

  // Fetches data from API and returns it as a Future
  Future<List<Product>> _fetchProducts() async {
    final prefs = await SharedPreferences.getInstance();
    final userId = prefs.getString('user_id') ?? "";
    return await ApiService.getProducts(userId);
  }

  void _refresh() {
    setState(() {
      _productsFuture = _fetchProducts();
    });
  }

  Future<void> handleSave() async {
  final prefs = await SharedPreferences.getInstance();
  final String? userId = prefs.getString('user_id'); // 获取当前登录的新 ID

  if (userId == null) {
    _showError("User not logged in. Please re-login.");
    return;
  }

  final double? price = double.tryParse(priceController.text.trim());
  final int? stock = int.tryParse(stockController.text.trim());

  if (price == null || stock == null || nameController.text.isEmpty) {
    _showError("Please fill all fields with valid data");
    return;
  }

  final data = {
    "user_id": userId, // ✅ 确保使用最新 U177... ID
    "product_id": editingId ?? "P${DateTime.now().millisecondsSinceEpoch}",
    "product_name": nameController.text.trim(),
    "selling_price": price,
    "stock": stock,
  };

  bool success = editingId != null 
      ? await ApiService.updateProduct(data) 
      : await ApiService.addProduct(data);

  if (success) {
    _refresh();
    setState(() { 
      showForm = false; 
      editingId = null; 
    });
    // 清空控制器
    _clearInputs();
  } else {
    _showError("Failed to save product. Check backend logs.");
  }
}

Future<void> handleDelete(String productId) async {
  // 弹出确认对话框
  bool confirm = await showDialog(
    context: context,
    builder: (context) => AlertDialog(
      title: const Text("Delete Product"),
      content: const Text("Are you sure you want to delete this product? This action cannot be undone."),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(context, false), 
          child: const Text("Cancel"),
        ),
        TextButton(
          onPressed: () => Navigator.pop(context, true), 
          child: const Text("Delete", style: TextStyle(color: Colors.red)),
        ),
      ],
    ),
  ) ?? false;

  if (confirm) {
    bool success = await ApiService.deleteProduct(productId);
    if (success) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text("Product deleted successfully")),
      );
      _refresh(); // 重新拉取数据，界面就会消失
    } else {
      _showError("Failed to delete product.");
    }
  }
}

void _clearInputs() {
  nameController.clear(); 
  priceController.clear(); 
  stockController.clear();
}

  Future<void> handleSaleAction(Product p) async {
  final prefs = await SharedPreferences.getInstance();
  final userId = prefs.getString('user_id');

  if (p.stock <= 0) {
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(content: Text("Out of stock!")),
    );
    return;
  }

  final data = {
    "user_id": userId,
    "product_id": p.id,
    "price": p.price,
    "cost": p.price * 0.6, 
  };

  bool success = await ApiService.recordSale(data);

  if (success) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text("Sold 1 unit of ${p.name}!")),
    );
    
    // ✅ 刷新当前 Product 列表
    _refresh(); 

    // ✅ 重要：通知 Dashboard 刷新。
    // 如果你没有使用全局状态管理（Provider），可以通过返回结果通知上一层
    // 或者在全局设置一个 Flag。
  } else {
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(content: Text("Failed to record sale.")),
    );
  }
}

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text("Inventory"), backgroundColor: Colors.green),
      floatingActionButton: FloatingActionButton(
        onPressed: () {
          setState(() {
            showForm = !showForm;
            if (!showForm) editingId = null; // Reset edit state if closing
          });
        },
        child: Icon(showForm ? Icons.close : Icons.add),
      ),
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [
            // Search Bar
            TextField(
              controller: searchController,
              decoration: const InputDecoration(
                labelText: "Search products...",
                prefixIcon: Icon(Icons.search),
                border: OutlineInputBorder(),
              ),
              onChanged: (value) {
                setState(() {
                  searchQuery = value.toLowerCase();
                });
              },
            ),
            const SizedBox(height: 10),
            if (showForm) _buildForm(),
            const SizedBox(height: 20),
            _buildList(),
          ],
        ),
      ),
    );
  }

  Widget _buildForm() {
    return Card(
      elevation: 4,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [
            Text(editingId == null ? "Add New Product" : "Edit Product", 
                 style: const TextStyle(fontWeight: FontWeight.bold)),
            TextField(controller: nameController, decoration: const InputDecoration(labelText: "Name")),
            TextField(controller: priceController, keyboardType: TextInputType.number, decoration: const InputDecoration(labelText: "Price (RM)")),
            TextField(controller: stockController, keyboardType: TextInputType.number, decoration: const InputDecoration(labelText: "Stock")),
            const SizedBox(height: 10),
            ElevatedButton(
              onPressed: handleSave, 
              style: ElevatedButton.styleFrom(backgroundColor: Colors.green),
              child: const Text("Save Product", style: TextStyle(color: Colors.white)),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildList() {
    return FutureBuilder<List<Product>>(
      future: _productsFuture,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const Center(child: CircularProgressIndicator());
        }
        
        if (snapshot.hasError) {
          return Center(child: Text("Error loading inventory: ${snapshot.error}"));
        }

        if (!snapshot.hasData || snapshot.data!.isEmpty) {
          return const Center(child: Text("No products found in database."));
        }

        // Apply Search Filtering
        final filteredList = snapshot.data!.where((p) {
          return p.name.toLowerCase().contains(searchQuery);
        }).toList();

        if (filteredList.isEmpty) {
          return const Center(child: Text("No products match your search."));
        }

        return ListView.builder(
          shrinkWrap: true, 
          physics: const NeverScrollableScrollPhysics(),
          itemCount: filteredList.length,
          itemBuilder: (context, index) {
            final p = filteredList[index];
            return Card(
              child: ListTile(
                title: Text(p.name),
                subtitle: Text("RM ${p.price.toStringAsFixed(2)} | Stock: ${p.stock}"),
                // 增加删除按钮
                trailing: Row(
                mainAxisSize: MainAxisSize.min, // 关键：让 Row 只占用必要的宽度
                children: [
                  // 1. 售出按钮：点击后 Dashboard 数据才会变
                  IconButton(
                    icon: const Icon(Icons.shopping_cart_checkout, color: Colors.green),
                    onPressed: () => handleSaleAction(p), 
                    tooltip: "Record a sale",
                    ),IconButton(
                  icon: const Icon(Icons.edit, color: Colors.blue), 
                  onPressed: () {
                    setState(() {
                      editingId = p.id;
                      nameController.text = p.name;
                      priceController.text = p.price.toString();
                      stockController.text = p.stock.toString();
                      showForm = true;
                    });
                  },
                )
                ,IconButton(
                  icon: const Icon(Icons.delete, color: Colors.red), 
                  onPressed: () => handleDelete(p.id),
                ),
                ],
              ),
              )
            );
          },
        );
      },
    );
  }

  void _showError(String message) {
    if (!mounted) return; // 确保 context 还在
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(message),
        backgroundColor: Colors.redAccent,
        behavior: SnackBarBehavior.floating, // 让提示悬浮，更好看
      ),
    );
  }
}