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
  final costController = TextEditingController(); // ✅ 新增成本价控制器
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

    final double? cost = double.tryParse(costController.text.trim());
    final double? price = double.tryParse(priceController.text.trim());
    final int? stock = int.tryParse(stockController.text.trim());

    if (cost == null ||
        price == null ||
        stock == null ||
        nameController.text.isEmpty) {
      _showError("Please fill all fields with valid data");
      return;
    }

    final data = {
      "user_id": userId,
      "product_id": editingId ?? "P${DateTime.now().millisecondsSinceEpoch}",
      "product_name": nameController.text.trim(),
      "cost_price": cost, // ✅ 发送成本价
      "selling_price": price, // ✅ 发送售价
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
    bool confirm =
        await showDialog(
          context: context,
          builder: (context) => AlertDialog(
            title: const Text("Delete Product"),
            content: const Text(
              "Are you sure you want to delete this product? This action cannot be undone.",
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.pop(context, false),
                child: const Text("Cancel"),
              ),
              TextButton(
                onPressed: () => Navigator.pop(context, true),
                child: const Text(
                  "Delete",
                  style: TextStyle(color: Colors.red),
                ),
              ),
            ],
          ),
        ) ??
        false;

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
    costController.clear();
  }

  Future<void> handleSaleAction(Product p) async {
    final int? qty = await showQuantityDialog(
      context,
      "Sell Product",
      "Confirm Sale",
      Colors.green,
    );
    if (qty == null || qty <= 0) return;

    if (p.stock < qty) {
      _showError("Not enough stock!");
      return;
    }

    final prefs = await SharedPreferences.getInstance();
    final data = {
      "user_id": prefs.getString('user_id'),
      "product_id": p.id,
      "quantity": qty, // ✅ 传给后端你卖了多少个
      "price": p.price,
      "cost": p.cost,
    };

    bool success = await ApiService.recordSale(data);
    if (success) {
      _refresh();
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text("Sold $qty units of ${p.name}")));
    }
  }

  Future<int?> showQuantityDialog(
    BuildContext context,
    String title,
    String buttonText,
    Color color,
  ) async {
    int quantity = 1;
    final TextEditingController controller = TextEditingController(text: "1");

    return showDialog<int>(
      context: context,
      builder: (context) => StatefulBuilder(
        // 必须用 StatefulBuilder 才能在弹窗里刷新数字
        builder: (context, setDialogState) {
          return AlertDialog(
            title: Text(title),
            content: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                Row(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    // 减号
                    IconButton(
                      icon: const Icon(
                        Icons.remove_circle_outline,
                        color: Colors.red,
                      ),
                      onPressed: () {
                        if (quantity > 1) {
                          setDialogState(() {
                            quantity--;
                            controller.text = quantity.toString();
                          });
                        }
                      },
                    ),
                    // 数字输入框
                    SizedBox(
                      width: 60,
                      child: TextField(
                        controller: controller,
                        textAlign: TextAlign.center,
                        keyboardType: TextInputType.number,
                        decoration: const InputDecoration(
                          border: OutlineInputBorder(),
                        ),
                        onChanged: (val) {
                          quantity = int.tryParse(val) ?? 1;
                        },
                      ),
                    ),
                    // 加号
                    IconButton(
                      icon: const Icon(
                        Icons.add_circle_outline,
                        color: Colors.green,
                      ),
                      onPressed: () {
                        setDialogState(() {
                          quantity++;
                          controller.text = quantity.toString();
                        });
                      },
                    ),
                  ],
                ),
              ],
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.pop(context),
                child: const Text("Cancel"),
              ),
              ElevatedButton(
                style: ElevatedButton.styleFrom(backgroundColor: color),
                onPressed: () => Navigator.pop(context, quantity),
                child: Text(
                  buttonText,
                  style: const TextStyle(color: Colors.white),
                ),
              ),
            ],
          );
        },
      ),
    );
  }

  Future<void> handleRestock(Product p) async {
    final int? qty = await showQuantityDialog(
      context,
      "Restock Inventory",
      "Add Stock",
      Colors.blue,
    );
    if (qty == null || qty <= 0) return;

    final data = {
      "product_id": p.id,
      "adjustment": qty, // ✅ 增加的数量
    };

    bool success = await ApiService.updateStock(data); // 你需要在 ApiService 增加此方法
    if (success) {
      _refresh();
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text("Added $qty units to ${p.name}")));
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text("Inventory"),
        backgroundColor: Colors.green,
      ),
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
            Text(
              editingId == null ? "Add New Product" : "Edit Product",
              style: const TextStyle(fontWeight: FontWeight.bold),
            ),
            TextField(
              controller: nameController,
              decoration: const InputDecoration(labelText: "Product Name"),
            ),

            // ✅ 成本价输入框
            TextField(
              controller: costController,
              keyboardType: TextInputType.number,
              decoration: const InputDecoration(
                labelText: "Cost Price (RM)",
                hintText: "How much you paid",
              ),
            ),

            // ✅ 售价输入框
            TextField(
              controller: priceController,
              keyboardType: TextInputType.number,
              decoration: const InputDecoration(
                labelText: "Selling Price (RM)",
                hintText: "How much you sell",
              ),
            ),

            TextField(
              controller: stockController,
              keyboardType: TextInputType.number,
              decoration: const InputDecoration(labelText: "Initial Stock"),
            ),
            const SizedBox(height: 10),
            const SizedBox(height: 10),
            ElevatedButton(
              onPressed: handleSave,
              style: ElevatedButton.styleFrom(backgroundColor: Colors.green),
              child: const Text(
                "Save Product",
                style: TextStyle(color: Colors.white),
              ),
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
          return Center(
            child: Text("Error loading inventory: ${snapshot.error}"),
          );
        }

        if (!snapshot.hasData || snapshot.data!.isEmpty) {
          return const Center(child: Text("No products found in database."));
        }

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
            final profit = p.price - p.cost; // 假设你的 Product Model 有 cost 字段

            return Card(
              child: ListTile(
                title: Text(p.name),
                subtitle: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      "Cost: RM ${p.cost?.toStringAsFixed(2)} | Sell: RM ${p.price.toStringAsFixed(2)}",
                    ),
                    Text(
                      "Profit/Unit: RM ${profit.toStringAsFixed(2)}",
                      style: TextStyle(
                        color: profit > 0 ? Colors.green : Colors.red,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    Text("Stock: ${p.stock}"),
                  ],
                ),
                // ✅ 所有的按钮都放在这一个 Row 的 children 列表里
                trailing: Row(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    // 1. 补货按钮
                    IconButton(
                      icon: const Icon(Icons.add_business, color: Colors.blue),
                      onPressed: () => handleRestock(p),
                      tooltip: "Add Stock",
                    ),
                    // 2. 售出按钮
                    IconButton(
                      icon: const Icon(
                        Icons.shopping_cart_checkout,
                        color: Colors.green,
                      ),
                      onPressed: () => handleSaleAction(p),
                      tooltip: "Record Sale",
                    ),
                    // 3. 编辑按钮
                    IconButton(
                      icon: const Icon(Icons.edit, color: Colors.orange),
                      onPressed: () {
                        setState(() {
                          editingId = p.id;
                          nameController.text = p.name;
                          priceController.text = p.price.toString();
                          stockController.text = p.stock.toString();
                          showForm = true;
                        });
                      },
                    ),
                    // 4. 删除按钮
                    IconButton(
                      icon: const Icon(Icons.delete, color: Colors.red),
                      onPressed: () => handleDelete(p.id),
                    ),
                  ],
                ),
              ),
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
