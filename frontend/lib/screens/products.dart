import 'package:flutter/material.dart';

class Product {
  String id;
  String name;
  double cost;
  double price;
  int stock;
  int sold;

  Product({
    required this.id,
    required this.name,
    required this.cost,
    required this.price,
    required this.stock,
    required this.sold,
  });
}

class ProductPage extends StatefulWidget {
  const ProductPage({super.key});

  @override
  State<ProductPage> createState() => _ProductPageState();
}

class _ProductPageState extends State<ProductPage> {
  List<Product> products = [];

  final nameController = TextEditingController();
  final costController = TextEditingController();
  final priceController = TextEditingController();
  final stockController = TextEditingController();
  final soldController = TextEditingController();

  String? editingId;
  bool showForm = false;

  void resetForm() {
    nameController.clear();
    costController.clear();
    priceController.clear();
    stockController.clear();
    soldController.clear();
    editingId = null;
    showForm = false;
  }

  void saveProduct() {
  final name = nameController.text.trim();
  final cost = double.tryParse(costController.text);
  final price = double.tryParse(priceController.text);
  final stock = int.tryParse(stockController.text);
  final sold = int.tryParse(soldController.text);

  // ❌ VALIDATION 1: empty fields
  if (name.isEmpty || cost == null || price == null || stock == null || sold == null) {
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(
        content: Text("❌ Invalid input: Please fill all fields correctly"),
        backgroundColor: Colors.red,
      ),
    );
    return;
  }

  // ❌ VALIDATION 2: sold cannot exceed stock
  if (sold > stock) {
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(
        content: Text("❌ Units sold cannot be more than stock"),
        backgroundColor: Colors.red,
      ),
    );
    return;
  }

  // ❌ VALIDATION 3: negative values
  if (cost < 0 || price < 0 || stock < 0 || sold < 0) {
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(
        content: Text("❌ Values cannot be negative"),
        backgroundColor: Colors.red,
      ),
    );
    return;
  }

  final product = Product(
    id: editingId ?? DateTime.now().millisecondsSinceEpoch.toString(),
    name: name,
    cost: cost,
    price: price,
    stock: stock,
    sold: sold,
  );

  setState(() {
    if (editingId != null) {
      products = products.map((p) => p.id == editingId ? product : p).toList();
    } else {
      products.add(product);
    }
  });

  ScaffoldMessenger.of(context).showSnackBar(
    SnackBar(
      content: Text(editingId != null
          ? "✅ Product updated successfully"
          : "✅ Product added successfully"),
      backgroundColor: Colors.green,
    ),
  );

  resetForm();
}

  void editProduct(Product p) {
    setState(() {
      nameController.text = p.name;
      costController.text = p.cost.toString();
      priceController.text = p.price.toString();
      stockController.text = p.stock.toString();
      soldController.text = p.sold.toString();
      editingId = p.id;
      showForm = true;
    });
  }

  void deleteProduct(String id) {
    setState(() {
      products.removeWhere((p) => p.id == id);
    });
  }

  String stockStatus(int stock) {
    if (stock < 20) return "Low";
    if (stock < 50) return "Medium";
    return "High";
  }

  Color stockColor(String status) {
    switch (status) {
      case "Low":
        return Colors.red;
      case "Medium":
        return Colors.orange;
      default:
        return Colors.green;
    }
  }

  double profit(Product p) => (p.price - p.cost) * p.sold;

  double margin(Product p) => ((p.price - p.cost) / p.price) * 100;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFFF5F6FA),

      appBar: AppBar(
        title: const Text("Products"),
        centerTitle: true,
        backgroundColor: const Color.fromARGB(255, 30, 175, 42),
      ),

      floatingActionButton: FloatingActionButton(
        backgroundColor: const Color.fromARGB(255, 59, 175, 30),
        onPressed: () {
          setState(() => showForm = !showForm);
        },
        child: Icon(showForm ? Icons.close : Icons.add),
      ),

      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [

            /// 📦 FORM SECTION
            if (showForm)
              Card(
                shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    children: [

                      TextField(
                        controller: nameController,
                        decoration: const InputDecoration(labelText: "Product Name"),
                      ),

                      TextField(
                        controller: costController,
                        keyboardType: TextInputType.number,
                        decoration: const InputDecoration(labelText: "Cost Price"),
                      ),

                      TextField(
                        controller: priceController,
                        keyboardType: TextInputType.number,
                        decoration: const InputDecoration(labelText: "Selling Price"),
                      ),

                      TextField(
                        controller: stockController,
                        keyboardType: TextInputType.number,
                        decoration: const InputDecoration(labelText: "Stock"),
                      ),

                      TextField(
                        controller: soldController,
                        keyboardType: TextInputType.number,
                        decoration: const InputDecoration(labelText: "Units Sold"),
                      ),

                      const SizedBox(height: 10),

                      Row(
                        children: [
                          Expanded(
                            child: ElevatedButton(
                              onPressed: saveProduct,
                              child: Text(editingId != null ? "Update" : "Save"),
                            ),
                          ),
                          const SizedBox(width: 10),
                          TextButton(
                            onPressed: resetForm,
                            child: const Text("Cancel"),
                          )
                        ],
                      )
                    ],
                  ),
                ),
              ),

            const SizedBox(height: 20),

            /// 📋 PRODUCT LIST
            if (products.isEmpty)
              const Text("No products yet"),

            ...products.map((p) {
              final status = stockStatus(p.stock);

              return Card(
                margin: const EdgeInsets.only(bottom: 12),
                shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [

                      /// NAME + ACTIONS
                      Row(
                        mainAxisAlignment: MainAxisAlignment.spaceBetween,
                        children: [
                          Text(
                            p.name,
                            style: const TextStyle(
                              fontSize: 16,
                              fontWeight: FontWeight.bold,
                            ),
                          ),

                          Row(
                            children: [
                              IconButton(
                                icon: const Icon(Icons.edit, color: Colors.green),
                                onPressed: () => editProduct(p),
                              ),
                              IconButton(
                                icon: const Icon(Icons.delete, color: Colors.red),
                                onPressed: () => deleteProduct(p.id),
                              ),
                            ],
                          )
                        ],
                      ),

                      /// STOCK STATUS
                      Container(
                        padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
                        decoration: BoxDecoration(
                          color: stockColor(status).withOpacity(0.2),
                          borderRadius: BorderRadius.circular(10),
                        ),
                        child: Text(
                          "Stock: $status",
                          style: TextStyle(color: stockColor(status)),
                        ),
                      ),

                      const SizedBox(height: 10),

                      /// DETAILS
                      Row(
                        mainAxisAlignment: MainAxisAlignment.spaceBetween,
                        children: [
                          Text("Cost: RM${p.cost}"),
                          Text("Price: RM${p.price}"),
                        ],
                      ),

                      Row(
                        mainAxisAlignment: MainAxisAlignment.spaceBetween,
                        children: [
                          Text("Stock: ${p.stock}"),
                          Text("Sold: ${p.sold}"),
                        ],
                      ),

                      const SizedBox(height: 10),

                      /// PROFIT SECTION
                      Container(
                        padding: const EdgeInsets.all(10),
                        decoration: BoxDecoration(
                          gradient: const LinearGradient(
                            colors: [Color(0xFFE0F2FE), Color(0xFFEEF2FF)],
                          ),
                          borderRadius: BorderRadius.circular(12),
                        ),
                        child: Row(
                          mainAxisAlignment: MainAxisAlignment.spaceBetween,
                          children: [
                            Text(
                              "Profit: RM${profit(p).toStringAsFixed(0)}",
                              style: const TextStyle(
                                fontWeight: FontWeight.bold,
                                color: Colors.green,
                              ),
                            ),
                            Text(
                              "Margin: ${margin(p).toStringAsFixed(1)}%",
                              style: const TextStyle(
                                fontWeight: FontWeight.bold,
                                color: Color(0xFF1E40AF),
                              ),
                            ),
                          ],
                        ),
                      ),
                    ],
                  ),
                ),
              );
            }).toList(),
          ],
        ),
      ),
    );
  }
}