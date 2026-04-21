import 'package:flutter/material.dart';

class Product {
  String id;
  String name;
  String category;
  double price;
  double cost;
  int stock;
  String trend; // up / down / stable
  int trendValue;
  String image;

  Product({
    required this.id,
    required this.name,
    required this.category,
    required this.price,
    required this.cost,
    required this.stock,
    required this.trend,
    required this.trendValue,
    required this.image,
  });
}

class ProductListPage extends StatefulWidget {
  const ProductListPage({super.key});

  @override
  State<ProductListPage> createState() => _ProductListPageState();
}

class _ProductListPageState extends State<ProductListPage> {
  String searchQuery = "";
  String selectedCategory = "All";

  final List<String> categories = ["All", "Beverages", "Bakery", "Food"];

  final List<Product> products = [
    Product(
        id: "1",
        name: "Premium Arabica Coffee",
        category: "Beverages",
        price: 12.99,
        cost: 6.5,
        stock: 245,
        trend: "up",
        trendValue: 12,
        image: "☕"),
    Product(
        id: "2",
        name: "Organic Croissant",
        category: "Bakery",
        price: 3.99,
        cost: 1.8,
        stock: 180,
        trend: "up",
        trendValue: 8,
        image: "🥐"),
    Product(
        id: "3",
        name: "Avocado Toast",
        category: "Food",
        price: 8.99,
        cost: 4.2,
        stock: 95,
        trend: "down",
        trendValue: -5,
        image: "🥑"),
    Product(
        id: "4",
        name: "Matcha Latte",
        category: "Beverages",
        price: 5.99,
        cost: 2.8,
        stock: 320,
        trend: "up",
        trendValue: 15,
        image: "🍵"),
    Product(
        id: "5",
        name: "Blueberry Muffin",
        category: "Bakery",
        price: 4.49,
        cost: 2.1,
        stock: 150,
        trend: "stable",
        trendValue: 0,
        image: "🧁"),
    Product(
        id: "6",
        name: "Cold Brew Coffee",
        category: "Beverages",
        price: 4.99,
        cost: 2.2,
        stock: 280,
        trend: "up",
        trendValue: 18,
        image: "🧊"),
  ];

  List<Product> get filteredProducts {
    return products.where((p) {
      final matchSearch =
          p.name.toLowerCase().contains(searchQuery.toLowerCase());

      final matchCategory =
          selectedCategory == "All" || p.category == selectedCategory;

      return matchSearch && matchCategory;
    }).toList();
  }

  Color trendColor(String trend) {
    if (trend == "up") return Colors.green;
    if (trend == "down") return Colors.red;
    return Colors.grey;
  }

  @override
  Widget build(BuildContext context) {
    final totalStock =
        products.fold(0, (sum, p) => sum + p.stock);

    return Scaffold(
      backgroundColor: const Color(0xFFF5F6FA),

      /// APP BAR
      appBar: AppBar(
        backgroundColor: Colors.white,
        elevation: 1,
        title: const Text("Products"),
        actions: [
          IconButton(
            icon: const Icon(Icons.add, color: Color(0xFF2ECC71)),
            onPressed: () {},
          )
        ],
      ),

      body: Column(
        children: [

          /// SEARCH + FILTER
          Padding(
            padding: const EdgeInsets.all(12),
            child: Column(
              children: [

                /// SEARCH
                TextField(
                  onChanged: (v) => setState(() => searchQuery = v),
                  decoration: InputDecoration(
                    hintText: "Search products...",
                    prefixIcon: const Icon(Icons.search),
                    filled: true,
                    fillColor: Colors.white,
                    border: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(12),
                      borderSide: BorderSide.none,
                    ),
                  ),
                ),

                const SizedBox(height: 10),

                /// CATEGORY FILTER
                SizedBox(
                  height: 40,
                  child: ListView.builder(
                    scrollDirection: Axis.horizontal,
                    itemCount: categories.length,
                    itemBuilder: (context, index) {
                      final cat = categories[index];

                      final isSelected = selectedCategory == cat;

                      return GestureDetector(
                        onTap: () {
                          setState(() => selectedCategory = cat);
                        },
                        child: Container(
                          margin: const EdgeInsets.only(right: 8),
                          padding: const EdgeInsets.symmetric(horizontal: 16),
                          decoration: BoxDecoration(
                            color: isSelected
                                ? const Color(0xFF2ECC71)
                                : Colors.grey[200],
                            borderRadius: BorderRadius.circular(12),
                          ),
                          alignment: Alignment.center,
                          child: Text(
                            cat,
                            style: TextStyle(
                              color: isSelected
                                  ? Colors.white
                                  : Colors.black,
                            ),
                          ),
                        ),
                      );
                    },
                  ),
                ),
              ],
            ),
          ),

          /// HEADER INFO
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 12),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Text("${filteredProducts.length} products found"),
                Row(
                  children: [
                    const Icon(Icons.inventory, size: 16),
                    const SizedBox(width: 4),
                    Text("Total Stock: $totalStock"),
                  ],
                )
              ],
            ),
          ),

          const SizedBox(height: 10),

          /// PRODUCT LIST
          Expanded(
            child: ListView.builder(
              padding: const EdgeInsets.all(12),
              itemCount: filteredProducts.length,
              itemBuilder: (context, index) {
                final p = filteredProducts[index];

                return GestureDetector(
                  onTap: () {
                    Navigator.pushNamed(context, "/product/${p.id}");
                  },
                  child: Card(
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(16),
                    ),
                    margin: const EdgeInsets.only(bottom: 12),
                    child: Padding(
                      padding: const EdgeInsets.all(12),
                      child: Row(
                        children: [

                          /// IMAGE
                          Container(
                            width: 60,
                            height: 60,
                            decoration: BoxDecoration(
                              color: Colors.grey[200],
                              borderRadius: BorderRadius.circular(12),
                            ),
                            child: Center(
                              child: Text(
                                p.image,
                                style: const TextStyle(fontSize: 28),
                              ),
                            ),
                          ),

                          const SizedBox(width: 12),

                          /// INFO
                          Expanded(
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  p.name,
                                  style: const TextStyle(
                                    fontWeight: FontWeight.bold,
                                  ),
                                ),
                                Text(
                                  p.category,
                                  style: const TextStyle(
                                    fontSize: 12,
                                    color: Colors.grey,
                                  ),
                                ),

                                const SizedBox(height: 8),

                                Row(
                                  children: [
                                    Text("RM${p.price}"),
                                    const SizedBox(width: 10),
                                    Text(
                                      "RM${p.cost}",
                                      style: const TextStyle(
                                        color: Colors.grey,
                                      ),
                                    ),
                                    const SizedBox(width: 10),
                                    Text("Stock: ${p.stock}"),
                                  ],
                                ),
                              ],
                            ),
                          ),

                          /// TREND
                          Column(
                            children: [
                              Icon(
                                p.trend == "up"
                                    ? Icons.trending_up
                                    : p.trend == "down"
                                        ? Icons.trending_down
                                        : Icons.remove,
                                color: trendColor(p.trend),
                              ),
                              Text(
                                "${p.trendValue}%",
                                style: TextStyle(
                                  color: trendColor(p.trend),
                                  fontWeight: FontWeight.bold,
                                ),
                              )
                            ],
                          )
                        ],
                      ),
                    ),
                  ),
                );
              },
            ),
          ),
        ],
      ),
    );
  }
}