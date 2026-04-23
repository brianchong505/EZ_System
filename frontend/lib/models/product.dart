class Product {
  final String id;
  final String name;
  final double price;
  final int stock;
  final double cost; // Kept for your separate stock table logic

  Product({
    required this.id,
    required this.name,
    required this.price,
    required this.stock,
    required this.cost,
  });

  factory Product.fromJson(Map<String, dynamic> json) {
    return Product(
      id: json['product_id'] ?? '',
      name: json['product_name'] ?? '',
      // 使用 ?? 0.0 在解析阶段处理掉可能的 null，这样后续代码就永远不需要再检查 null
      price: (json['selling_price'] as num? ?? 0.0).toDouble(),
      cost: (json['cost_price'] as num? ?? 0.0).toDouble(),
      stock: json['stock'] ?? 0,
    );
  }
}
