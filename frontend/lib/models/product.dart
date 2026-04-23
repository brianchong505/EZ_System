class Product {
  final String id;
  final String name;
  final double price;
  final int stock; // Kept for your separate stock table logic

  Product({
    required this.id,
    required this.name,
    required this.price,
    required this.stock,
  });

  factory Product.fromJson(Map<String, dynamic> json) {
  return Product(
    id: json['product_id']?.toString() ?? '',
    name: json['product_name'] ?? '',
    price: (json['selling_price'] ?? 0.0).toDouble(),
    // Ensure this matches the 'as stock' alias in your SQL query above
    stock: (json['stock'] ?? 0).toInt(), 
  );
}
}