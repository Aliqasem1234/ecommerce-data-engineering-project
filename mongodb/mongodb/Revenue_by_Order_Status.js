[
  {
    $group: {
      _id: "$status",
      total_revenue: { $sum: "$total_revenue" },
      orders: { $sum: 1 }
    }
  }
]