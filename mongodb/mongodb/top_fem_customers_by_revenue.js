[
  {
    $group: {
      _id: "$customer.customer_id",
      total_spent: { $sum: "$total_revenue" },
      name: { $first: "$customer.first_name" }
    }
  },
  { $sort: { total_spent: -1 } },
  { $limit: 5 }
]