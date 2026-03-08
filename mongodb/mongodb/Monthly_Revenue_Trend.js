[
  {
    $group: {
      _id: {
        year: { $year: "$order_date" },
        month: { $month: "$order_date" }
      },
      revenue: { $sum: "$total_revenue" }
    }
  },
  { $sort: { "_id.year": 1, "_id.month": 1 } }
]