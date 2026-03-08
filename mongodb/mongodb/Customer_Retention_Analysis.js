[
  {
    $group: {
      _id: "$customer.customer_id",
      order_count: { $sum: 1 }
    }
  },

  {
    $match: {
      order_count: { $gt: 1 }
    }
  },

  {
    $group: {
      _id: null,
      returning_customers: { $sum: 1 }
    }
  }
]