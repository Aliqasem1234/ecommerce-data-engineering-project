[
  {
    $unwind: "$items"
  },
  {
    $group: {
      _id: {
        customer: "$customer.customer_id",
        product: "$items.product_id"
      },
      total_quantity: {
        $sum: "$items.quantity"
      }
    }
  },
  {
    $sort: {
      total_quantity: -1
    }
  },
  {
    $limit: 10
  }
]