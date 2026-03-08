[
  { $unwind: "$items" },

  {
    $group: {
      _id: "$order_id",
      products: { $push: "$items.product_id" }
    }
  },

  {
    $project: {
      pairs: {
        $reduce: {
          input: "$products",
          initialValue: [],
          in: {
            $concatArrays: [
              "$$value",
              {
                $map: {
                  input: "$products",
                  as: "p",
                  in: {
                    product1: "$$this",
                    product2: "$$p"
                  }
                }
              }
            ]
          }
        }
      }
    }
  },

  { $unwind: "$pairs" },

  {
    $match: {
      $expr: {
        $ne: [
          "$pairs.product1",
          "$pairs.product2"
        ]
      }
    }
  },

  {
    $group: {
      _id: {
        product1: "$pairs.product1",
        product2: "$pairs.product2"
      },
      frequency: { $sum: 1 }
    }
  },

  { $sort: { frequency: -1 } },
  { $limit: 10 }
]