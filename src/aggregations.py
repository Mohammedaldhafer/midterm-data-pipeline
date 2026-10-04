from __future__ import annotations



import argparse

from typing import Any



from pymongo import MongoClient





MONGO_URI = "mongodb://localhost:27017"

DATABASE_NAME = "midterm_data_pipeline"

COLLECTION_NAME = "orders_validated"





def get_collection():

    client = MongoClient(

        MONGO_URI,

        serverSelectionTimeoutMS=5000,

    )

    client.admin.command("ping")

    return client, client[DATABASE_NAME][COLLECTION_NAME]





def amount_expression(field: str = "$total_amount") -> dict[str, Any]:

    """

    Convert total_amount from string to numeric.



    The current dataset contains values such as:

    135,000.00

    706000٫0

    546500.0

    """



    normalized = {

        "$replaceAll": {

            "input": {"$ifNull": [field, "0"]},

            "find": ",",

            "replacement": "",

        }

    }



    normalized = {

        "$replaceAll": {

            "input": normalized,

            "find": "٫",

            "replacement": ".",

        }

    }



    return {

        "$convert": {

            "input": normalized,

            "to": "double",

            "onError": 0,

            "onNull": 0,

        }

    }





def date_expression(field: str = "$order_date") -> dict[str, Any]:

    """Convert order_date to a MongoDB Date."""



    return {

        "$convert": {

            "input": field,

            "to": "date",

            "onError": None,

            "onNull": None,

        }

    }





def run_sales_by_city(

    collection,

    limit: int = 20,

) -> list[dict[str, Any]]:

    """Report total sales and order count by city."""



    pipeline = [

        {

            "$group": {

                "_id": "$city",

                "order_count": {"$sum": 1},

                "total_sales": {"$sum": amount_expression()},

            }

        },

        {

            "$sort": {

                "total_sales": -1,

            }

        },

        {

            "$limit": limit,

        },

        {

            "$project": {

                "_id": 0,

                "city": "$_id",

                "order_count": 1,

                "total_sales": {

                    "$round": ["$total_sales", 2],

                },

            }

        },

    ]



    return list(collection.aggregate(pipeline, allowDiskUse=True))





def run_top_products(

    collection,

    limit: int = 20,

) -> list[dict[str, Any]]:

    """

    Report the top products by quantity and sales.



    items_json is stored as a JSON string in the current dataset,

    so the pipeline safely parses either a JSON string or an array.

    """



    pipeline = [

        {

            "$set": {

                "_items": {

                    "$function": {

                        "body": """

                            function(value) {

                                if (Array.isArray(value)) {

                                    return value;

                                }



                                if (typeof value === "string") {

                                    try {

                                        return JSON.parse(value);

                                    } catch (e) {

                                        return [];

                                    }

                                }



                                return [];

                            }

                        """,

                        "args": ["$items_json"],

                        "lang": "js",

                    }

                }

            }

        },

        {

            "$unwind": "$_items",

        },

        {

            "$set": {

                "_qty": {

                    "$convert": {

                        "input": {"$ifNull": ["$_items.qty", 0]},

                        "to": "double",

                        "onError": 0,

                        "onNull": 0,

                    }

                },

                "_item_total": {

                    "$convert": {

                        "input": {"$ifNull": ["$_items.total", 0]},

                        "to": "double",

                        "onError": 0,

                        "onNull": 0,

                    }

                },

            }

        },

        {

            "$group": {

                "_id": {

                    "sku": "$_items.sku",

                    "name": "$_items.name",

                },

                "quantity_sold": {"$sum": "$_qty"},

                "sales": {"$sum": "$_item_total"},

            }

        },

        {

            "$sort": {

                "sales": -1,

                "quantity_sold": -1,

            }

        },

        {

            "$limit": limit,

        },

        {

            "$project": {

                "_id": 0,

                "sku": "$_id.sku",

                "product_name": "$_id.name",

                "quantity_sold": {"$round": ["$quantity_sold", 2]},

                "sales": {"$round": ["$sales", 2]},

            }

        },

    ]



    return list(collection.aggregate(pipeline, allowDiskUse=True))





def run_top_customers(

    collection,

    limit: int = 20,

) -> list[dict[str, Any]]:

    """Report customers ranked by total sales."""



    pipeline = [

        {

            "$group": {

                "_id": "$customer_id",

                "order_count": {"$sum": 1},

                "total_sales": {"$sum": amount_expression()},

            }

        },

        {

            "$sort": {

                "total_sales": -1,

            }

        },

        {

            "$limit": limit,

        },

        {

            "$project": {

                "_id": 0,

                "customer_id": "$_id",

                "order_count": 1,

                "total_sales": {

                    "$round": ["$total_sales", 2],

                },

            }

        },

    ]



    return list(collection.aggregate(pipeline, allowDiskUse=True))





def run_sales_by_month(

    collection,

    limit: int | None = None,

) -> list[dict[str, Any]]:

    """Report sales and order count by month."""



    pipeline = [

        {

            "$set": {

                "_order_date": date_expression(),

            }

        },

        {

            "$match": {

                "_order_date": {

                    "$ne": None,

                }

            }

        },

        {

            "$group": {

                "_id": {

                    "$dateToString": {

                        "format": "%Y-%m",

                        "date": "$_order_date",

                    }

                },

                "order_count": {"$sum": 1},

                "total_sales": {"$sum": amount_expression()},

            }

        },

        {

            "$sort": {

                "_id": 1,

            }

        },

    ]



    if limit is not None:

        pipeline.append({"$limit": limit})



    pipeline.append(

        {

            "$project": {

                "_id": 0,

                "month": "$_id",

                "order_count": 1,

                "total_sales": {

                    "$round": ["$total_sales", 2],

                },

            }

        }

    )



    return list(collection.aggregate(pipeline, allowDiskUse=True))





def run_orders_by_status(

    collection,

    limit: int = 20,

) -> list[dict[str, Any]]:

    """Report order count and sales distribution by status."""



    pipeline = [

        {

            "$group": {

                "_id": "$status",

                "order_count": {"$sum": 1},

                "total_sales": {"$sum": amount_expression()},

            }

        },

        {

            "$sort": {

                "order_count": -1,

            }

        },

        {

            "$limit": limit,

        },

        {

            "$project": {

                "_id": 0,

                "status": "$_id",

                "order_count": 1,

                "total_sales": {

                    "$round": ["$total_sales", 2],

                },

            }

        },

    ]



    return list(collection.aggregate(pipeline, allowDiskUse=True))







def run_sales_by_payment_method(
    collection,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Report order count and sales by payment method."""

    pipeline = [
        {
            "$group": {
                "_id": "$payment_method",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": amount_expression()},
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "payment_method": "$_id",
                "order_count": 1,
                "total_sales": {"$round": ["$total_sales", 2]},
            }
        },
    ]

    return list(collection.aggregate(pipeline, allowDiskUse=True))


def run_sales_by_delivery_type(
    collection,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Report order count and sales by delivery type."""

    pipeline = [
        {
            "$group": {
                "_id": "$delivery_type",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": amount_expression()},
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "delivery_type": "$_id",
                "order_count": 1,
                "total_sales": {"$round": ["$total_sales", 2]},
            }
        },
    ]

    return list(collection.aggregate(pipeline, allowDiskUse=True))


REPORTS = {
    # Core five reports selected for the final-project requirement.
    "sales_by_city": run_sales_by_city,
    "orders_by_status": run_orders_by_status,
    "sales_by_month": run_sales_by_month,
    "sales_by_payment_method": run_sales_by_payment_method,
    "sales_by_delivery_type": run_sales_by_delivery_type,

    # Additional useful reports available through the same interface.
    "top_products": run_top_products,
    "top_customers": run_top_customers,
}

def run_report(

    name: str,

    collection,

    limit: int = 20,

) -> list[dict[str, Any]]:

    """Run one named aggregation report."""



    if name not in REPORTS:

        available = ", ".join(REPORTS)

        raise ValueError(

            f"Unknown report '{name}'. Available: {available}"

        )



    if name == "sales_by_month":

        return REPORTS[name](collection)



    return REPORTS[name](collection, limit)





def run_all_reports(collection, limit: int = 20):

    """Run all registered aggregation reports independently."""



    for name in REPORTS:

        print()

        print("=" * 80)

        print(f"REPORT: {name}")

        print("=" * 80)



        result = run_report(

            name,

            collection,

            limit=limit,

        )



        print(f"Rows returned: {len(result)}")



        for row in result:

            print(row)





def main():

    parser = argparse.ArgumentParser(

        description="MongoDB aggregation reports for the final project."

    )



    parser.add_argument(

        "--report",

        choices=list(REPORTS) + ["all"],

        default="all",

        help="Report name or all reports.",

    )



    parser.add_argument(

        "--limit",

        type=int,

        default=20,

        help="Maximum rows returned by ranked reports.",

    )



    args = parser.parse_args()



    client, collection = get_collection()



    try:

        if args.report == "all":

            run_all_reports(

                collection,

                limit=args.limit,

            )

        else:

            print("=" * 80)

            print(f"REPORT: {args.report}")

            print("=" * 80)



            result = run_report(

                args.report,

                collection,

                limit=args.limit,

            )



            print(f"Rows returned: {len(result)}")



            for row in result:

                print(row)



    finally:

        client.close()





if __name__ == "__main__":

    main()