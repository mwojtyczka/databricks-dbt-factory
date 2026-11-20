# Databricks notebook source
"""Control task for the jaffle-shop reuse benchmark.

action=reset   regenerates the raw sources at scale factor `sf`, drops every table and view in the arm
               schema and, for the on arm, initializes reuse state there.
action=append  appends ~1% new orders and their items to the raw sources.
action=capture runs the shipped reuse capture in this serverless environment (its context fingerprints the
               runtime packages) and uploads the manifest and provenance to the artifact volume.
"""
# pylint: skip-file
# mypy: ignore-errors

import io
import json
import os

ORDERS_PER_SF = 1_000_000
EPOCH_2024 = 1704067200
STORES = 6
# (sku, name, type, price in cents). Types drive stg_products flags; prices drive order subtotals.
PRODUCTS = [
    ("JAF-001", "the nutellaphone", "jaffle", 1100),
    ("JAF-002", "the doctor", "jaffle", 1000),
    ("JAF-003", "the flying pig", "jaffle", 1200),
    ("JAF-004", "the dolce", "jaffle", 1150),
    ("JAF-005", "the classic", "jaffle", 900),
    ("JAF-006", "the vegan", "jaffle", 1300),
    ("BEV-001", "tangaroo", "beverage", 500),
    ("BEV-002", "orange you glad", "beverage", 450),
    ("BEV-003", "watermelon", "beverage", 480),
    ("BEV-004", "sparkling water", "beverage", 300),
]


def customers_for(orders):
    return max(10, orders // 10)


def build_orders(spark, first, count, customers):
    """Deterministic orders and items for order numbers [first, first + count)."""
    from pyspark.sql import functions as F

    products = spark.createDataFrame(
        [(index, sku, price) for index, (sku, _, _, price) in enumerate(PRODUCTS)],
        "idx int, sku string, price int",
    )
    numbers = spark.range(first, first + count).toDF("n")
    items = (
        numbers.withColumn("k", F.pmod(F.xxhash64("n"), F.lit(4)) + 1)
        .select("n", F.explode(F.sequence(F.lit(1), F.col("k"))).alias("j"))
        .withColumn("idx", F.pmod(F.xxhash64("n", "j"), F.lit(len(PRODUCTS))).cast("int"))
        .join(products, "idx")
        .select(
            F.format_string("ITM-%010d-%d", "n", "j").alias("id"),
            F.format_string("ORD-%010d", "n").alias("order_id"),
            "sku",
            "price",
            "n",
        )
    )
    totals = items.groupBy("n").agg(F.sum("price").cast("int").alias("subtotal"))
    orders = numbers.join(totals, "n").select(
        F.format_string("ORD-%010d", "n").alias("id"),
        F.format_string("CUST-%08d", F.pmod(F.xxhash64("n", F.lit("customer")), F.lit(customers))).alias("customer"),
        F.timestamp_seconds(F.lit(EPOCH_2024) + F.pmod(F.col("n") * 7919, F.lit(31536000))).alias("ordered_at"),
        F.format_string("STORE-%02d", F.pmod("n", F.lit(STORES)) + 1).alias("store_id"),
        "subtotal",
        F.round(F.col("subtotal") * 0.06).cast("int").alias("tax_paid"),
    )
    orders = orders.withColumn("order_total", F.col("subtotal") + F.col("tax_paid"))
    return orders, items.select("id", "order_id", "sku")


def static_sources(spark):
    """Products, stores and supplies: small fixed reference tables with the jaffle-shop column names."""
    products = spark.createDataFrame(
        [(sku, name, kind, price, f"{name} ({kind})") for sku, name, kind, price in PRODUCTS],
        "sku string, name string, type string, price int, description string",
    )
    stores = spark.createDataFrame(
        [(f"STORE-{i:02d}", f"Store {i}", "2016-09-01 00:00:00", 0.06 + 0.005 * i) for i in range(1, STORES + 1)],
        "id string, name string, opened_at string, tax_rate double",
    )
    stores = stores.selectExpr("id", "name", "cast(opened_at as timestamp) as opened_at", "tax_rate")
    supplies = spark.createDataFrame(
        [
            (f"SUP-{index * 3 + k + 1:03d}", f"supply {k}", 40 + 13 * k + index, k == 0, sku)
            for index, (sku, _, _, _) in enumerate(PRODUCTS)
            for k in range(3)
        ],
        "id string, name string, cost int, perishable boolean, sku string",
    )
    return products, stores, supplies


def write_raw(spark, catalog, schema, sf, mode):
    orders_count = max(1, int(ORDERS_PER_SF * sf))
    if mode == "append":
        orders_count = max(1, orders_count // 100)
        first = int(ORDERS_PER_SF * sf)
        customers = customers_for(int(ORDERS_PER_SF * sf))
    else:
        first = 0
        customers = customers_for(orders_count)
    orders, items = build_orders(spark, first, orders_count, customers)
    table = f"{catalog}.{schema}"
    if mode == "append":
        orders.write.mode("append").saveAsTable(f"{table}.raw_orders")
        items.write.mode("append").saveAsTable(f"{table}.raw_items")
        return {"appended_orders": orders_count}
    customer_rows = spark.range(0, customers).selectExpr(
        "format_string('CUST-%08d', id) as id", "concat('Customer ', id) as name"
    )
    products, stores, supplies = static_sources(spark)
    for name, frame in {
        "raw_customers": customer_rows,
        "raw_orders": orders,
        "raw_items": items,
        "raw_products": products,
        "raw_stores": stores,
        "raw_supplies": supplies,
    }.items():
        frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{table}.{name}")
    return {"orders": orders_count, "customers": customers}


def reset_arm_schema(spark, catalog, schema):
    """Drops every table and view in the arm schema, including reuse state; the volume is kept."""
    rows = spark.sql(
        f"SELECT table_name, table_type FROM {catalog}.information_schema.tables WHERE table_schema = '{schema}'"
    ).collect()
    for row in rows:
        kind = "VIEW" if row.table_type == "VIEW" else "TABLE"
        spark.sql(f"DROP {kind} IF EXISTS `{catalog}`.`{schema}`.`{row.table_name}`")
    return len(rows)


def capture(ws, volume_root, variant, target, deployment_ref, project_dir, profiles_dir):
    from databricks_dbt_factory.reuse_prepare import capture_parse

    # dbt parse never connects, so the profile's token reference only needs a value to render.
    os.environ["DBT_ACCESS_TOKEN"] = "parse-only"
    identity = {
        "project_id": "jaffle_shop_bench",
        "environment": "bench",
        "workspace_id": str(ws.get_workspace_id()),
        "metastore_id": ws.metastores.summary().metastore_id,
        "principal": ws.current_user.me().user_name,
    }
    manifest, provenance = capture_parse(
        project_dir, profiles_dir, identity=identity, target=target, deployment_ref=deployment_ref
    )
    directory = f"{volume_root}/capture/{variant}"
    ws.files.create_directory(directory)
    ws.files.upload(directory + "/manifest.json", io.BytesIO(manifest), overwrite=True)
    ws.files.upload(directory + "/provenance.json", io.BytesIO(provenance), overwrite=True)
    return {"manifest": directory + "/manifest.json", "provenance": directory + "/provenance.json"}


def main(dbutils, spark):
    from databricks.sdk import WorkspaceClient
    from databricks_dbt_factory import reuse_runtime as runtime
    from databricks_dbt_factory.reuse import ReuseConfig

    names = [
        "action", "sf", "arm", "arm_schema", "variant", "target", "deployment_ref", "reuse_config",
        "volume_root", "catalog", "raw_schema", "project_directory", "profiles_directory",
    ]  # fmt: skip
    for name in names:
        dbutils.widgets.text(name, "")
    value = {name: dbutils.widgets.get(name) for name in names}
    result = {"action": value["action"]}
    if value["action"] == "reset":
        result["raw"] = write_raw(spark, value["catalog"], value["raw_schema"], float(value["sf"]), "reset")
        result["dropped"] = reset_arm_schema(spark, value["catalog"], value["arm_schema"])
        if value["arm"] == "on":
            runtime.initialize_state(spark, ReuseConfig.from_dict(json.loads(value["reuse_config"])))
    elif value["action"] == "append":
        result["raw"] = write_raw(spark, value["catalog"], value["raw_schema"], float(value["sf"]), "append")
    elif value["action"] == "capture":
        result.update(
            capture(
                WorkspaceClient(),
                value["volume_root"],
                value["variant"],
                value["target"],
                value["deployment_ref"],
                value["project_directory"],
                value["profiles_directory"],
            )
        )
    else:
        raise ValueError("action must be reset, append or capture")
    dbutils.notebook.exit(json.dumps(result))


if __name__ == "__main__":
    from databricks.sdk.runtime import dbutils, spark

    main(dbutils, spark)
