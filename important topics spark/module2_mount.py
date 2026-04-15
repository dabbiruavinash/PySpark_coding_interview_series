
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EcommerceMount").getOrCreate()

# Example: Mount Azure Blob Storage
container_name = "ecommerce-data"
storage_account_name = "yourstorageaccount"
access_key = "youraccesskey"
mount_point = f"/mnt/{container_name}"

# Check if already mounted
if not any(mount.mountPoint == mount_point for mount in spark.conf.get("fs.azure.mounts")):
    try:
        # Configure access
        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
            access_key
        )
        
        # Mount
        dbutils.fs.mount(
            source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
            mount_point = mount_point,
            extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
        )
        print(f"Mounted successfully at {mount_point}")
    except Exception as e:
        print(f"Mount failed: {e}")
else:
    print(f"Already mounted at {mount_point}")

# List files in mounted directory
display(dbutils.fs.ls(mount_point))

# Read a file from mount
df = spark.read.format("parquet").load(f"{mount_point}/orders/")
df.show(5)