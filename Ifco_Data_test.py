# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/data_test/orders__1_.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "orders__1__csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

### Test 1: Distribution of Crate Type per Company

from pyspark.sql.functions import col, count

# Load the data into a DataFrame
file_path = "/FileStore/tables/data_test"
orders_df = spark.read.option("header", "true").option("delimiter", ";").csv(file_path)

# Select relevant columns and calculate the distribution of crate types per company,  and sort by company name,cratetype
crate_distribution = (
    orders_df.groupBy("company_name", "crate_type")
    .agg(count("*").alias("order_count"))
    .orderBy("company_name", "crate_type")
)

# Show the results
crate_distribution.display()



# COMMAND ----------

import pandas as pd
import unittest

# Sample data with order_id
data = {
    "company_id": [
        "1e2b47e6-499e-41c6-91d3-09d12dddfbbd", 
        "0f05a8f1-2bdf-4be7-8c82-4c9b58f04898", 
        "1e2b47e6-499e-41c6-91d3-09d12dddfbbd", 
        "1c4b0b50-1d5d-463a-b56e-1a6fd3aeb7d6", 
        "34538e39-cd2e-4641-8d24-3c94146e6f16"
    ],
    "company_name": [
        "Fresh Fruits Co", "Fresh Fruits Co",
        "Veggies Inc", "Meat Packers Ltd", "Seafood Supplier"
        
    ],
     "crate_type": [
                "Plastic", "Wood", "Wood", "Plastic", "Metal"
     ],
     "order_count": [2, 1, 1, 2, 1]
   
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Calculate the distribution of crate types per company
distribution_df = df.groupby(["company_id", "company_name", "crate_type"]).size().reset_index(name='count')

# Unit Tests using unittest
class TestCrateTypeDistribution(unittest.TestCase):

    def setUp(self):
        # Create DataFrame again for testing purposes
        self.df = pd.DataFrame(data)

    def test_crate_type_distribution(self):
        # Expected distribution result
        expected_data = {
            "company_id": [
                "1e2b47e6-499e-41c6-91d3-09d12dddfbbd", 
                "1e2b47e6-499e-41c6-91d3-09d12dddfbbd", 
                "0f05a8f1-2bdf-4be7-8c82-4c9b58f04898", 
                "34538e39-cd2e-4641-8d24-3c94146e6f16", 
                "1c4b0b50-1d5d-463a-b56e-1a6fd3aeb7d6"
            ],
            "company_name": [
                "Fresh Fruits Co", "Fresh Fruits Co", 
                "Veggies Inc", "Meat Packers Ltd", "Seafood Supplier"
            ],
            "crate_type": [
                "Plastic", "Wood", "Wood", "Plastic", "Metal"
            ],
            "order_count": [2, 1, 1, 2, 1]
        }
        
        expected_df = pd.DataFrame(expected_data)

        # Perform the calculation
        actual_df = self.df.groupby(["company_id", "company_name", "crate_type"]).size().reset_index(name='count')

        # Sort both DataFrames by 'company_id', 'company_name', and 'crate_type' to ensure the order is the same
        expected_df_sorted = expected_df.sort_values(by=["company_id", "company_name", "crate_type"]).reset_index(drop=True)
        actual_df_sorted = actual_df.sort_values(by=["company_id", "company_name", "crate_type"]).reset_index(drop=True)

        # Assert the actual result matches the expected result
        pd.testing.assert_frame_equal(expected_df_sorted, actual_df_sorted)

if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)


# COMMAND ----------

###Test 2: DataFrame of Orders with Full Name of the Contact

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, concat_ws, udf,col
from pyspark.sql.types import StringType, StructType, StructField

# Initialize Spark session
spark = SparkSession.builder.appName("FixContactData").getOrCreate()

# Contact data 
data = [
    ('f47ac10b-58cc-4372-a567-0e02b2c3d479', '''[{ """"contact_name"""":""""Curtis"""", """"contact_surname"""":""""Jackson"""", """"city"""":""""Chicago"""", """"cp"""": """"12345""""}]'''),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d480', '''[{ """"contact_name"""":""""Maria"""", """"contact_surname"""":""""Theresa"""", """"city"""":""""Calcutta""""}]'''),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d481', '''[{ """"contact_name"""":""""Para"""", """"contact_surname"""":""""Cetamol"""", """"city"""":""""Frankfurt am Oder"""", """"cp"""": 3934}]'''),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d482', None),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d483', None),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d484', '''[{ """"contact_name"""":""""John"""", """"contact_surname"""":""""Krasinski"""", """"city"""":""""New York"""", """"cp"""": """"1203""""}]'''),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d485',None),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d486', '''[(""""contact_name"""":""""Jennifer"""", """"contact_surname"""":""""Lopez"""", """"city"""":""""Esplugues de Llobregat""""}]'''),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d487','''[{""""contact_name"""":""""Liav"""", """"contact_surname"""": """"Ichenbaum"""", """"city"""":""""Tel Aviv""""}]''')
]

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("contact_data", StringType(), True)
])

# Create DataFrame
orders_df = spark.createDataFrame(data, schema)

# Define a UDF to clean and parse the JSON string
def clean_parse_json(json_str):
    if not json_str:
        return None, None
    try:
        # Remove extra double quotes and brackets
        clean_str = json_str.replace('""""', '"').replace('[{', '{').replace('}]', '}')
        # Parse JSON
        data = json.loads(clean_str)
        if isinstance(data, list):
            data = data[0]
        return data.get('contact_name', None), data.get('contact_surname', None)
    except:
        return None, None

# Register the UDF
clean_parse_json_udf = udf(clean_parse_json, StructType([StructField("contact_name", StringType()), StructField("contact_surname", StringType())]))

# Apply the UDF to the DataFrame(cleaned data)
orders_df = orders_df.withColumn("parsed_contact_data", clean_parse_json_udf("contact_data"))

# Extract contact_name and contact_surname from the parsed JSON
df_1 = (
    orders_df
    .withColumn("contact_name", col("parsed_contact_data.contact_name"))
    .withColumn("contact_surname", col("parsed_contact_data.contact_surname"))
    .withColumn(
        "contact_full_name",
        when(
            col("contact_name").isNull() & col("contact_surname").isNull(),  # If both are null
            lit("John Doe")
        ).otherwise(
            concat_ws(" ", col("contact_name"), col("contact_surname"))  # Combine non-null values
        )
    )
    .select("order_id", "contact_full_name")  # Keep only required columns
)

# Show the resulting DataFrame
df_1.display(truncate=False)


# COMMAND ----------

### UNIT TEST FOR CONTACT FULLNAME
import pandas as pd
import unittest

# Sample DataFrame creation
data = {
    'order_id': ['f47ac10b-58cc-4372-a567-0e02b2c3d479', 'f47ac10b-58cc-4372-a567-0e02b2c3d480'],
    'contact_first_name': ['Leonard', 'Luke'],
    'contact_last_name': ['Cohen', 'Skywalker'],
}

df = pd.DataFrame(data)

# Add full name column
df['contact_full_name'] = df.apply(lambda row: f"{row['contact_first_name']} {row['contact_last_name']}" if pd.notna(row['contact_first_name']) and pd.notna(row['contact_last_name']) else 'John Doe', axis=1)

# Create the required DataFrame
df_1 = df[['order_id', 'contact_full_name']]

# Unit test class
class TestOrderDataFrame(unittest.TestCase):
    
    def test_full_name(self):
        # Check the first entry
        self.assertEqual(df_1.loc[0, 'contact_full_name'], 'Leonard Cohen')
        
        # Check the second entry
        self.assertEqual(df_1.loc[1, 'contact_full_name'], 'Luke Skywalker')
    
    def test_missing_name(self):
        # Create a DataFrame with missing name
        data_missing = {
            'order_id': ['f47ac10b-58cc-4372-a567-0e02b2c3d481'],
            'contact_first_name': [None],
            'contact_last_name': [None],
        }
        df_missing = pd.DataFrame(data_missing)
        
        # Apply the full name logic
        df_missing['contact_full_name'] = df_missing.apply(lambda row: f"{row['contact_first_name']} {row['contact_last_name']}" if pd.notna(row['contact_first_name']) and pd.notna(row['contact_last_name']) else 'John Doe', axis=1)
        
        # Verify the missing name is replaced by 'John Doe'
        self.assertEqual(df_missing.loc[0, 'contact_full_name'], 'John Doe')

# Run the tests
if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)


# COMMAND ----------

### Test 3: DataFrame of Orders with Contact Address
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, lit, concat_ws

# Initialize Spark session
spark = SparkSession.builder.appName("ContactAddress").getOrCreate()

# Sample data
data = [
    {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "contact_data": """[{ "contact_name":"Curtis", "contact_surname":"Jackson", "city":"Chicago", "cp":"12345"}]"""},
    {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d480", "contact_data": """[{ "contact_name":"Maria", "contact_surname":"Theresa", "city":"Calcutta"}]"""},
    {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d481", "contact_data": """[{ "contact_name":"Para", "contact_surname":"Cetamol", "city":"Frankfurt am Oder", "cp":3934}]"""},
    {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d482", "contact_data": None},
    {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d483", "contact_data": """[{ "contact_name":"Bruce", "contact_surname":"Wayne", "city":"Gotham"}]"""},
    {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d484", "contact_data": """[{ "contact_name":"Tony", "contact_surname":"Stark", "city":"New York", "cp":"10001"}]"""},
]

# Create a DataFrame
orders_df = spark.createDataFrame(data)

# Process the contact_data column to extract city and postal code (cp)
df_2 = (
    orders_df
    .withColumn(
        "city",
        regexp_extract(col("contact_data"), r'"city":"([^"]+)"', 1)  # Extract city
    )
    .withColumn(
        "postal_code",
        regexp_extract(col("contact_data"), r'"cp":"?([^",]+)"?', 1)  # Extract postal code
    )
    .withColumn(
        "city",
        when(col("city").isNull() | (col("city") == ""), lit("Unknown")).otherwise(col("city"))  # Handle missing city
    )
    .withColumn(
        "postal_code",
        when(col("postal_code").isNull() | (col("postal_code") == ""), lit("UNK00")).otherwise(col("postal_code"))  # Handle missing postal code
    )
    .withColumn(
        "contact_address",
        concat_ws(", ", col("city"), col("postal_code"))  # Combine city and postal code
    )
    .select("order_id", "contact_address")  # Select required columns
)

# Show the result
df_2.display(truncate= False)


# COMMAND ----------

## Unit Test For Contact Address

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, lit, concat_ws

# Initialize Spark session
spark = SparkSession.builder.appName("ManualTest").getOrCreate()

# Define the test function
def test_contact_address():
    # Sample test data
    test_data = [
        {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "contact_data": """[{ "city":"Chicago", "cp":"12345"}]"""},
        {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d480", "contact_data": """[{ "city":"Calcutta"}]"""},
        {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d481", "contact_data": None},
        {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d482", "contact_data": """[{ "city":"Gotham"}]"""},
        {"order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d483", "contact_data":  """[{ "city":"New York", "cp":"10001"}]"""}, 
    ]

    # Expected results
    expected_data = [
    {"contact_address": "Chicago, 12345", "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479"},
    {"contact_address": "Calcutta, UNK00", "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d480"},
    {"contact_address": "Unknown, UNK00", "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d481"},
    {"contact_address": "Gotham, UNK00", "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d482"},
    {"contact_address": "New York, 10001", "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d483"}
]

    # Create DataFrames
    test_df = spark.createDataFrame(test_data)
    expected_df = spark.createDataFrame(expected_data)

    # Apply transformation logic
    result_df = (
        test_df
        .withColumn(
            "city",
            regexp_extract(col("contact_data"), r'"city":"([^"]+)"', 1)
        )
        .withColumn(
            "postal_code",
            regexp_extract(col("contact_data"), r'"cp":"?([^",]+)"?', 1)
        )
        .withColumn(
            "city",
            when(col("city").isNull() | (col("city") == ""), lit("Unknown")).otherwise(col("city"))
        )
        .withColumn(
            "postal_code",
            when(col("postal_code").isNull() | (col("postal_code") == ""), lit("UNK00")).otherwise(col("postal_code"))
        )
        .withColumn(
            "contact_address",
            concat_ws(", ", col("city"), col("postal_code"))
        )
        .select("order_id", "contact_address")
    )

    # Compare results
    actual = [row.asDict() for row in test_df.collect()]
    expected = expected_df.collect()
    assert sorted(actual, key=lambda x: x['order_id']) == sorted(expected, key=lambda x: x['order_id'])
    assert actual == expected, f"Test failed! Expected: {expected}, but got: {actual}"


# Call the test function directly
try:
    test_contact_address()
    print("All tests passed!")
except AssertionError as e:
    print(str(e))


# COMMAND ----------

### Test 4: Calculation of Sales Team Commissions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, when, lit, round, expr, sum as _sum

# Initialize Spark session
spark = SparkSession.builder.appName("SalesCommissions").getOrCreate()

# Provided invoice data (in cents for grossValue)
invoice_data = [
    {"orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "grossValue": 324222, "vat": 0, "salesowners": "Leonard Cohen, Luke Skywalker, Ammy Winehouse"},
    {"orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d480", "grossValue": 193498, "vat": 19, "salesowners": "Luke Skywalker, David Goliat, Leon Leonov"},
    {"orderId":  "f47ac10b-58cc-4372-a567-0e02b2c3d481","grossValue": 345498, "vat": 21, "salesowners": "Luke Skywalker"},
    {"orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d482", "grossValue": 245412, "vat": 34, "salesowners": "David Goliat, Leonard Cohen"},
    {"orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d483", "grossValue": 145467, "vat": 0, "salesowners": "Chris Pratt, David Henderson, Marianov Merschik, Leon Leonov"},
    {"orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d484", "grossValue": 223344, "vat": 19, "salesowners": "Leonard Cohen, David Henderson"},
]

# Create a DataFrame
invoices_df = spark.createDataFrame(invoice_data)

# Calculate net_value (convert grossValue from cents to euros and exclude VAT)
invoices_df = invoices_df.withColumn(
    "net_value",
    round((col("grossValue") / 100) / (1 + col("vat") / 100), 2)  # Convert cents to euros and remove VAT
)

# Explode salesowners into individual rows and assign ranks
exploded_df = (
    invoices_df
    .withColumn("salesowner", explode(split(col("salesowners"), ", ")))  # Split and explode salesowners
    .withColumn("rank", expr("row_number() over (partition by orderId order by salesowners)"))  # Assign rank
)

# Calculate commissions based on rank
commissions_df = (
    exploded_df
    .withColumn(
        "commission_rate",
        when(col("rank") == 1, lit(0.06))  # Main Owner: 6%
        .when(col("rank") == 2, lit(0.025))  # Co-owner 1: 2.5%
        .when(col("rank") == 3, lit(0.0095))  # Co-owner 2: 0.95%
        .otherwise(lit(0))  # Others: 0%
    )
    .withColumn(
        "commission",
        round(col("net_value") * col("commission_rate"), 2)  # Calculate commission in euros
    )
)

# Aggregate total commission per salesowner
total_commissions_df = (
    commissions_df
    .groupBy("salesowner")
    .agg(round(_sum("commission"), 2).alias("total_commission"))  # Sum and round total commissions
    .orderBy(col("total_commission").desc())  # Sort by descending total commission
)

# Show results
total_commissions_df.display(truncate=False)



# COMMAND ----------

## UNIT TEST FOR CALCULATE_COMMISIONS_PYSPARK
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, explode, row_number, lit
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.master("local[*]").appName("CommissionCalculation").getOrCreate()

def calculate_commissions_pyspark(orders_df):
    """
    Calculate commissions for salesowners using PySpark DataFrame operations.
    
    Parameters:
        orders_df (DataFrame): A PySpark DataFrame containing `order_id`, `net_invoiced_value_cents`, and `salesowners`.
    
    Returns:
        DataFrame: A DataFrame with salesowners and their total commissions, sorted in descending order.
    """
    # Convert net invoiced value from cents to euros
    orders_df = orders_df.withColumn("net_invoiced_value_euros", col("net_invoiced_value_cents") / 100)

    # Explode salesowners list into individual rows and assign ranks
    exploded_df = orders_df.withColumn("salesowner", explode(col("salesowners"))) \
                           .withColumn("rank", row_number().over(Window.partitionBy("order_id").orderBy(lit(1))))

    # Define commission rates for ranks
    commission_rates = {1: 0.06, 2: 0.025, 3: 0.0095}
    
    # Add commission rate based on rank
    exploded_df = exploded_df.withColumn(
        "commission_rate",
        expr(f"CASE rank WHEN 1 THEN {commission_rates[1]} WHEN 2 THEN {commission_rates[2]} WHEN 3 THEN {commission_rates[3]} ELSE 0 END")
    )
    
    # Calculate commission for each salesowner
    exploded_df = exploded_df.withColumn(
        "commission_euros",
        col("net_invoiced_value_euros") * col("commission_rate")
    )

    # Aggregate total commissions per salesowner
    commission_df = exploded_df.groupBy("salesowner") \
                               .agg(expr("ROUND(SUM(commission_euros), 2)").alias("total_commission")) \
                               .orderBy(col("total_commission").desc())

    return commission_df

# Example data for testing
data = [
    ("f47ac10b-58cc-4372-a567-0e02b2c3d479", 324222, ["Leonard Cohen", "Luke Skywalker", "Ammy Winehouse"]),
    ("f47ac10b-58cc-4372-a567-0e02b2c3d480", 193498, ["Luke Skywalker", "David Goliat", "Leon Leonov"]),
    ("f47ac10b-58cc-4372-a567-0e02b2c3d481", 345498, ["Chris Pratt", "David Henderson", "Marianov Merschik", "Leon Leonov"]),
]

schema = ["order_id", "net_invoiced_value_cents", "salesowners"]
orders_df = spark.createDataFrame(data, schema)

# Calculate commissions
commission_df = calculate_commissions_pyspark(orders_df)
commission_df.show()

import unittest

class TestCalculateCommissions(unittest.TestCase):
    def test_commission_calculation(self):
        # Expected results
        expected_results = [
            ("Chris Pratt", 20729.88),
            ("Leonard Cohen", 19453.32),
            ("Luke Skywalker", 19715.43),
            ("David Henderson", 8637.45),
            ("David Goliat", 4837.45),
            ("Marianov Merschik", 3282.23),
            ("Ammy Winehouse", 3080.11),
            ("Leon Leonov", 1838.23),
        ]

        # Run the function
        result_df = calculate_commissions_pyspark(orders_df)

        # Collect results
        result_list = [(row.salesowner, row.total_commission) for row in result_df.collect()]

        # Assert equality
        self.assertEqual(result_list, expected_results)

if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)


# COMMAND ----------

### Test 5: DataFrame of Companies with Sales Owners
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, udf, array_distinct, sort_array,collect_set
from pyspark.sql.types import ArrayType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CompanySalesOwners").getOrCreate()

# Sample data
data = [
    ('f47ac10b-58cc-4372-a567-0e02b2c3d479', '1e2b47e6-499e-41c6-91d3-09d12dddfbbd', 'Fresh Fruits Co', 'Leonard Cohen, Luke Skywalker, Ammy Winehouse'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d480', '0f05a8f1-2bdf-4be7-8c82-4c9b58f04898', 'Veggies Inc', 'Luke Skywalker, David Goliat, Leon Leonov'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d481', '1e2b47e6-499e-41c6-91d3-09d12dddfbbd', 'Fresh Fruits c.o', 'Luke Skywalker'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d482', '1c4b0b50-1d5d-463a-b56e-1a6fd3aeb7d6', 'Seafood Supplier', 'David Goliat, Leonard Cohen'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d483', '34538e39-cd2e-4641-8d24-3c94146e6f16', 'Meat Packers Ltd', 'Chris Pratt, David Henderson, Marianov Merschik, Leon Leonov'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d484', 'fa14c3ed-3c48-49f4-bd69-4d7f5b5f4b1b', 'Green Veg Co', 'Leonard Cohen, David Henderson'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d485', '2e90f2b1-d237-47a6-96e8-6d01c0d78c3e', 'Seafood Supplier GmbH', 'Markus Söder, Ammy Winehouse'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d486', 'acdb6f30-764f-404e-8b8e-7e7e3e6fa1a9', 'Organic Farms', 'David Henderson, Leonard Cohen, Leon Leonov'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d487', '5f0bdbdf-1d84-4c23-957c-8bb8c0ddc89d', 'Tropical Fruits Ltd', 'Yuri Gagarin, David Goliat, David Henderson')
]

# Create DataFrame
columns = ['order_id', 'company_id', 'company_name', 'salesowners']
df = spark.createDataFrame(data, columns)

# Split the salesowners string into a list
df = df.withColumn('salesowners_list', split(col('salesowners'), ', '))

# Remove duplicates and sort the salesowners list
df = df.withColumn('salesowners_list', array_distinct(col('salesowners_list')))
df = df.withColumn('salesowners_list', sort_array(col('salesowners_list')))

# Group by company_id and company_name and aggregate the list of salesowners
df_3 = df.groupBy('company_id', 'company_name').agg({'salesowners_list': 'first'})

# Show the final DataFrame
df_3.display(truncate=False)




# COMMAND ----------

### Test 6: Data Visualization 
## Distribtion of orders by crate type

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Data creation
data = {
    'order_id': [
        'f47ac10b-58cc-4372-a567-0e02b2c3d479', 'f47ac10b-58cc-4372-a567-0e02b2c3d480', 
        'f47ac10b-58cc-4372-a567-0e02b2c3d481', 'f47ac10b-58cc-4372-a567-0e02b2c3d482', 
        'f47ac10b-58cc-4372-a567-0e02b2c3d483', 'f47ac10b-58cc-4372-a567-0e02b2c3d484', 
        'f47ac10b-58cc-4372-a567-0e02b2c3d485', 'f47ac10b-58cc-4372-a567-0e02b2c3d486', 
        'f47ac10b-58cc-4372-a567-0e02b2c3d487', 'f47ac10b-58cc-4372-a567-0e02b2c3d488'
    ],
    'date': [
        '29.01.22', '21.02.22', '03.04.22', '14.07.21', '23.10.22', 
        '15.05.22', '31.12.22', '01.04.22', '04.09.22', '07.11.22'
    ],
    'crate_type': [
        'Plastic', 'Wood', 'Metal', 'Plastic', 'Plastic', 
        'Wood', 'Metal', 'Metal', 'Plastic', 'Plastic'
    ],
    'salesowners': [
        'Leonard Cohen, Luke Skywalker, Ammy Winehouse', 
        'Luke Skywalker, David Goliat, Leon Leonov', 
        'Luke Skywalker', 
        'David Goliat, Leonard Cohen', 
        'Chris Pratt, David Henderson, Marianov Merschik, Leon Leonov', 
        'Leonard Cohen, David Henderson', 
        'Markus Söder, Ammy Winehouse', 
        'David Henderson, Leonard Cohen, Leon Leonov', 
        'Yuri Gagarin, David Goliat, David Henderson', 
        'Ammy Winehouse, Marie Curie, Chris Pratt'
    ]
}

# Create DataFrame
df = pd.DataFrame(data)

# Convert 'date' column to datetime for analysis
df['date'] = pd.to_datetime(df['date'], format='%d.%m.%y')

# Expand salesowners into separate rows
df = df.assign(salesowners=df['salesowners'].str.split(', ')).explode('salesowners')

# Filter data for the last 12 months (relative to max date in dataset)
last_date = df['date'].max()
start_date = last_date - pd.DateOffset(months=12)
df_last_12_months = df[(df['date'] >= start_date) & (df['date'] <= last_date)]

# --- Visualization 1: Distribution of Orders by Crate Type ---
plt.figure(figsize=(8, 6))
sns.countplot(data=df, x='crate_type', palette='viridis', order=df['crate_type'].value_counts().index)
plt.title('Distribution of Orders by Crate Type')
plt.xlabel('Crate Type')
plt.ylabel('Order Count')
plt.xticks(rotation=45)
plt.show()




# COMMAND ----------


### Analysing Sales-owners improvement, based on the last 12 months orders

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Data creation
data = {
    'order_id': [
        'f47ac10b-58cc-4372-a567-0e02b2c3d479', 'f47ac10b-58cc-4372-a567-0e02b2c3d480', 
        'f47ac10b-58cc-4372-a567-0e02b2c3d481', 'f47ac10b-58cc-4372-a567-0e02b2c3d482', 
        'f47ac10b-58cc-4372-a567-0e02b2c3d483', 'f47ac10b-58cc-4372-a567-0e02b2c3d484', 
        'f47ac10b-58cc-4372-a567-0e02b2c3d485', 'f47ac10b-58cc-4372-a567-0e02b2c3d486', 
        'f47ac10b-58cc-4372-a567-0e02b2c3d487', 'f47ac10b-58cc-4372-a567-0e02b2c3d488'
    ],
    'date': [
        '29.01.22', '21.02.22', '03.04.22', '14.07.21', '23.10.22', 
        '15.05.22', '31.12.22', '01.04.22', '04.09.22', '07.11.22'
    ],
    'crate_type': [
        'Plastic', 'Wood', 'Metal', 'Plastic', 'Plastic', 
        'Wood', 'Metal', 'Metal', 'Plastic', 'Plastic'
    ],
    'salesowners': [
        'Leonard Cohen, Luke Skywalker, Ammy Winehouse', 
        'Luke Skywalker, David Goliat, Leon Leonov', 
        'Luke Skywalker', 
        'David Goliat, Leonard Cohen', 
        'Chris Pratt, David Henderson, Marianov Merschik, Leon Leonov', 
        'Leonard Cohen, David Henderson', 
        'Markus Söder, Ammy Winehouse', 
        'David Henderson, Leonard Cohen, Leon Leonov', 
        'Yuri Gagarin, David Goliat, David Henderson', 
        'Ammy Winehouse, Marie Curie, Chris Pratt'
    ]
}

# Create DataFrame
df = pd.DataFrame(data)

# Convert 'date' column to datetime for analysis
df['date'] = pd.to_datetime(df['date'], format='%d.%m.%y')
# --- Visualization 2: Sales Owners Needing Training on Plastic Crates ---
# Filter for Plastic Crates
plastic_crates = df_last_12_months[df_last_12_months['crate_type'] == 'Plastic']

# Count orders per salesowner
salesowner_counts = plastic_crates['salesowners'].value_counts()

# Plot salesowners with the least plastic crate orders
salesowner_counts.tail(10).plot(kind='barh', figsize=(10, 6), color='orange')
plt.title('Sales Owners Needing Training on Plastic Crates (Last 12 Months)')
plt.xlabel('Order Count')
plt.ylabel('Sales Owner')
plt.show()

# COMMAND ----------

### Top 5 performers selling plastic crates for a rolling 3 months evaluation window

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Sample DataFrame setup
data = {
    'order_id': [
        'f47ac10b-58cc-4372-a567-0e02b2c3d479', 'f47ac10b-58cc-4372-a567-0e02b2c3d480',
        'f47ac10b-58cc-4372-a567-0e02b2c3d481', 'f47ac10b-58cc-4372-a567-0e02b2c3d482',
        'f47ac10b-58cc-4372-a567-0e02b2c3d483', 'f47ac10b-58cc-4372-a567-0e02b2c3d484',
        'f47ac10b-58cc-4372-a567-0e02b2c3d485', 'f47ac10b-58cc-4372-a567-0e02b2c3d486',
        'f47ac10b-58cc-4372-a567-0e02b2c3d487', 'f47ac10b-58cc-4372-a567-0e02b2c3d488'
    ],
    'date': [
        '2022-01-29', '2022-02-21', '2022-04-03', '2021-07-14', '2022-10-23',
        '2022-05-15', '2022-12-31', '2022-04-01', '2022-09-04', '2022-11-07'
    ],
    'crate_type': [
        'Plastic', 'Wood', 'Metal', 'Plastic', 'Plastic',
        'Wood', 'Metal', 'Metal', 'Plastic', 'Plastic'
    ],
    'salesowners': [
        'Leonard Cohen, Luke Skywalker, Ammy Winehouse', 'Luke Skywalker, David Goliat, Leon Leonov',
        'Luke Skywalker', 'David Goliat, Leonard Cohen', 'Chris Pratt, David Henderson, Marianov Merschik, Leon Leonov',
        'Leonard Cohen, David Henderson', 'Markus Söder, Ammy Winehouse',
        'David Henderson, Leonard Cohen, Leon Leonov', 'Yuri Gagarin, David Goliat, David Henderson',
        'Ammy Winehouse, Marie Curie, Chris Pratt'
    ]
}
df = pd.DataFrame(data)

# Convert 'date' column to datetime
df['date'] = pd.to_datetime(df['date'])

# Add a 'month' column for grouping (as string or categorical)
df['month'] = df['date'].dt.to_period('M').astype(str)

# Split salesowners into individual rows
df = df.assign(salesowners=df['salesowners'].str.split(', ')).explode('salesowners')

# Filter for plastic crates
plastic_df = df[df['crate_type'] == 'Plastic']

# Group by 'month' and 'salesowners' and count orders
plastic_by_month = (
    plastic_df.groupby(['month', 'salesowners'])
    .size()
    .reset_index(name='order_count')
)

# Compute rolling 3-month total for each salesowner
plastic_by_month['rolling_3_month'] = (
    plastic_by_month.groupby('salesowners')['order_count']
    .transform(lambda x: x.rolling(3, min_periods=1).sum())
)

# Filter top 5 performers per month
top_5_per_month = (
    plastic_by_month.sort_values(['month', 'rolling_3_month'], ascending=[True, False])
    .groupby('month')
    .head(5)
)

# Ensure rolling_3_month is numeric for plotting
top_5_per_month['rolling_3_month'] = pd.to_numeric(top_5_per_month['rolling_3_month'])

# Plot results
plt.figure(figsize=(12, 6))
sns.lineplot(
    data=top_5_per_month,
    x='month', 
    y='rolling_3_month', 
    hue='salesowners', 
    marker='o'
)
plt.title('Top 5 Performers Selling Plastic Crates (Rolling 3-Month Evaluation)')
plt.xlabel('Month')
plt.ylabel('Rolling 3-Month Order Count')
plt.xticks(rotation=45)
plt.legend(title='Sales Owner', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
