import pytest
from pyspark.sql.types import StructType, StructField, StringType
from src.glue.scripts.transform_customer_table import lowercase_column_values, filter_column_from_dynamic_frame
from pyspark.context import SparkContext
from awsglue.context import GlueContext 
from awsglue.dynamicframe import DynamicFrame

@pytest.fixture(scope="session") #The fixture is created and finalized once for the entire test session.
# This means that the test fixture instance is shared across all test modules and test classes within the session.
def spark_session():
  sc = SparkContext()
  sc.setLogLevel("ERROR")
  glueContext = GlueContext(sc)
  spark = glueContext.spark_session
  return spark, glueContext

@pytest.fixture
def sample_df_dataset(sparkSession)
  # Define the schema for the DataFrame
  schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", StringType(), True)
  ])

  # Create a sample DataFrame
  data = [
    ("John", "Doe", "male", 30),
    ("Jane", "Smith", "female", 25),
    ("Bob", "Johnson", "male", 28),
    ("Sarah", "Williams", "Ffemale", 32)
  ]
  sparkSession, glueContext = spark_session
  # Create the DataFrame
  df = sparkSession.createDataFrame(data, schema=schema)
  return df

def test_lowercase_column_values(sample_df_dataset):
  # Test the lowercase_column_values function
  df_transformed = lowercase_column_values(sample_df_dataset, "first_name")
  assert df.count() == 4
  assert df_transformed.first()[0] == "john"
  assert df_transformed.second()[0] == "jane"
  assert df_transformed.third()[0] == "bob"
  assert df_transformed.fourth()[0] == "sarah"  

def test_filter_gender(sample_df_dataset):
  # Test the filter_column_from_dynamic_frame function
  dyf_filtered = filter_column_from_dynamic_frame(sample_df_dataset, "male", "gender")
  assert dyf_filtered.count() == 2
  assert dyf_filtered.first()[2] == "male"
  assert dyf_filtered.second()[2] == "male"