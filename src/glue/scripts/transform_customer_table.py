from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lower
import logging

def create_spark_context():
  """
  This function creates a spark context
  :return GlueContext, SparkContext
  """
  sc = SparkContext()
  glueContext = GlueContext(sc)
  spark = glueContext.spark
  job = Job(glueContext)
  return glueContext, spark

# Create function to 
def read_customer_dataset(glueContext):
  """
  This function reads a customer dataset from aws s3 bucket
  :param glueContext: GlueContext
  :return dyf: DynamicFrame
  """
  logging.info("Read customer dataset from AWS s3 bucket")
  dyf = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                      connection_option={
                                                        paths: [
                                                          "s3://my-bucket-data-uploads/customer/customer_info/"
                                                          ]
                                                        },
                                                      format="parquet")
  return dyf 

def filter_column_from_dynamic_frame(dyf, filter_value, column_name:str):
  """
  This function filters a column in a dynamic frame
  :param dyf: DynamicFrame
  :param filter_value: Value to filter
  :param column_name: Column to filter
  :return dyf: DynamicFrame with filtered column 

  Note: This function assumes that the column is case-insensitive.
  """
  logging.info(f"Filter column {column_name} with value {filter_value}")
  dyf = dyf.filter(f=lambda x: lower(x[column_name]) == lower(filter_value))
  return dyf


def dynamic_frame_to_pyspark_dataframe(dyf):
  """
  Converts a DynamicFrame to a PySpark DataFrame
  :param dyf: DynamicFrame
  :return df: PySpark DataFrame
  """
  logging.info("Convert DynamicFrame to PySpark DataFrame")
  df = dyf.toDF()
  return df


def lowercase_column_values(df, column_name:str):
  """
  This function makes values in a specific column that is a string all lower case.
  :param df: PySpark DataFrame
  :param column_name: Column to make lower case
  :return df: PySpark DataFrame with lowercase values in the specified column
  """
  df = df.withColumnName(column_name, lower(col(column_name)))
  return df


def write_dataframe_to_s3(df, glueContext, outputPath:str):
  """
  This function writes a PySpark DataFrame to an AWS S3 bucket in parquet format
  :param df: PySpark DataFrame
  :param glueContext: GlueContext
  :param outputPath: S3 path to write the output
  """
  logging.info(f"Write PySpark DataFrame to AWS S3 bucket at {outputPath}")
  # Convert pyspark dataframw to dynamic frame
  dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
  ## write dynamic frame to s3 bucket
  glueContext = GlueContext(SparkContext.getOrCreate())
  glueContext.write_dynamic_frame.from_options(frame=dyf, 
                                              connectionType="s3",
                                              connection_option={
                                                "path": outputPath,
                                                "partionKeys": []
                                              },
                                              format="parquet")
   
def main():
  """
   
  """   
  glueContext, spark = create_spark_context()
  dyf = read_customer_dataset(glueContext)
  dyf_filtered = filter_column_from_dynamic_frame(dyf, 'male', 'gender')
  dyf_filtered.show()
  df = dynamic_frame_to_pyspark_dataframe(dyf_filtered)
  df_transformed = lowercase_column_values(df, 'first_name')
  outputPath = r"s3://data-uploads/customer/customer_info_filtered"
  df_transformed.Show()
  write_dataframe_to_s3(df_transformed, glueContext, outputPath)

if __name__ == "__main__":
  main()