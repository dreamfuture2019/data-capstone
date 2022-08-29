import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create and Return a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['AWS']['AWS_ACCESS_KEY_ID'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['AWS']['AWS_SECRET_ACCESS_KEY'])

    return spark

def sas_value_parser(sas_file, value, columns):
    """
    This procedure is common function to extract data and return DataFrame
    Parameters:
    * value: such as i94cntyl, i94prtl, i94model, i94addrl, I94VISA, etc
    * columns: such as [code, name]
    """
    file_string = ''
    
    with open(sas_file) as f:
        file_string = f.read()
    
    file_string = file_string[file_string.index(value):]
    file_string = file_string[:file_string.index(';')]
    
    line_list = file_string.split('\n')[1:]
    data = []
   
    for line in line_list:
        
        if '=' in line:
            code, val = line.split('=')
            code = code.strip()
            val = val.strip()

            if code[0] == "'":
                code = code[1:-1]

            if val[0] == "'":
                val = val[1:-1]
            
            data.append((code, val))
        
            
    return pd.DataFrame(data, columns=columns)

def process_immigration_data(spark, input_data, output_data):
    """
    This procedure processes immigration SAS files and load into DataFrame to extract and then store into DataLake
    Parameters:
    * spark: the Spark Session variable
    * input_data: the data source path variable
    * output_data: the s3 path variable to store dimensional tables
    """    
    
    # read immigration data file
    df = spark.read.format('com.github.saurfang.sas.spark').load(input_data)

    # extract columns to create immigration table
    immigration_table = df.dropDuplicates(['cicid'])
    
    # write songs table to parquet files partitioned by year and artist
    immigration_table.write.partitionBy('i94yr', 'i94mon') \
                     .parquet(os.path.join(output_data, 'immigrations'), 'overwrite')
    
    # create a udf to convert arrival date in SAS format to datetime object
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    
    # extract data to create arrival date table
    arrivals_df = df_spark.select(['arrdate']).withColumn("arrdate", get_datetime(df_spark.arrdate)) \
                                              .dropDuplicates('arrdate') \
                                              .withColumn('arrival_day', dayofmonth('arrdate')) \
                                              .withColumn('arrival_week', weekofyear('arrdate')) \
                                              .withColumn('arrival_month', month('arrdate')) \
                                              .withColumn('arrival_year', year('arrdate')) \
                                              .withColumn('arrival_weekday', dayofweek('arrdate'))
    
    # write arrivals date table to parquet files
    arrivals_df.write.partitionBy('arrival_year') \
                     .parquet(os.path.join(output_data, 'arrival_dates'), 'overwrite')
    
def process_sas_data(spark, input_data, output_data):
    """
    This procedure processes I94_SAS_Labels_Descriptions SAS files and load into DataFrame to extract and then store into DataLake
    Parameters:
    * spark: the Spark Session variable
    * input_data: the data source path variable
    * output_data: the s3 path variable to store dimensional tables
    """ 
    
    # extract countries data
    i94cit_res = sas_value_parser(input_data, 'i94cntyl', ['code', 'name'])
    i94cit_res=spark.createDataFrame(i94cit_res)    
    # write countries to s3
    i94cit_res.write.csv(os.path.join(output_data, 'i94cntyl'), sep=',')
    
    # extract ports data
    i94prtl_res = sas_value_parser(input_data, 'i94prtl', ['code', 'name'])
    i94prtl_res=spark.createDataFrame(i94prtl_res)
    # write ports data to s3
    i94prtl_res.write.csv(os.path.join(output_data, 'i94prtl'), sep=',')
    
    # extract address data
    i94addrl_res = sas_value_parser('data/I94_SAS_Labels_Descriptions.SAS', 'i94addrl', ['code', 'address'])
    i94addrl_res=spark.createDataFrame(i94addrl_res)
    # write ports data to s3
    i94addrl_res.write.csv(os.path.join(output_data, 'i94prtl'), sep=',')
    
def main():
    """
    - Create a Spark Session
    - Load and Transform SAS file from DataSource into DataLake
    """
    spark = create_spark_session()
    output_data = "s3a://datalake-udacity-haipd4/"
    
    process_immigration_data(spark, "../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat", output_data)    


if __name__ == "__main__":
    main()