"""
etl_job.py
~~~~~~~~~~

This Python module contains an Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \ #change it to the spark master cluster url
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql import functions as F
from operator import add
from functools import reduce
from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute Today_Load ETL
    url = 'tests/test_data/energy/NOP_LOAD_FORECAST_20180214_04_input.csv'
    df_NOP_0214_04 = extract_data_csv(spark,url)
    
    groupbyList = ["CONGESTION_ZONE","FORECAST_DT","HOUR_NUM"]
    targetColumn = "NOP"
    resultColumnName = "TODAY_LOAD"
    df_NOP_0214_04_GB = groupby_data(df_NOP_0214_04,groupbyList,targetColumn,resultColumnName)
    #df_NOP_0214_04_GB.show()
    
    #execute Prev_Day_Load ETL
    url = 'tests/test_data/energy/NOP_LOAD_FORECAST_20180213_11_input.csv'
    df_NOP_0213_11 = extract_data_csv(spark,url)

    groupbyList = ["CONGESTION_ZONE","FORECAST_DT","HOUR_NUM"]
    targetColumn = "NOP"
    resultColumnName = "PREV_DAY_LOAD"
    df_NOP_0213_11_GB = groupby_data(df_NOP_0213_11,groupbyList,targetColumn,resultColumnName)
    
    #execute Hour_Load ETL
    url = 'tests/test_data/energy/LFG_ST_Hourly_20180213_input.csv'
    df_LFG_0213 = extract_data_csv(spark,url)

    groupbyList = ["CONGESTION_ZONE","FORECAST_DT","HOUR_NUM"]
    sumList = ["UNADJ_LOAD","DISTRIB_LOSS_LOAD","TRANSMISSION_LOSS_LOAD"]
    resultColumnName = "ADJ_LOAD"
    df_LFG_0213_GB = groupby_agg_data(df_LFG_0213,groupbyList,sumList,resultColumnName)
    
    #Join three DataFrames
    joinList = ["CONGESTION_ZONE","FORECAST_DT","HOUR_NUM"]
    df_join = join_data(df_NOP_0214_04_GB,df_NOP_0213_11_GB,joinList,'left')
    df_join_three = join_data(df_join,df_LFG_0213_GB,joinList,'left')
    output = order_data(df_join_three, joinList)
    
    #Write output to output.csv
    load_data(output)
    
    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data_csv(spark,url):
    """Load data from Csv file format.
        
    :param spark: Spark session object.
    :param url: Csv directory.
    :return: Spark DataFrame.
    """
    df_csv = (
            spark
            .read
            .format('csv')
            .options(header='true', inferSchema='true')
            .load(url))

    #df_csv.show()

    return df_csv


def groupby_data(df_csv,groupbyList,targetColumn,resultColumnName):
    """Group the tagetColumn by multiple columns name from the groupbyList.
        
    :param df_csv: Input DataFrame.
    :param groupbyList: List of columns for group by.
    :param targetColumn: Target column to be grouped.
    :param resultColumnName: Rename the target sum column.
    :return: Transformed DataFrame.
    """
    df_groupby = (
                  df_csv
                  .groupby(groupbyList)
                  .sum(targetColumn))

    sumColumnName = df_groupby.columns[-1]

    df_groupby_csv = (
                      df_groupby
                      .withColumnRenamed(sumColumnName,resultColumnName))

    return df_groupby_csv


def groupby_agg_data(df_csv,groupbyList,sumList,resultColumnName):
    """Sum multiple columns using the column names from sumList into a new column named resultColumnName. Then group this new column by the column names from groupbyList.
        
        :param df_csv: Input DataFrame.
        :param groupbyList: List of columns for group by.
        :param sumList: List of columns to be summed up.
        :param resultColumnName: Rename the target output column.
        :return: Transformed DataFrame.
    """
    temp_columnName = "sum(" + resultColumnName + ")"

    df_agg_csv = (
                  df_csv
                  .withColumn(resultColumnName, reduce(add, [F.col(x) for x in sumList])))
    
    df_groupby_agg_csv = (
                          df_agg_csv
                          .groupby(groupbyList)
                          .sum(resultColumnName)
                          .orderBy(groupbyList)
                          .withColumnRenamed(temp_columnName,resultColumnName))

    return df_groupby_agg_csv


def join_data(df_left, df_right, joinList,joinType):
    """join left DataFrame with right DataFrame.
        
        :param df_left, df_right: Input DataFrame.
        :param joinList: List of columns to be joined on.
        :param joinType: inner, outer, left, or right join
        :return: Joined DataFrame.
    """
    output = (
              df_left
              .join(df_right,
                    joinList,
                    how = joinType)
              .na
              .fill(0))
    return output


def order_data(df, orderbyList):
    """Order the data.
    
        :param df: Input DataFrame.
        :param orderbyList: List of columns for order by.
        :return: Ordered DataFrame.
    """
    df_orderby = (
                  df
                  .orderBy(orderbyList))
    return df_orderby


def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     #.coalesce(1)
     #.write
     #.csv('tests/test_data/energy/data/',mode='overwrite',header=True))
     .toPandas()
     .to_csv('tests/test_data/energy/data/output.csv', header = True))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
