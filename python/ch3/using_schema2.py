from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql.functions import col, countDistinct, to_timestamp, year
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = (SparkSession
        .builder
        .appName("Example-3_6")
        .getOrCreate())

    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
        StructField('UnitID', StringType(), True),
        StructField('IncidentNumber', IntegerType(), True),
        StructField('CallType', StringType(), True),
        StructField('CallDate', StringType(), True),
        StructField('WatchDate', StringType(), True),
        StructField('CallFinalDisposition', StringType(), True),
        StructField('AvailableDtTm', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('City', StringType(), True),
        StructField('Zipcode', IntegerType(), True),
        StructField('Battalion', StringType(), True),
        StructField('StationArea', StringType(), True),
        StructField('Box', StringType(), True),
        StructField('OriginalPriority', StringType(), True),
        StructField('Priority', StringType(), True),
        StructField('FinalPriority', IntegerType(), True),
        StructField('ALSUnit', BooleanType(), True),
        StructField('CallTypeGroup', StringType(), True),
        StructField('NumAlarms', IntegerType(), True),
        StructField('UnitType', StringType(), True),
        StructField('UnitSequenceInCallDispatch', IntegerType(), True),
        StructField('FirePreventionDistrict', StringType(), True),
        StructField('SupervisorDistrict', StringType(), True),
        StructField('Neighborhood', StringType(), True),
        StructField('Location', StringType(), True),
        StructField('RowID', StringType(), True),                
        StructField('Delay', FloatType(), True)])

    sf_fire_file = "../../data/fire-department_updated.csv"
    #sf_fire_file = "../../data/fire-department-calls-for-service.csv"
    #sf_fire_file = "../../data/sf-fire-calls.csv"
    fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
    few_fire_df = (fire_df  
                    .select("IncidentNumber", "AvailableDtTm", "CallType")   
                    .where(col("CallType") != "Medical Incident"))
    few_fire_df.show(5, truncate=False)

    few_fire_df = (fire_df
        .select("IncidentNumber", "AvailableDtTm", "CallType")
        .where(col("CallType") != "Medical Incident"))
    few_fire_df.show(5, truncate=False)

    (fire_df
        .select("CallType")
        .where(col("CallType").isNotNull())
        .agg(countDistinct("CallType").alias("DistinctCallTypes"))
        .show())

    (fire_df
        .select("CallType")
        .where(col("CallType").isNotNull())
        .distinct()
        .show(10, False))

    #change the colomn name, select on it, filter on greater than 5, show 5
    new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
    (new_fire_df
        .select("ResponseDelayedinMins")
        .where(col("ResponseDelayedinMins") > 5)
        .show(5, False))

    #.withColumn() is to rename, change the value, or convert the type of an
    #existing dataframe column

    #2020-07-16 00:00:00

    (fire_df                                # '2020-07-16 00:00:00'
        .select("CallDate", "WatchDate", "AvailableDtTm")
        .show(200, False))

    #make a column then drop the column it was formed from.
    #change the string format it is currently in to day time format
    #reference https://html.developreference.com/article/15793767/PySpark+dataframe+convert+unusual+string+format+to+Timestamp
    fire_ts_df = (new_fire_df
        .withColumn("IncidentDate", to_timestamp("CallDate", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .withColumn("Year", year('IncidentDate'))
        .drop("CallDate")                                    #2020-07-16T00:00:00.000
        .withColumn("OnWatchDate", to_timestamp("WatchDate", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .drop("WatchDate")
        .withColumn("AvailableDtTS", to_timestamp("AvailableDtTm",
        "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .drop("AvailableDtTm"))


    #check out the columns we just reformated from string format to datetime 
    (fire_ts_df
        .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
        .show(50, False))

    #order by unique year combinations
    (fire_ts_df
        .select(year('IncidentDate'))
        .distinct()
        .orderBy(year('IncidentDate'))
        .show())

    #Select CallType and Year, check where calltype is not null, group by year
    #and Calltype, make a count of it, order by CallType and Year, Show the
    #first 100
    (fire_ts_df
        .select("CallType", "Year")
        .where(col("CallType").isNotNull())
        .groupBy("CallType", "Year")
        .count()
        .orderBy("CallType", "Year", ascending=False)
        .show(n=100, truncate=False))

    (fire_ts_df
        .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
        F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
        .show())
