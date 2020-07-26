from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql.functions import col, countDistinct, to_timestamp, year

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

    #make a column then drop the column it was formed from.
    fire_ts_df = (new_fire_df
        .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
        .drop("CallDate")
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
        .drop("WatchDate")
        .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), 
        "MM/dd/yyyy hh:mm:ss a"))
        .drop("AvailableDtTm"))

    
    (fire_ts_df
        .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
        .show(5, False))

    #(fire_ts_df
    #    .select(year('IncindentDate'))
    #    .district()
    #    .orderBy(year('IncidentDate'))
    #    .show())

    
