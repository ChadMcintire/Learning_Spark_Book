from pyspark.sql import Row
from pyspark.sql import SparkSession


spark = (SparkSession
  .builder
  .appName("Example-3_6")
  .getOrCreate())

#making a row
blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
    ["twitter", "LinkedIn"])

#accessing elements
print("element 1", blog_row[1])

#make a dataframe from a row
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()



