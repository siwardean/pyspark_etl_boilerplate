// Databricks notebook source
val dfDimCar = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/dimCar.csv")
val dfDimCarRenting = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/dimCarRenting.csv")
val dfDimCustomer = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/dimCustomer.csv")
val dfDimDate = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/dimDate.csv")
val dfDimEmployee = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/dimEmployee.csv")
val dfDimHotel = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/dimHotel.csv")
val dfDimHotelBooking = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/dimHotelBooking.csv")
val dfDimInsurance = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/dimInsurance.csv")
val dfDimTravelBooking = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/dimTravelBooking.csv")
val dfFactCarRenting = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/factCarRenting.csv")
val dfFactHotelBooking = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/factHotelBooking.csv")
val dfFactInsurance = spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/factInsurance.csv")
val dfFactTravelBooking= spark.read.option("inferSchema", true).option("header",true).csv("/FileStore/tables/factTravelBooking.csv")

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

//ex1
dfDimCar.select($"Update_Date",unix_timestamp($"Update_Date").as("TimeStamp"))
  .withColumn("DateTimestamp", to_date(to_timestamp($"TimeStamp"),"ss"))
  .withColumn("Date", to_timestamp($"TimeStamp")).show

// COMMAND ----------

//ex2 
val splitDf = dfDimCar.randomSplit(Array(0.8,0.2))
println(splitDf(0).count)
println(splitDf(1).count)

// COMMAND ----------

//ex3
// First way
sqlContext.setConf("spark.sql.shuffle.partitions", "100")
println(sqlContext.getConf("spark.sql.shuffle.partitions"))

sqlContext.setConf("spark.sql.shuffle.partitions", "200")
println(sqlContext.getConf("spark.sql.shuffle.partitions"))


// COMMAND ----------

//ex3
// Second way

println(sc.range(1,10).toDF.distinct.rdd.getNumPartitions)

//Third way
val df1 = sc.range(1,10).toDF
val df2 = sc.range(1,10).toDF

println(df1.join(df2, "value").rdd.getNumPartitions)

// COMMAND ----------

// ex4
sc.range(1,100).toDF.show


// COMMAND ----------

//ex6
dfDimCar.write.mode("overwrite").option("sep","\t").option("header","true").csv("/FileStore/tables/dimCar2.csv")

// COMMAND ----------

spark.read.option("sep","\t").option("header","true").csv("/FileStore/tables/dimCar2.csv").show

// COMMAND ----------

//ex7

val regex = """((?:https:\/\/www\.)([a-z]+\.[a-z]+))"""
val url = Seq("https://www.google.com").toDF

// 2 = on récupère le deuxième ()  => ([a-z]+\.[a-z]+) mais on doit matcher tout le pattern
url.select(regexp_extract($"value", regex ,2)).show(false)


// COMMAND ----------

//ex8
val df_json = spark.read.option("inferSchema","true").json("/FileStore/tables/test.json")

println(df_json.schema)

val newSchemaJSON = StructType(Seq(
  StructField("DEST_COUNTRY_NAME",StringType,true),
  StructField("ORIGIN_COUNTRY_NAME",StringType, true),
  StructField("array_col",ArrayType(StringType, true),true)
))
val df_json_2 = spark.read.schema(newSchemaJSON).json("/FileStore/tables/test.json").show

// COMMAND ----------

//ex9
def myFunct(input:Int):Int = {
  input * input
} 
// Convert to UDF in scala
val myFunctUDF = udf(myFunct(_:Int):Int)

// Convert to UDF in sql
spark.udf.register("myFunctUDF_sql",myFunct(_:Int):Int)

dfDimCar.select($"PK_Car", myFunctUDF($"PK_Car"), expr("myFunctUDF_sql(PK_Car)")).show

// COMMAND ----------

// ex 10
dfDimEmployee.select($"DSC_Employee")
  .withColumn("split", split($"DSC_Employee", " "))
  .withColumn("First Name", expr("split[0]"))
  .withColumn("Last Name", upper($"split".getItem(1)))
  .drop("split")
.show

// COMMAND ----------

//ex 11 

import org.apache.spark.sql.expressions.Window
val windowCarRent = Window.partitionBy("FK_TravelAgency")
  
dfFactCarRenting.na.drop()
  .withColumn("MeanByComp_CatInsurance", mean("AMT_CarInsurance").over(windowCarRent))
  .groupBy("MeanByComp_CatInsurance","FK_TravelAgency","FK_Employee")
  .agg(sum("AMT_CarInsurance").as("sum_AMT_CarInsurance"), mean("AMT_CarInsurance").as("Mean_CarInsurance"))
  .show



// COMMAND ----------

//ex12
val df1 = dfDimCustomer.withColumn("ID_add", when($"DSC_Gender" === "Male", 1).when($"DSC_Nationality" === "Portuguese", 1).otherwise(2))
val df2 = dfDimCustomer.withColumn("ID_add",lit(1))
val df3 = df2.union(df2)
println(df1.count)
println(df3.count)

println(df1.intersect(df2).count)
println(df1.intersectAll(df2).count)
println(df3.intersectAll(df2).count)
println(df3.intersectAll(df3).count)

// COMMAND ----------



// COMMAND ----------

//ex14
df3.orderBy($"ID_add".asc_nulls_first).show


// COMMAND ----------

//ex15
val dfWithNull = dfDimCustomer.select("PK_Customer","DSC_Customer","DSC_Gender").join(dfDimCar, $"PK_Customer" === $"PK_Car", "left").orderBy($"PK_Car".asc_nulls_first)

dfWithNull.na.drop(7).show


// COMMAND ----------

//ex 16
dfDimCar.createOrReplaceTempView("dimCar")

// COMMAND ----------

//ex17
val firstRow = dfDimCar.first
firstRow.getInt(0)

// COMMAND ----------

// ex18
import org.apache.spark.storage.StorageLevel.DISK_ONLY
dfDimCar.persist(DISK_ONLY)

// COMMAND ----------

// ex19
dfDimCar.repartition(5).write.parquet("/FileStore/tables/dimCar.parquet")

// COMMAND ----------

spark.read.parquet("/FileStore/tables/dimCar.parquet").show

// COMMAND ----------

//ex20 and 21

dfFactCarRenting
    .groupBy("FK_TravelAgency")
    .agg(max("AMT_CarInsurance").as("max_AMT_CarInsurance"), min("AMT_CarInsurance").as("min_AMT_CarInsurance"))
    .withColumnRenamed("max_AMT_CarInsurance", "max")
    .withColumnRenamed("min_AMT_CarInsurance", "min")

.show


// COMMAND ----------

val dfDimCarClean = spark.read.option("inferSchema","true").option("header","true").parquet("/FileStore/tables/dimCar.parquet")

// COMMAND ----------

dfDimCarClean.write.mode("overwrite").option("header","true").csv("/FileStore/tables/dimCar.csv")

// COMMAND ----------

df.map(funct(_))
