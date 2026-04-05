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
val dfDimHotelBooking = spark.read.option("inferSchema","true").option("header","true").csv("/FileStore/tables/dimHotelBooking.csv")
dfDimHotelBooking.count

// COMMAND ----------

//ex2
dfDimHotelBooking.select($"DSC_Employee").distinct.count

// COMMAND ----------

//ex3
val dfDimCustomer = spark.read.option("inferSchema","true").option("header","true").csv("/FileStore/tables/dimCustomer.csv")

// COMMAND ----------

dfDimCustomer.select("DSC_Customer").where($"DSC_Customer".like("A%")).show
dfDimCustomer.select("DSC_Customer").where($"DSC_Customer".startsWith("A")).show

// COMMAND ----------

val dfDimTravelBooking = spark.read.option("inferSchema","true").option("header","true").csv("/FileStore/tables/dimTravelBooking.csv")

// COMMAND ----------

//ex4
dfDimTravelBooking.select($"DSC_TravelDestiny").where(!($"DSC_TravelDestiny".endsWith("a"))).show

// COMMAND ----------

//ex5
dfDimHotelBooking.select(expr("*"),upper(expr("substring(DSC_TravelDestiny,1,2)")).as("COD_TravelDestiny")).show

// COMMAND ----------

//ex6
dfDimHotelBooking.where($"DSC_TravelDestiny" === "Brazil").count


// COMMAND ----------

//ex7
dfDimTravelBooking.groupBy($"DSC_Employee",$"DSC_TravelAgency").orderBy("DSC_TravelAgency").count().show

// COMMAND ----------

//ex8
val dfDimEmployee_date = dfDimEmployee.withColumn("Contract_Date",to_date($"Contract_Date".cast(StringType),"yyyyMMdd"))
dfDimEmployee_date.select($"DSC_Employee",round(months_between(current_date, $"Contract_Date")/12).as("Antiquity_compagny")).orderBy($"Antiquity_compagny".desc).show

// COMMAND ----------

dfDimEmployee_date.withColumn("DSC_Employee",split($"DSC_Employee"," "))
                  .withColumn("first_name",$"DSC_Employee".getItem(0))
                  .withColumn("Last_name",$"DSC_Employee".getItem(1)).show

// COMMAND ----------

//ex10
dfDimHotel.select(concat($"DSC_Hotel",lit(" "),$"DSC_HotelCountry")).show

// COMMAND ----------

//ex11
val dfDimCustomer_date = dfDimCustomer.withColumn("Birthdate", to_date($"Birthdate".cast(StringType),"yyyyMMdd"))
dfDimCustomer_date.show()

// COMMAND ----------

//ex12

val dfDimCustomer_age = dfDimCustomer_date.withColumn("Age", round(months_between(current_date, $"Birthdate")/12)).orderBy($"Age")

// COMMAND ----------

//ex13

dfDimCustomer_age.withColumn("Segment", when($"Age" <= 17, "A").when($"Age" <= 24, "B").when($"Age" <= 34, "C") .otherwise("G")).show

// COMMAND ----------

// ex 14
dfDimCustomer_age.select($"DSC_Customer", $"Age").orderBy($"Age".desc).limit(1).show
dfDimEmployee.withColumn("Age", round(months_between( current_date, to_date($"Birthdate".cast(StringType), "yyyyMMdd"))/12)).select($"DSC_Employee",$"Age").orderBy($"Age".desc).show

// COMMAND ----------

dfDimEmployee.show

// COMMAND ----------

//ex15
dfDimCar.join(dfFactCarRenting, $"PK_Car" === $"FK_Car", "left_anti").show()

// COMMAND ----------

dfDimEmployee.select($"Amt_Salary".as("Highest_Salary")).orderBy($"Highest_Salary".desc).limit(1).show
dfDimEmployee.select(max($"Amt_Salary").as("Highest_Salary")).show

// COMMAND ----------

dfDimEmployee.select(min($"Amt_Salary").as("Lowest_salary")).show

// COMMAND ----------

//ex18
dfFactHotelBooking.join(dfDimHotelBooking.where($"DSC_HotelService" ==="Breakfast"), "FK_TravelBooking")
    .select($"DSC_Hotel",$"DSC_HotelService",$"AMT_Accomodation")
    .orderBy($"AMT_Accomodation".asc)
    .limit(1)
    .show()

// COMMAND ----------

//ex19
import org.apache.spark.sql.expressions.Window
val windowCar = Window.partitionBy("DSC_RentalCompany")

dfFactCarRenting.join(dfDimCar, $"FK_Car" === $"PK_Car")
                .select($"DSC_RentalCompany", $"DSC_Car", $"AMT_DailyRate")
                .distinct
                .withColumn("AMT_Max", max($"AMT_DailyRate").over(windowCar))
                .where($"AMT_DailyRate"===$"AMT_Max")
                .show

// COMMAND ----------

//ex20

dfFactCarRenting.show

// COMMAND ----------

dfFactCarRenting.where($"FK_Date".like("201707%"))
  .select("FK_Car", "AMT_Rental")
  .groupBy("FK_Car")
  .agg(sum("AMT_Rental").as("Amt_rental"))
  .join(dfDimCar, $"FK_Car" === $"PK_Car")
  .select("DSC_Car","AMT_Rental")
  .orderBy($"AMT_Rental".desc)
  .limit(1)
  .show

// COMMAND ----------

//ex21

dfFactCarRenting
  .where($"FK_Date".like("2016%"))
  .groupBy($"FK_Car")
  .agg(sum("AMT_CarInsurance").as("AMT_insurance"))
  .join(dfDimCar, $"FK_Car" === $"PK_Car")
  .select("DSC_Car","AMT_insurance")
  .orderBy($"AMT_insurance".desc)
  .show

// COMMAND ----------

//ex22
dfFactHotelBooking
  .select("FK_TravelBooking","FK_Employee")
  .groupBy("FK_Employee")
  .agg(count("*").as("NbBooking"))
  .join(dfDimTravelBooking.select("PK_TravelBooking","DSC_TravelAgency","DSC_Employee"),$"PK_TravelBooking" === $"FK_TravelBooking"  )
  .select("DSC_TravelAgency","DSC_Employee","NbBooking")
  .show

// COMMAND ----------

//ex23
import org.apache.spark.sql.expressions.Window
val windowTravel = Window.partitionBy("DSC_TravelAgency").orderBy($"NbBooking".desc)
display(
dfDimTravelBooking
    .where($"DSC_TravelStatus" === "Confirmed")
    .join(dfFactHotelBooking, $"PK_TravelBooking" === $"FK_TravelBooking")
    .groupBy("DSC_TravelAgency","DSC_Employee")
    .agg(count("*").as("NbBooking"))
    .withColumn("Rank", rank.over(windowTravel))
   .withColumn("RankPerc", percent_rank().over(windowTravel))
    .where($"Rank" <= 3)
     )
    

// COMMAND ----------

//ex 24
val windowDesc = Window.orderBy($"NbBookings".desc)
val windowAsc = Window.orderBy($"NbBookings".asc)


dfDimTravelBooking.join(dfFactHotelBooking, $"PK_TravelBooking" === $"FK_TravelBooking")
  .groupBy("DSC_TravelAgency","DSC_Employee")
  .agg(countDistinct("PK_HotelBooking").as("NbBookings"))
  .withColumn("Top3", row_number().over(windowDesc))
  .withColumn("Bot3", row_number().over(windowAsc))
  .where($"Top3".lt(4).or($"Bot3".lt(4)))
  .drop("Top3","Bot3")
  .orderBy($"NbBookings".desc)
  .show


// COMMAND ----------

//ex 25
display(
dfFactInsurance
   .where( col("PK_Insurance") =!= 1 && col("AMT_Travel_Insurance")=!=0.0)
  .join(dfDimEmployee, $"FK_Employee" === $"PK_Employee")
  .groupBy("DSC_Employee")
  .agg(round(sum("AMT_Travel_Insurance"),2).as("Amt_Insurance"))
  
  .select("DSC_Employee","Amt_Insurance")
  .orderBy($"Amt_Insurance".desc)
)

// COMMAND ----------

//ex26
display(
dfDimEmployee.withColumn("Seniority", round(months_between(current_date, to_date($"Contract_Date".cast(StringType), "yyyyMMdd"))/12))
  .select("DSC_Employee", "DSC_JobTitle", "Seniority")
  .orderBy($"Seniority".desc)

  )



// COMMAND ----------

//ex27

val schemaTravelBook = new StructType(Array(
  new StructField("PK_TravelBooking",IntegerType),
  new StructField("FK_date",StringType),
  new StructField("FK_TravelAgency",IntegerType),
  new StructField("FK_Employee",IntegerType),
  new StructField("FK_HotelBooking",IntegerType),
  new StructField("FK_Customer",IntegerType),
  new StructField("AMT_Travel",DoubleType)
))

val dfFactTravelBooking_date = spark.read.option("header",true).schema(schemaTravelBook).csv("/FileStore/tables/factTravelBooking.csv")
display(
dfFactTravelBooking.withColumn("Year", year(to_date($"FK_date".cast(StringType),"yyyyMMdd")))
    .withColumn("Month", month(to_date($"FK_Date".cast(StringType),"yyyyMMdd")))
    .groupBy("Year","Month")
    .agg(sum($"AMT_Travel").as("Amt_travel"))
    .orderBy($"Year",$"Month")
)

// COMMAND ----------

//ex 28

display(
dfFactTravelBooking.withColumn("Year", year(to_date($"FK_date".cast(StringType),"yyyyMMdd")))
  .where($"Year"===2016)
  .groupBy("FK_Employee","Year")
    .agg(round(avg($"AMT_Travel"),2).as("AVG_Amt_travel"),round(sum($"AMT_Travel"),2).as("SUM_Amt_travel"),count($"AMT_Travel").as("Cnt_Amt_travel"))
  .join(dfDimEmployee, $"FK_Employee"===$"PK_Employee")
  .select("DSC_Employee","Year","AVG_Amt_travel","SUM_Amt_travel","Cnt_Amt_travel")
  .orderBy($"AVG_Amt_travel".desc)
)

// COMMAND ----------

//ex29
import org.apache.spark.sql.expressions.Window

val windowEmpTOP = Window.orderBy($"PercentTotal".desc)
val windowEmpBOT = Window.orderBy($"PercentTotal".asc)

val dfDimEmployee_sen = dfDimEmployee.withColumn("Seniority", round(months_between(current_date, to_date($"Contract_Date".cast(StringType),"yyyyMMdd"))/12))
display(
dfFactTravelBooking.groupBy("FK_Employee")
  .agg(sum("AMT_Travel").as("SUM_AMT_Travel"))
  .join(dfDimEmployee_sen, $"PK_Employee" === $"FK_Employee")
  .withColumn("Total",sum("SUM_AMT_Travel").over())
  .where($"Seniority" >= 5)
  .withColumn("PercentTotal",round($"SUM_AMT_Travel"/$"Total"*100,2))
  .withColumn("TOP",rank.over(windowEmpTOP))
  .withColumn("BOT",rank.over(windowEmpBOT))
  .select("DSC_Employee","Seniority","PercentTotal","SUM_AMT_Travel","Total")
  
  .orderBy($"PercentTotal".desc)
  
)

// COMMAND ----------



// COMMAND ----------

//ex30
val dfDimCarColor = dfDimCar.withColumn("DSC_CarColor", lit(null))


// COMMAND ----------

//ex31

val colors = List(
   ("Nissan Micra  1.0" ,"White"),
   ("Volkswagen UP 1.0", "Black"),
   ("Peugeot 108 1.0", "White"),
   ("Smart Forfour 1.1", "Black"),
   ("Renault Twingo 1.2", "White"),
   ("Volkswagen Polo 1.2", "Black"),
   ("Opel Adam 1.4 D", "White"),
   ("Opel Corsa 1.4 D", "Black"),
   ("Seat Ibiza 1.4 TDI", "Red"),
   ("Renault Clio 1.5 DCI", "Black"),
   ("Ford Fiesta 1.5", "White"),
   ("Audi A3 1.6", "Black"),
   ("BMW 116i 1.6", "Red"),
   ("Mercedes Benz C Class 1.5", "White"),
   ("Volkswagen Passat 2.0", "Grey"),
   ("BMW 320i Sport 2.0 Turbo", "Blue"),
   ("Mercedes-Benz CLS 500 5.0", "White"),
   ("Ford Mustang GT", "Green"),
   ("Ferrari 488 GTB", "Red"),
   ("Lamborghini Aventador", "Black"),
   ("Renault 5 GT Turbo", "Yellow"),
   ("Opel Corsa A 1.5 D", "Red"),
   ("Volkswagen Lupo 1.0", "Green"),
   ("Fiat Panda 1.2", "Grey"),
   ("Skoda Fabia 1.6", "Blue")
).toDF("DSC_Car","DSC_CarColor")

dfDimCar.join(colors ,Seq("DSC_car"), "left").show

// COMMAND ----------

//ex32
val schemaMotor = StructType(Seq(
StructField("PK_Moto",IntegerType),
  StructField("DSC_RentalCompany",IntegerType),
  StructField("DSC_Moto",IntegerType),
  StructField("Load_ID",IntegerType),
  StructField("HashColumn",IntegerType),
  StructField("Insert_Date",IntegerType),
  StructField("Update_Date",IntegerType)
))

val dfDimMotor = List(
  ("GlobalMotors", "Harley-Davidson Street 500"),
  ("GlobalMotors", "Harley-Davidson Street 750"),
  ("GlobalMotors", "Harley-Davidson Sportster Iron 883"),
  ("GlobalMotors", "Harley-Davidson V-Rod Night"),
  ("GlobalMotors", "Harley-Davidson Dyna Fat Bob")
).toDF("DSC_RentalCompany", "DSC_Moto")
val dfDimMoto = dfDimMotor.repartition(2).withColumn("PK_Motocycle", row_number.over(Window.orderBy(lit(1))))
  .withColumn("Load_ID", lit(1))
  .withColumn("HashColumn",lit(null))
  .withColumn("Insert_Date", current_date)
  .withColumn("Update_Date", current_date)
  .select("PK_Motocycle", "DSC_RentalCompany", "DSC_Moto", "Load_ID", "HashColumn", "Insert_Date", "Update_Date")


// COMMAND ----------

//ex33
dfDimCar.union(dfDimMoto).show(30)

// COMMAND ----------

def seniority(date:Int):Int = {
  1000
}
val seniorityUDF = udf(seniority(_:Int):Int)

dfDimCustomer.select(seniorityUDF($"Birthdate")).show

// COMMAND ----------

def power3(number:Double):Double = number * number * number
val seniorityUDF = udf(seniority(_:Int):Int)

// COMMAND ----------

val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
format.parse("2013-07-06").getDate()

// COMMAND ----------

def squareT(num:Int):Int={
  if(num < 500){
     return num/2
  }else{
    return num*num
  }  
}
val squareTval = squareT(_:Int):Int
val squareTUDF = udf(squareT(_:Int):Int)
spark.udf.register("squareTUDF",squareTval)
dfFactCarRenting.select($"AMT_Rental", squareTUDF($"AMT_Rental"), expr("squareTUDF(AMT_Rental)")).show

// COMMAND ----------

squareT(300)

// COMMAND ----------

dfDimCar.cube("DSC_RentalCompany","DSC_Car","PK_Car").agg(grouping_id()).orderBy($"DSC_RentalCompany".asc_nulls_first).show(50)

// COMMAND ----------

dfDimCar.groupBy("DSC_RentalCompany").agg(collect_list("DSC_Car")).collect

// COMMAND ----------

dfDimCar.show

// COMMAND ----------

dfDimCar.createOrReplaceGlobalTempView("ooo")

// COMMAND ----------

spark.sql("Select * from global_temp.ooo").show

// COMMAND ----------

spark.table("global_temp.ooo")

// COMMAND ----------

val myrdd = sc.parallelize(Seq(dfDimCar.first))

// COMMAND ----------

spark.createDataFrame(myrdd, dfDimCar.schema).show

// COMMAND ----------

myrdd.toDF(dfDimCar.schema)

// COMMAND ----------

val rdd = sc.parallelize(
  Seq(
    ("first", Array(2.0, 1.0, 2.1, 5.4)),
    ("test", Array(1.5, 0.5, 0.9, 3.7)),
    ("choose", Array(8.0, 2.9, 9.1, 2.5))
  )
)
rdd.toDF.rdd.collect

// COMMAND ----------

case class test(v1:Int)

sc.parallelize(Seq(test(5),test(3))).toDF.rdd.collect

// COMMAND ----------

dfDimCar.select(split($"DSC_Car"," ").as("tab")).selectExpr("tab[0]","tab[04]").show

// COMMAND ----------

dfDimCar.select(split($"DSC_Car"," ").as("tab")).selectExpr(Map($"tab[0]",$"tab[1]")).show

// COMMAND ----------


dfDimCar.select(map($"DSC_Car",$"DSC_RentalCompany").as("MAP")).select(explode($"MAP")).show(false)

// COMMAND ----------

case class Car(PK_Car:Int,DSC_RentalCompany:String,DSC_Car:String,Load_ID:Int,HashColumn:String,Insert_Date:String,Update_Date:String)
spark.table("dimcar_csv").as[Car]

// COMMAND ----------



// COMMAND ----------

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

class MonUDAF extends UserDefinedAggregateFunction{
  def inputSchema
}

// COMMAND ----------

spark.

// COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "300")

// COMMAND ----------

Seq(
     ("first", 1),
     ("test", 2),
     ("choose", 3)
  ).toDF("name","array").as[test1].collect

// COMMAND ----------



// COMMAND ----------

case class test1(name:String, array: Int)


// COMMAND ----------

spark.range(1,200).toDF.show

// COMMAND ----------

val dfDimCar = spark.read.option("header","true").csv("/FileStore/tables/dimCar.csv")

// COMMAND ----------

dfDimCar.write.mode("append").csv("/FileStore/tables/dimCar2.csv")

// COMMAND ----------

spark.read.csv("/FileStore/tables/dimCar2.csv").count

// COMMAND ----------

dfDimCar.count

// COMMAND ----------

dfDimCar.cache

// COMMAND ----------

dfDimCar.unpersist

// COMMAND ----------

spark.sql("select * from dimcar_csv")

// COMMAND ----------

dfDimCar.select(explode(split($"DSC_Car"," ")).as("test")).withColumn("test2",$"test").na.replace("test",Map("Peugeot"->null)).na.fill(Map("test"->"OOOO")).show

// COMMAND ----------

val my_regex = "(Nissan&Micra)"
dfDimCar.select($"DSC_Car",regexp_extract($"DSC_Car",my_regex,1)).show


// COMMAND ----------

dfDimCar.select(translate($"DSC_Car","DaaC","1234")).show

// COMMAND ----------

dfDimCar.select(regexp_replace($"DSC_Car","Ford","dddd")).show

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
val windowCar = Window.orderBy("DSC_RentalCompany")

dfDimCar.withColumn("Count",lit(1))
.withColumn("Rank", rank.over(windowCar))
.withColumn("RankDense", dense_rank.over(windowCar))
.withColumn("Row", row_number.over(windowCar)).show

// COMMAND ----------

dfDimCar.rdd.getNumPartitions

// COMMAND ----------


import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
class BoolAnd extends UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", BooleanType) :: Nil)
  def bufferSchema: StructType = StructType(
    StructField("result", BooleanType) :: Nil
  )
  def dataType: DataType = BooleanType
  def deterministic: Boolean = true
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = true
  }
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
  }
  def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}



// COMMAND ----------

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

// COMMAND ----------

def test2(a:Int):Int = 5*5

// COMMAND ----------

val udfTest = udf(test2(_:Int):Int)


// COMMAND ----------

dfDimCar.select(udfTest(lit(1)), expr("ddd(1)")).show

// COMMAND ----------

sparkSession.conf.set("spark.sql.shuffle.partitions",100)


// COMMAND ----------

spark.udf.register("ddd",test2(_:Int):Int)

// COMMAND ----------

sparkSession.conf.set("spark.sql.shuffle.partitions","100")

// COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions","22")

// COMMAND ----------

val df = spark.range(100)
println(df.count)
val split_df = df.randomSplit(Array(0.5,0.5))
println(split_df(0).count)
println(split_df(1).count)

// COMMAND ----------

df.show

// COMMAND ----------


