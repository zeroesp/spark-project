package test.parsing

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object ParsingTest {
  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("ParsingTest").setMaster("local[2]").set("spark.driver.bindAddress", "127.0.0.1")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().appName("sqlTest").getOrCreate()

    //log level modify
    LogManager.getRootLogger.setLevel(Level.WARN)

    //---------- sample data
    println("@sample data ----- start")
    val sample = Array("{\"index\":1,\"host\":\"dev#aa\"}", "{\"index\":2,\"host\":\"stg#bb\"}", "{\"index\":3,\"host\":\"tst#cc\"}", "{\"index\":4,\"host\":\"op#dd\"}")
    val data = sparkContext.parallelize(sample)
    println("res-----")
    data.foreach(f=>{
      println(f)
    })
    println("@sample data ----- end")


    //---------- sample data to DataFrame
    println("@sample data(rdd[Sting]) to DataFrame ----- start")
    val df = sparkSession.read.json(data)
    val schema = df.schema
    println("schema-----")
    println(schema)
         //StructType(StructField(host,StringType,true), StructField(index,LongType,true))
    println("@sample data to DataFrame ----- end")


    //---------- df to rdd & rdd to DataFrame with schema
    println("@df to rdd[Row] & rdd[Row] to DataFrame with schema: StructType ----- start")
    val df2 = sparkSession.read.json(data).rdd

    val explodedDf = sparkSession.createDataFrame(df2, schema)
    explodedDf.createOrReplaceTempView("test")
    val res = sparkSession.sql("select * from test").rdd
    println("res-----")
    res.foreach(f=>{
      println(f)
    })
    println("@df to rdd & rdd to DataFrame with schema ----- end")


    //---------- createDataFrame - colume modify
    println("@createDataFrame - colume modify ----- start")
    //--- flat
    val explodedDf2 = sparkSession.createDataFrame(df2, schema).withColumn("res",explode(split(col("host"), "#"))).drop(col("host"))
    explodedDf2.limit(50).createOrReplaceTempView("test2")
    val res2 = sparkSession.sql("select * from test2").rdd

    println("schema----- " + schema)
    println("schema2----- " + explodedDf2.schema)
    println("res2-----")
    res2.foreach(f=>{
      println(f)
    })
    println("@createDataFrame - colume modify ----- end")


    //---------- createDataFrame - make schema
    println("@createDataFrame - make schema ----- start")
    //--- error case test
    var column: Array[StructField] = Array()
    column = column :+ StructField("host" , StringType, true)
    column = column :+ StructField("index" , LongType, true)
    //column = column :+ StructField("res" , StringType, true)
    val schema3 = StructType(column)

    val explodedDf3 = sparkSession.createDataFrame(df2, schema3).withColumn("res",explode(split(col("host"), "#")))//.drop(col("host"))
    explodedDf3.limit(50).createOrReplaceTempView("test3")
    val res3 = sparkSession.sql("select * from test3").rdd

    println("schema3----- " + schema3)
         //StructType(StructField(host,StringType,true), StructField(index,LongType,true))
    println("schema3 df----- " + explodedDf3.schema)
    println("res3-----")
    res3.foreach(f=>{
      println(f)
    })
    println("@createDataFrame - make schema ----- end")

    //---------- createDataFrame - make schema
    println("@createDataFrame - two schema ----- start")
    val explodedDf4 = sparkSession.createDataFrame(df2, schema3).withColumn("res",explode(split(col("host"), "#"))).select("res")
    explodedDf4.limit(50).createOrReplaceTempView("test4")
    val res4 = sparkSession.sql("select * from test4").rdd

    println("schema4 df----- " + explodedDf4.schema)
    explodedDf4.foreach(f=>{
      println(f + ", " + f.schema)
    })
    println("res4-----")
    res4.foreach(f=>{
      println(f)
    })
    println("@createDataFrame - two schema ----- end")

  }
}
