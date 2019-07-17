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

    val sample = Array("{\"index\":1,\"host\":\"dev#aa\"}", "{\"index\":2,\"host\":\"stg#bb\"}", "{\"index\":3,\"host\":\"tst#cc\"}", "{\"index\":4,\"host\":\"op#dd\"}")
    val data = sparkContext.parallelize(sample)

    data.foreach(f=>{
      println(f)
    })

    val df = sparkSession.read.json(data)
    val schema = df.schema
    println("schema-----")
    println(schema)
    //StructType(StructField(host,StringType,true), StructField(index,LongType,true))

    val df2 = sparkSession.read.json(data).rdd

    //---
    val explodedDf = sparkSession.createDataFrame(df2, schema)
    explodedDf.createOrReplaceTempView("test")
    val res = sparkSession.sql("select * from test").rdd
    println("res-----")
    res.foreach(f=>{
      println(f)
    })

    //---
    val explodedDf2 = sparkSession.createDataFrame(df2, schema).withColumn("res",explode(split(col("host"), "#"))).drop(col("host"))
    explodedDf2.limit(50).createOrReplaceTempView("test2")
    val res2 = sparkSession.sql("select * from test2").rdd
    println("res2-----")
    res2.foreach(f=>{
      println(f)
    })

    //--- error case test
    var column: Array[StructField] = Array()
    column = column :+ StructField("host" , StringType, true)
    column = column :+ StructField("index" , LongType, true)
    //column = column :+ StructField("res" , StringType, true)
    var schema2 = StructType(column)
    println("schema2-----")
    println(schema2)
    //StructType(StructField(host,StringType,true), StructField(index,LongType,true))

    val explodedDf3 = sparkSession.createDataFrame(df2, schema2).withColumn("res",explode(split(col("host"), "#")))//.drop(col("host"))
    explodedDf3.limit(50).createOrReplaceTempView("test3")
    val res3 = sparkSession.sql("select * from test3").rdd
    println(explodedDf3.schema)
    println("res3-----")
    res3.foreach(f=>{
      println(f)
    })

  }
}
