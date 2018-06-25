package test.tcp

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.CanCommitOffsets

object TcpConsumeTest {
  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("TcpConsumer").setMaster("local[2]").set("spark.driver.bindAddress", "127.0.0.1")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    val sqlContext = SparkSession.builder().appName("sqltest").getOrCreate()

    //log level modify
    LogManager.getRootLogger.setLevel(Level.WARN)

    var dstreams_raw: Array[DStream[String]] = Array()
    var dstreams: Array[DStream[Row]] = Array()
    dstreams_raw = dstreams_raw :+ streamingContext.socketTextStream("127.0.0.1", 17888)//.map(v => new String(v.getBytes()))
    //dstreams_raw(0).print()

    println("test")

    dstreams = dstreams :+ dstreams_raw(0).transform{ r=>
      if(r.isEmpty()){
        println("paser r empty")
      }else{
        println("paser r")
        println(r.first.toString)
      }
      val colSize = r.first.split(",",-1).length
      println(colSize)
      r.map{s=>
        val elems = s.split(",", -1)
        var columns: Seq[Any] = Seq()
        columns = columns :+ elems(0)
        columns = columns :+ elems(1)
        columns = columns :+ elems(2)
        Row.fromSeq(columns)
      }
    }

    dstreams(0).print

    /*
    val lines = streamingContext.socketTextStream("127.0.0.1", 17888)

    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    */

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
