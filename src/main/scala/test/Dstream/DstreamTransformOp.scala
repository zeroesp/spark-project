package test.Dstream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

object DstreamTransformOp extends Serializable {
  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("sparkDstreamTest").setMaster(args(0))
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    //sql context
    val session = SparkSession.builder().appName("sqlTest").enableHiveSupport().getOrCreate()
    //DF transform
    import session.implicits._

    //log level modify
    LogManager.getRootLogger.setLevel(Level.WARN)


    //logic start
    //-----raw Kafka
    val rawKafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:6667,127.0.0.1:6668", //"localhost:9092,anotherhost:9092"
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "raw_consumer_group", //use_a_separate_group_id_for_each_stream
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val rawTopics = Array("raw_test") //Array("topicA", "topicB")
    val rawKafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](rawTopics, rawKafkaParams)
    )

    //-----event Kafka
    val eventKafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:6667,127.0.0.1:6668", //"localhost:9092,anotherhost:9092"
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "event_consumer_group", //use_a_separate_group_id_for_each_stream
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val eventTopics = Array("event_test") //Array("topicA", "topicB")
    val eventKafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](eventTopics, eventKafkaParams)
    )

    /*
    //-----parsing raw Kafka
    val rStream: DStream[String] = rawKafkaStream.transform(t => {
      t.mapPartitions(m => {
        m.flatMap(f => {
          var array: Array[String] = Array()
          val json = new JSONObject(f.value)
          val newJson = new JSONObject()
          newJson.put("host", json.get("hosts"))
          for((value, index) <- json.getString("num").split(",").zipWithIndex){
            val resultJson = new JSONObject(newJson.toString)
            resultJson.put("cnt", value)
            array = array :+ resultJson.toString
          }
          array.iterator
        })
      })
    })

    //-----parsing event Kafka
    val eStream: DStream[String] = eventKafkaStream.transform(t => {
      t.mapPartitions(m => {
        m.map(f => {
          val json = new JSONObject(f.value)
          val newJson = new JSONObject()
          newJson.put("host", json.get("hosts"))
          newJson.toString
        })
      })
    })
    */

    //-----parsing raw Kafka
    val rStream: DStream[String] = rawKafkaStream.transform(t => {
      t.mapPartitions(m => {
        m.flatMap(f => {
          var array: Array[String] = Array()
          val json = new JSONObject(f.value)
          val newJson = new JSONObject()
          newJson.put("host", json.get("hosts"))
          for((value, index) <- json.getString("num").split(",").zipWithIndex){
            val resultJson = new JSONObject(newJson.toString)
            resultJson.put("cnt", value.toLong)
            array = array :+ resultJson.toString
          }
          array.iterator
        })
      })
    })
    //-----parsing event Kafka
    val eStream: DStream[String] = eventKafkaStream.transform(t => {
      t.mapPartitions(m => {
        m.map(f => {
          val json = new JSONObject(f.value)
          val newJson = new JSONObject()
          newJson.put("host", json.get("hosts"))
          newJson.toString
        })
      })
    })

    //-----processing
    val pStream: DStream[String] = rStream.transformWith(eStream, (rRdd: RDD[String],eRdd: RDD[String]) => {
      val test = eRdd.collect()
      rRdd.filter(f => {
        var res = false
        val rJson = new JSONObject(f)
        for((value,index) <- test.zipWithIndex) {
          if(rJson.get("host") equals new JSONObject(value).get("host")) res = true
        }
        res
      })
    })

    //-----processing
    val aggStream = pStream.repartition(1).transform(r=> {
      val res = if (!r.isEmpty()) {
        val df = session.read.json(r).toDF
        df.printSchema()
        df.createOrReplaceTempView("stream")
        val sql = session.sql("select host, count(cnt), max(cnt), min(cnt), avg(cnt) from stream group by host").toJSON.rdd
        sql
      }else {
        r
      }
      res
    })

    rStream.print(100)
    eStream.print(100)
    pStream.print(100)
    aggStream.print(100)

    /*
    rawKafkaStream.foreachRDD(f=>{println("\n-------------\nrawKafkaStream : " + f.count)})
    eventKafkaStream.foreachRDD(f=>{println("eventKafkaStream : " + f.count)})

    rStream.print(100)
    eStream.print(100)
    */

    //logic end

    streamingContext.start()
    streamingContext.awaitTermination()

    //streamingContext.stop()

  }
}
/* spark sql -> table does not exist
    //-----parsing event Kafka
    val eStream: DStream[String] = eventKafkaStream.transform(t => {
      val res = if(!t.isEmpty()) {
        val op = t.mapPartitions(m => {
          m.map(f => {
            val json = new JSONObject(f.value)
            val newJson = new JSONObject()
            newJson.put("hostEvent", json.get("hosts"))
            newJson.toString
          })
        })
        op.toDF().createOrReplaceTempView("event")
        op
      }else {
        t.map(x=>x.value)
      }
      res
    })

    val rStream: DStream[String] = rawKafkaStream.transform(t => {
      val res = if(!t.isEmpty()) {
        t.mapPartitions(m => {
          m.flatMap(f => {
            var array: Array[String] = Array()
            val json = new JSONObject(f.value)
            val newJson = new JSONObject()
            newJson.put("host", json.get("hosts"))
            for ((value, index) <- json.getString("num").split(",").zipWithIndex) {
              val resultJson = new JSONObject(newJson.toString)
              resultJson.put("cnt", value)
              try {
                if (session.catalog.tableExists("event")) {
                  val hosts = session.sql("select hostEvent from event")
                  //val hostsArray = hosts.collect
                }
              }catch{
                case e: Exception => {
                  println("table does not exist")
                }

              }
              array = array :+ resultJson.toString
            }
            array.iterator
          })
        })
      }else {
        t.map(x=>x.value)
      }
      res
    })
    */

/* error
 * Caused by: java.io.NotSerializableException: Graph is unexpectedly null when DStream is being serialized.
 * java.io.NotSerializableException: Object of org.apache.spark.streaming.kafka010.DirectKafkaInputDStream$DirectKafkaInputDStreamCheckpointData is being serialized
 *    possibly as a part of closure of an RDD operation.
 *    This is because  the DStream object is being referred to from within the closure.
 *    Please rewrite the RDD operation inside this DStream to avoid this.
 *    This has been enforced to avoid bloating of Spark tasks  with unnecessary objects.
 *
//-----parsing event Kafka
val eStream: DStream[String] = eventKafkaStream.transform(t => {
  t.mapPartitions(m => {
    m.map(f => {
      val json = new JSONObject(f.value)
      val newJson = new JSONObject()
      newJson.put("hostEvent", json.get("hosts"))
      val parsingStream = rStream.filter(x=> new JSONObject(x).get("host") equals json.get("hostEvent"))
      parsingStream.print(100)
      newJson.toString
    })
  })
})

//-----processing event Kafka
eStream.foreachRDD(r => {
  r.foreachPartition(p => {
    p.foreach(f => {
      val json = new JSONObject(f)
      println("\n------------\n" + json.toString)
      val parsingStream = rStream.filter(x=> new JSONObject(x).get("host") equals json.get("hostEvent"))
      parsingStream.repartition(1).print
    })
  })
})
*/
