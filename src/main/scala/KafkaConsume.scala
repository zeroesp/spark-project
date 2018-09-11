import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object ALogger extends Serializable {
  val log = Logger.getRootLogger
}

object KafkaConsume {
  def main(args: Array[String]): Unit ={
    //Conf, Context
    val sparkConf = new SparkConf().setAppName("KafkacConsumer").setMaster(args(0))
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    //logic start
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.250.64.118:6667", //"localhost:9092,anotherhost:9092"
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer_group", //use_a_separate_group_id_for_each_stream
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    ALogger.log.info("-------123123123123123")

    val topics = Array("test13") //Array("topicA", "topicB")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    kafkaStream.foreachRDD(r => {
      println("* RDD size = " + r.count())
      //r.foreach(s => println(s))
      //sample Data : ConsumerRecord(topic = apart, partition = 8, offset = 9, CreateTime = 1525347287521, checksum = 1193147794, serialized key size = -1, serialized value size = 105, key = null, value = 201803|11110|서울특별시 종로구|창신동|창신쌍용2|79.87|46,200|1993|2018|3|11~20|703|11110|7)
      //r.foreach(s => println(s.value))
      r.mapPartitions{ rows =>
        var mapRes: List[String] = null
        mapRes = rows.toList.map{ row =>
          println("rows : " + row.value())
          ALogger.log.info("A row : " + row.value())
          row.toString()
        }.filter(_ != "{}")
        mapRes.iterator
      }
      //if (r.count() > 0) {
      //  println("* " + r.getNumPartitions + " partitions")
      //  r.glom().foreach(a => println("* partition size = " + a.size))
      //}
    })
    //logic end

    streamingContext.start()
    streamingContext.awaitTermination()

    //streamingContext.stop()
  }
}
