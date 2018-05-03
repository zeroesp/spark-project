import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession

case class Apt(year_month : String, reg_cd : String, reg_nm : String, dong_nm : String, apart : String, space : String, price : String, built_year : String, year : String, month : String, day : String, land_num : String, reg_cd2 : String, floor : String)

object KafkaConsumeAndSQL {
  def main(args: Array[String]): Unit ={
    //Conf, Context
    val sparkConf = new SparkConf().setAppName("KafkacConsumer").setMaster(args(0))
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    val sqlContext = SparkSession.builder().appName("sqltest").getOrCreate()
    import sqlContext.implicits._

    //logic start
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092", //"localhost:9092,anotherhost:9092"
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer_group", //use_a_separate_group_id_for_each_stream
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("apart") //Array("topicA", "topicB")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    kafkaStream.foreachRDD(rdd => {
      println("* RDD size = " + rdd.count())
      //r.foreach(s => println(s))
      //sample Data : ConsumerRecord(topic = apart, partition = 8, offset = 9, CreateTime = 1525347287521, checksum = 1193147794, serialized key size = -1, serialized value size = 105, key = null, value = 201803|11110|서울특별시 종로구|창신동|창신쌍용2|79.87|46,200|1993|2018|3|11~20|703|11110|7)
      rdd.foreach(s => println(s.value))

      rdd.map(s => s.value().split("\\|"))
        .map(s => Apt(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10), s(11), s(12), s(13)))
        .toDF().createOrReplaceTempView("apt_table")

      sqlContext.sql("select * from apt_table").collect().foreach(println)

    })
    //logic end

    streamingContext.start()
    streamingContext.awaitTermination()

    //streamingContext.stop()
  }
}
