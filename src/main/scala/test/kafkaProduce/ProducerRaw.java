package test.kafkaProduce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Date;

public class ProducerRaw {
  // compile : /usr/jdk64/jdk1.8.0_112/bin/javac -cp .:kafka-clients-1.0.0.jar:slf4j-api-1.7.25.jar ProducerRaw.java
  // execute : /usr/jdk64/jdk1.8.0_112/bin/java -cp .:kafka-clients-1.0.0.jar:slf4j-api-1.7.25.jar ProducerRaw 10000
  // kafka consumer : /usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh --bootstrap-server hdp03.my.pe.kr:6667 --topic test11

  public static void main(String[] args) {
    boolean result = false;

    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", "127.0.0.1:6667");
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer< String, String> kafkaProcuder = new KafkaProducer<String, String>(kafkaProps);
    int num = Integer.parseInt(args[0]);
    System.out.println("num : " + num);

    String[] ips = {"101","12","131","143","112","51","74","36","29","181"};
    String[] hosts = {"poc","stg","dev","op","test"};

    try {
      System.out.println("start");
      for (int i = 0; i < num; i++) {
        String msg = "{\"message\":" + i + ",\"hosts\":\"" + hosts[(int)Math.floor(Math.random()*4.9)] + "\",\"num\":\""
            + ips[(int)Math.floor(Math.random()*9.9)] + "," + ips[(int)Math.floor(Math.random()*9.9)] + "\"}";
        kafkaProcuder.send(new ProducerRecord<String, String>("raw_test", msg));
        System.out.println("i : " + i + ", Sent:" + msg);
        Thread.sleep(900);
      }
      result = true;
    }catch(Exception e){
      e.printStackTrace();
    }finally {
      kafkaProcuder.close();
    }

    System.out.println("end");
  }

}
