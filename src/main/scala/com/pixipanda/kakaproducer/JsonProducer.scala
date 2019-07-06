package com.pixipanda.kakaproducer

import java.io.{BufferedReader, FileReader}
import java.util.{Properties, Random}

import com.google.gson.{Gson, JsonObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


class  JsonProducer(brokers:String, topic:String) {

  val props = createProducerConfig(brokers)
  val producer = new KafkaProducer[String, String](props)

  def createProducerConfig(brokers: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    /*    props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);*/
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }


  def publish(file:String) = {

    val gson: Gson = new Gson
    val rand: Random = new Random
    val br = new BufferedReader(new FileReader(file))

    var line = br.readLine()

    while (line != null) {

      val fields = line.split(",")
      val obj: JsonObject = new JsonObject

      obj.addProperty("cc_num", fields(0))
      obj.addProperty("transId", fields(1))
      obj.addProperty("transTime", fields(2))
      obj.addProperty("category", fields(3))
      obj.addProperty("merchant", fields(4))
      obj.addProperty("amt", fields(5))
      obj.addProperty("merchLatitude", fields(6))
      obj.addProperty("merchLongitude", fields(7))

      val json: String = gson.toJson(obj)
      val producerRecord = new ProducerRecord[String, String](topic, json)
      producer.send(producerRecord)
      line = br.readLine()
      println("Transaction Record: " + json)
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)
    }
    br.close()
    producer.close()
  }
}

object  JsonProducer {

  def main(args: Array[String]) {

    if (args.length != 2) {
      println("Please provide command line arguments: topic and transaction file path")
      System.exit(-1)
    }

    val topic = args(0)
    val transactionfile = args(1)


    val jsonProducer = new JsonProducer("localhost:9092", topic)
    jsonProducer.publish(transactionfile)
  }
}