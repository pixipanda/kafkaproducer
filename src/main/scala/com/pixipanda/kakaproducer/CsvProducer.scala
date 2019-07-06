package com.pixipanda.kakaproducer

import java.io.{BufferedReader, FileReader}
import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


class  CsvProducer(brokers:String, topic:String) {

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


  def publish(file:String): Unit =  {

    val rand: Random = new Random
    val br = new BufferedReader(new FileReader(file))

    var line = br.readLine()
    println("line: " + line)
    while(line != null) {
      val producerRecord = new ProducerRecord[String, String](topic, line)
      producer.send(producerRecord)
      line = br.readLine()
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)

      println("line: " + line)
    }
    br.close()
    producer.close()
  }
}


object CsvProducer {

  def main(args: Array[String]) {

    if (args.length != 2) {
      println("Please provide command line arguments: topic and transaction file path")
      System.exit(-1)
    }

    val topic = args(0)
    val transactionfile = args(1)


    val csvProducer = new CsvProducer("localhost:9092", topic)
    csvProducer.publish(transactionfile)
  }
}