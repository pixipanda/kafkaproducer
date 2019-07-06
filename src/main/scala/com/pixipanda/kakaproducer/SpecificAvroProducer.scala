package com.pixipanda.kakaproducer

import java.io.{BufferedReader, FileReader}
import java.util.{Properties, Random}

import com.pixipanda.avro.Transaction
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


class  SpecificAvroProducer(brokers: String, topic: String) {

  val props = createProducerConfig(brokers)
  val producer = new KafkaProducer[String, Transaction](props)


  def parse(line: String) = {
    val fields = line.split(",")
    val cc_num = fields(0)
    val transId = fields(1)
    val transTime = fields(2)
    val category = fields(3)
    val merchant = fields(4)
    val amt = fields(5).toDouble
    val lat = fields(6).toDouble
    val long = fields(7).toDouble
    new Transaction(cc_num, transId, transTime, category, merchant, amt, lat, long)
  }

  def createProducerConfig(brokers: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    /*    props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);*/
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", "http://localhost:8081")
    //props.put("auto.register.schemas", "false")
    props
  }


  def publish(file: String) {
    val rand: Random = new Random
    val br = new BufferedReader(new FileReader(file))

    var line = br.readLine()
    while (line != null) {
      var transaction = parse(line)
      val producerRecord = new ProducerRecord[String, Transaction](topic, transaction)
      producer.send(producerRecord)
      line = br.readLine()
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)

      println("transaction: " + transaction)
    }
    br.close()
    producer.close()
  }

}


object SpecificAvroProducer {

  def main(args: Array[String]) {

    if (args.length != 2) {
      println("Please provide command line arguments: topic and transaction file path and schem")
      System.exit(-1)
    }

    val topic = args(0)
    val transactionfile = args(1)
    val specificAvroProducer = new SpecificAvroProducer("localhost:9092", topic)
    specificAvroProducer.publish(transactionfile)
  }
}