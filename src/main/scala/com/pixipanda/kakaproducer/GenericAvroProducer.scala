package com.pixipanda.kakaproducer

import java.io.{BufferedReader, FileReader}
import java.util.{Properties, Random}

import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


class GenericAvroProducer(brokers:String, topic:String, schemaRegistryURL: String) {

  val props = createProducerConfig(brokers, schemaRegistryURL)
  val producer = new KafkaProducer[String, GenericRecord](props)
  val schema = getSchema(schemaRegistryURL, topic)



  def getSchema(schemaRegistryURL: String, topic:String) =  {

    val subject = topic + "-value"
    val restService:RestService = new RestService(schemaRegistryURL)
    val parser = new Schema.Parser()
    val schema = parser.parse(restService.getLatestVersion(subject).getSchema)
    schema
  }


  def createGenericRecord(line: String) = {

    val transactionGenericRecord = new GenericData.Record(schema)
    val fields = line.split(",")
    transactionGenericRecord.put("cc_num", fields(0))
    transactionGenericRecord.put("transId", fields(1))
    transactionGenericRecord.put("transTime", fields(2))
    transactionGenericRecord.put("category", fields(3))
    transactionGenericRecord.put("merchant", fields(4))
    transactionGenericRecord.put("amt", fields(5).toDouble)
    transactionGenericRecord.put("merchLatitude", fields(6).toDouble)
    transactionGenericRecord.put("merchLongitude", fields(7).toDouble)

    transactionGenericRecord
  }

 def createProducerConfig(brokers: String, schemaRegistryURL:String) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    /*    props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);*/
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", schemaRegistryURL)
    //props.put("auto.register.schemas", "false")
    props
  }


  def publish(file: String) {
    val rand: Random = new Random
    val br = new BufferedReader(new FileReader(file))

    var line = br.readLine()
    while (line != null) {
      var genericRecord = createGenericRecord(line)
      val producerRecord = new ProducerRecord[String, GenericRecord](topic, genericRecord)
      producer.send(producerRecord)
      line = br.readLine()
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)

      println("genericRecord: " + genericRecord)
    }
    br.close()
    producer.close()
  }

}

object GenericAvroProducer {

  def main(args: Array[String]) {

      if (args.length != 3) {
        println("Please provide command line arguments: topic, transaction file path and schemaRegistryURL")
        System.exit(-1)
      }

      val topic = args(0)
      val transactionfile = args(1)
      val schemaRegistryURL = args(2)

      println("topic: " + topic, " transactionfile: " + transactionfile + " schemaRegistryURL: " + schemaRegistryURL)
      val genericAvroProducer = new GenericAvroProducer("localhost:9092", topic, schemaRegistryURL)
      genericAvroProducer.publish(transactionfile)
    }
}