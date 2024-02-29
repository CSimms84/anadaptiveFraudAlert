package frauddetectionservice

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, ByteArrayDeserializer}
import java.time.Duration
import java.util.Properties
import scala.sys.exit
import scala.util.Using

object Main extends App {
  val topic = "orders"
  val groupID = "frauddetectionservice"
  val props = new Properties()

  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)

  val bootstrapServers = sys.env.get("KAFKA_SERVICE_ADDR") match {
    case Some(addr) => addr
    case None =>
      println("KAFKA_SERVICE_ADDR is not supplied")
      exit(1)
  }

  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  val consumer = new KafkaConsumer[String, Array[Byte]](props)
  consumer.subscribe(java.util.Arrays.asList(topic))

  var totalCount = 0L

  Using.resource(consumer) { consumer =>
    while (true) {
      val records = consumer.poll(Duration.ofMillis(100))
      records.forEach { record =>
        totalCount += 1
        val orders = OrderResult.parseFrom(record.value()) // Assuming OrderResult.parseFrom is available
        println(s"Consumed record with orderId: ${orders.orderId}, and updated total count to: $totalCount")
      }
    }
  }
}
