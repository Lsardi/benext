import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.ConnectionFactory

import scala.io.Source
import scala.util.Random


object RabbitMqProducer {
  def main(args: Array[String]) = {

    val factory = new ConnectionFactory()
    factory.setHost("localhost")
    val connection = factory.newConnection()
    val channel = connection.createChannel()

    val queueName = "hello"
    val durable = false
    val exclusive = false
    val autoDelete = false
    val arguments = null
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments)
    val bufferedSource = Source.fromFile("/Users/user/Desktop/title.basics.tsv")
    for (line <- bufferedSource.getLines) {
      val message = line.split('\t')
    val genre = message(8).split(',')
      for(i <- genre){
        if (i == "Comedy"){
          channel.basicPublish("", queueName, null, line.getBytes)

        }
      }
    }

    //val exchange = ""


    //println(s"sent message $message")

    channel.close()
    connection.close()
  }
}
