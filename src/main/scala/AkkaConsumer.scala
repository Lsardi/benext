import com.rabbitmq.client.{CancelCallback, ConnectionFactory, DeliverCallback}
case class receive()
object AkkaConsumer {

  def main(args: Array[String]) = {
    val QUEUE_NAME = "hello"

    val factory = new ConnectionFactory()
    factory.setHost("localhost")

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.queueDeclare(QUEUE_NAME, false, false, false, null)

    println(s"waiting for messages on $QUEUE_NAME")

    val callback: DeliverCallback = (consumerTag, delivery) => {
      val message = new String(delivery.getBody, "UTF-8")
      println(s" $message")
    }

    val cancel: CancelCallback = consumerTag => {}

    val autoAck = true
    channel.basicConsume(QUEUE_NAME, autoAck, callback, cancel)

    while(true) {
      Thread.sleep(1000)
    }

    channel.close()
    connection.close()
  }


}
