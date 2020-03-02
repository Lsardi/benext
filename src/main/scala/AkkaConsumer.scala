import com.rabbitmq.client.{CancelCallback, ConnectionFactory, DeliverCallback}
import akka.actor._

case class Take()
object AkkaConsumer {

  class Consumer extends Actor {
     def receive = {
       case Take() => {
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
  }



  def main(args: Array[String]) = {
    val system = ActorSystem("Consumer")
    val consumer = system.actorOf(Props[Consumer], name ="My_Consumer")

    consumer ! Take()
  }


}
