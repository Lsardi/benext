


object AkkaProducer{
  import akka.actor._
  import com.rabbitmq.client.ConnectionFactory
  import scala.io.Source
  case class Send()

  class Produceur extends Actor {
    def receive = {


      case Send() => {

        val factory = new ConnectionFactory()
        factory.setHost("localhost")
        val connection = factory.newConnection()
        val channel = connection.createChannel()


        channel.queueDeclare("hello", false, false, false, null)
        val bufferedSource = Source.fromFile("/Users/user/Desktop/title.basics.tsv")

        for (line <- bufferedSource.getLines) {
          val message = line.split('\t')
          val genre = message(8).split(',')
          val titre = message(3)
          for(i <- genre){
            if (i == "Comedy"){
              val send = titre + "\t" + genre.mkString(", ")
              channel.basicPublish("", "hello", null, send.getBytes)

            }
          }
        }

        //val exchange = ""


        //println(s"sent message $message")

        channel.close()
        connection.close()
      }

    }
  }
  def main(args: Array[String]){
    val system = ActorSystem("match")
    val borg = system.actorOf(Props[Produceur], name ="My_produceur")
    borg ! Send()
  }}