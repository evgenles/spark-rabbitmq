package  org.apache.spark.streaming.rabbitmq.logsender

import com.rabbitmq.client.{Channel, ConnectionFactory}
import org.apache.log4j.Logger

class RabbitMqLogSender(params: Map[String, String]) {
  val logger = Logger.getLogger(this.getClass.getName)
  val connectionFactory = new ConnectionFactory
  connectionFactory.setHost(params.get("hosts").getOrElse(""))
  connectionFactory.setUsername(params.get("userName").getOrElse(""))
  connectionFactory.setPassword(params.get("password").getOrElse(""))
  connectionFactory.setVirtualHost(params.get("virtualHost").getOrElse(""))
  connectionFactory.setPort(params.get("port").getOrElse("5672").toInt)
  logger.info("ConnectionFactory: Host-> " + connectionFactory.getHost + " Login-> " +connectionFactory.getUsername +" Pass -> " + connectionFactory.getPassword + " vHost -> " +connectionFactory.getVirtualHost +" port -> " + connectionFactory.getPort)

  var exchangeName = params.get("exchange").getOrElse("")
  logger.info("Exchange name: " + exchangeName)

  var routingKey = params.get("routingKey").getOrElse("")
  logger.info("Routing key: " + routingKey)

  var channel : Channel = null
  try {
    val connection = connectionFactory.newConnection()
    channel = connection.createChannel()
    logger.info("Chanel created")
  }
  catch {
    case ex: Exception => logger.error(ex)
  }
  def Publish(serializedObj: Array[Byte]): Unit = {
    if(channel!=null) channel.basicPublish(exchangeName, routingKey, null, serializedObj)
  }

}