package  org.apache.spark.streaming.rabbitmq.logsender

import com.rabbitmq.client.ConnectionFactory

private[rabbitmq]
class RabbitMqLogSender (
                      params: Map[String, String]
                      ) {
  val connectionFactory = new ConnectionFactory
  connectionFactory.setHost(params.get("host").getOrElse(""))
  connectionFactory.setUsername(params.get("username").getOrElse(""))
  connectionFactory.setPassword(params.get("password").getOrElse(""))
  connectionFactory.setVirtualHost(params.get("virtualHost").getOrElse(""))
  private[rabbitmq] var exchangeName = params.get("exchange").getOrElse("")
  private[rabbitmq] var routingKey = params.get("routingKey").getOrElse("")

  private[rabbitmq] var connection = connectionFactory.newConnection()

  private[rabbitmq] var channel = connection.createChannel()

  def Publish(serializedObj: Array[Byte]): Unit = {
    channel.basicPublish(exchangeName, "", null, serializedObj)
  }

}