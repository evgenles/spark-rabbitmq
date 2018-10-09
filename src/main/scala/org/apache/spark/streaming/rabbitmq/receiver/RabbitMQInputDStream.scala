/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.rabbitmq.receiver

import com.rabbitmq.client.QueueingConsumer.Delivery
import com.rabbitmq.client._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.rabbitmq.ConfigParameters
import org.apache.spark.streaming.rabbitmq.consumer.Consumer
import org.apache.spark.streaming.rabbitmq.consumer.Consumer._
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.ClassTag
import scala.util._

private[rabbitmq]
class RabbitMQInputDStream[R: ClassTag](
                                         @transient ssc_ : StreamingContext,
                                         params: Map[String, String],
                                         messageHandler: Delivery => R,
                                         tag: String = null
                                       ) extends ReceiverInputDStream[R](ssc_) with Logging {

  private val storageLevelParam =
    params.getOrElse(ConfigParameters.StorageLevelKey, ConfigParameters.DefaultStorageLevel)

  override def getReceiver(): Receiver[R] = {
    System.out.println("---get-reciver")
    new RabbitMQReceiver[R](params, StorageLevel.fromString(storageLevelParam), messageHandler, tag)

  }
}

private[rabbitmq]
class RabbitMQReceiver[R: ClassTag](
                                     params: Map[String, String],
                                     storageLevel: StorageLevel,
                                     messageHandler: Delivery => R,
                                     tag: String = null
                                   )
  extends Receiver[R](storageLevel) with Logging {

  def onStart() {
    System.out.println("---reciver-on-start")

    implicit val akkaSystem = akka.actor.ActorSystem()

    Try {
      val consumer = Consumer(params, tag)

      if (getFairDispatchFromParams(params))
        consumer.setFairDispatchQoS(getPrefetchCountFromParams(params))

      consumer.setQueue(params)

      (consumer, consumer.startConsumer)
    } match {
      case Success((consumer, queueConsumer)) =>
        System.out.println("onStart, Connecting..")
        new Thread() {
          override def run() {
            receive(consumer, queueConsumer)
          }
        }.start()
      case Failure(f) =>
        System.out.println("Could not connect"); restart("Could not connect", f)
    }
    System.out.println("---reciver-on-start-end")
    
  }

  def onStop() {
    Consumer.closeConnections()
    System.out.println("Closed all RabbitMQ connections")
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive(consumer: Consumer, queueConsumer: QueueingConsumer) {
    System.out.println("---reciver-receive")

    try {
      System.out.println("RabbitMQ consumer start consuming data")
      while (!isStopped() && consumer.channel.isOpen) {
        
        Try(queueConsumer.nextDelivery())
        match {
          case Success(delivery) =>
            processDelivery(consumer, delivery)
          case Failure(e) =>
            throw new Exception(s"An error happen while getting next delivery: ${e.getLocalizedMessage}", e)
        }
      }
    } catch {
      case unknown: Throwable =>
        System.out.println("Got this unknown exception: " + unknown, unknown)
      case exception: Exception =>
        System.out.println("Got this Exception: " + exception, exception)
    }
    finally {
      System.out.println("it has been stopped")
      try {
        consumer.close()
      } catch {
        case e: Throwable =>
          System.out.println(s"error on close consumer, ignoring it : ${e.getLocalizedMessage}", e)
      }
      restart("Trying to connect again")
    }
    System.out.println("---reciver-receive-end")

  }

  private def processDelivery(consumer: Consumer, delivery:Delivery) {
    System.out.println("---reciver-process-delivery")

    Try(store(messageHandler(delivery)))
    match {
      case Success(data) =>
        //Send ack if not set the auto ack property
        if (sendingBasicAckFromParams(params))
          consumer.sendBasicAck(delivery)
      case Failure(e) =>
        //Send noack if not set the auto ack property
        if (sendingBasicAckFromParams(params)) {
          System.out.println(s"failed to process message. Sending noack ...", e)
          consumer.sendBasicNAck(delivery)
        }
    }
    System.out.println("---reciver-process-delivery-end")

  }
}
