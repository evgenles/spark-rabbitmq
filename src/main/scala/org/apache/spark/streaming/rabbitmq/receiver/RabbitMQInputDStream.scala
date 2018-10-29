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
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.rabbitmq.ConfigParameters
import org.apache.spark.streaming.rabbitmq.consumer.Consumer
import org.apache.spark.streaming.rabbitmq.consumer.Consumer._
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.streaming.rabbitmq.logsender.RabbitMqLogSender
import org.apache.spark.streaming.rabbitmq.models.DeliveryParseException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util._

private[rabbitmq]
class RabbitMQInputDStream[R: ClassTag](
                                         @transient ssc: StreamingContext,
                                         params: Map[String, String],
                                         messageHandler: Delivery => R,
                                         tag: String = null,
                                         isLogSenderEnable: Boolean = false,
                                         logSenderParam: Map[String, String] = null
                                       ) extends ReceiverInputDStream[R](ssc) with Logging {
  var appId = if (ssc != null && ssc.sparkContext != null) ssc.sparkContext.applicationId else "Unknown"
  private val storageLevelParam =
    params.getOrElse(ConfigParameters.StorageLevelKey, ConfigParameters.DefaultStorageLevel)

  override def getReceiver(): Receiver[R] = {
    new RabbitMQReceiver[R](appId, params, StorageLevel.fromString(storageLevelParam), messageHandler, tag, isLogSenderEnable, logSenderParam)
  }
}

private[rabbitmq]
class RabbitMQReceiver[R: ClassTag](
                                     applicationId: String,
                                     params: Map[String, String],
                                     storageLevel: StorageLevel,
                                     messageHandler: Delivery => R,
                                     tag: String = null,
                                     isLogSenderEnable: Boolean = false,
                                     logSenderParam: Map[String, String] = null
                                   )
  extends Receiver[R](storageLevel) with Logging {
  private[rabbitmq] var logSender: RabbitMqLogSender = null

  @transient var _consumer: Consumer = _

  private var _blockGenerator: BlockGenerator = null

  def blockGenerator: BlockGenerator = _blockGenerator

  def consumer: Consumer =
    _consumer

  def onStart() {
    if(_blockGenerator != null) _blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)
    _blockGenerator.start()
    implicit val akkaSystem = akka.actor.ActorSystem()
    if (isLogSenderEnable && logSender == null) {
      logSender = new RabbitMqLogSender(logSenderParam)
      log.info("LogSender created")
    }
    Try {
      _consumer = Consumer(params, tag)

      if (getFairDispatchFromParams(params))
        _consumer.setFairDispatchQoS(getPrefetchCountFromParams(params))

      _consumer.setQueue(params)

      (_consumer, _consumer.startConsumer)
    } match {
      case Success((_consumer, queueConsumer)) =>
        log.info("onStart, Connecting..")
        new Thread() {
          override def run() {
            receive(_consumer, queueConsumer)
          }
        }.start()
      case Failure(f) =>
        log.error("Could not connect"); restart("Could not connect", f)
    }
  }

  def onStop() {
    Consumer.closeConnections()
    log.info("Closed all RabbitMQ connections")
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive(consumer: Consumer, queueConsumer: QueueingConsumer) {
    log.info("Receive started, isLogEnable" + isLogSenderEnable + " logSender == null " + (logSender == null))

    try {
      log.info("RabbitMQ consumer start consuming data")
      while (!isStopped() && consumer.channel.isOpen) {
        Try(queueConsumer.nextDelivery())
        match {
          case Success(delivery) =>
            processDelivery(consumer, delivery)
          case Failure(e) => {
            if (isLogSenderEnable) {
              try {
                val jsonLog = s"""{"Exception":"${e.toString}", "Comment":"Cannot get next delivery", "ApplicationId":"${applicationId}","InstantId":"$tag"}"""
                logSender.Publish(jsonLog.getBytes())
              }
              catch {
                case unknown: Throwable =>
                  log.error("Got this unknown exception: " + unknown, unknown)
                case exception: Exception =>
                  log.error("Got this Exception: " + exception, exception)
              }
            }
            throw new Exception(s"An error happen while getting next delivery: ${e.getLocalizedMessage}", e)
          }
        }
      }
    } catch {
      case unknown: Throwable =>
        log.error("Got this unknown exception: " + unknown, unknown)
      case exception: Exception =>
        log.error("Got this Exception: " + exception, exception)
    }
    finally {
      log.info("it has been stopped")
      try {
        consumer.close()
      } catch {
        case e: Throwable =>
          log.error(s"error on close consumer, ignoring it : ${e.getLocalizedMessage}", e)
      }
      restart("Trying to connect again")
    }
  }

  private def processDelivery(consumer: Consumer, delivery: Delivery) {
    try {
     _blockGenerator.addData(messageHandler(delivery))
      //Send ack if not set the auto ack property
      if (sendingBasicAckFromParams(params))
        consumer.sendBasicAck(delivery)
      if (isLogSenderEnable) {
        try {
          val jsonLog = s"""{"SuccessDelivery":${new String(delivery.getBody(), "UTF-8")},"ApplicationId":"${applicationId}","InstantId":"$tag"}"""
          logSender.Publish(jsonLog.getBytes())
        }
        catch {
          case unknown: Throwable =>
            log.error("Got this unknown exception: " + unknown, unknown)
          case exception: Exception =>
            log.error("Got this Exception: " + exception, exception)
        }
      }
    }
    catch {
      case jsonE: DeliveryParseException =>
        {
          consumer.sendBasicAck(delivery)
          RabbitLogFail(delivery, jsonE)
        }
      case e: Exception =>

        //Send noack if not set the auto ack property
        if (sendingBasicAckFromParams(params)) {
          log.warn(s"failed to process message. Sending noack ...", e)
          consumer.sendBasicNAck(delivery)
          RabbitLogFail(delivery, e)
        }
    }
  }
  private def StoreBlock( blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit ={
    var count = 0
    var pushed = false
    var exception: Exception = null
    while (!pushed && count <= 3) {
      try {
        store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[R]])
        pushed = true
      } catch {
        case ex: Exception =>
          count += 1
          exception = ex
      }
    }
    if (pushed) {
      logInfo("block " + blockId + " pushed " + arrayBuffer.length + "messages")
    } else {
      stop("Error while storing block into Spark", exception)
    }
  }


  private def RabbitLogFail(delivery: Delivery, e: Throwable) {
    if (isLogSenderEnable) {
      try {
        val jsonLog = s"""{"FailedDelivery":"${new String(delivery.getBody(), "UTF-8").replaceAll("\"", "\\\\\"")}","ApplicationId":"${applicationId}","InstantId":"$tag", "Exception" : "${e.toString}", "Comment":"Cannot process delivery" }"""
        logSender.Publish(jsonLog.getBytes())
      }
      catch {
        case unknown: Throwable =>
          log.error("Got this unknown exception: " + unknown, unknown)
        case exception: Exception =>
          log.error("Got this Exception: " + exception, exception)
      }
    }
  }

  private final class GeneratedBlockHandler
    extends BlockGeneratorListener {


    def onAddData(data: Any, metadata: Any): Unit = {
    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {
      StoreBlock(blockId, arrayBuffer)
    }

    def onError(message: String, throwable: Throwable): Unit = {
      log.error("[GHB] "+ message, throwable)
      if (isLogSenderEnable) {
        val jsonLog = s"""{"BlockErrorMessage":"${message}","ApplicationId":"${applicationId}","InstantId":"$tag", "Exception" : "${throwable.toString}", "Comment":"Error while putting block" }"""
        if (isLogSenderEnable) logSender.Publish(jsonLog.getBytes())
      }
    }
  }
}
