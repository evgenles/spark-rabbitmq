package org.apache.spark.streaming.rabbitmq.receiver

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.BlockGeneratorListener

import scala.collection.mutable.ArrayBuffer

private final class GeneratedBlockHandler
  extends BlockGeneratorListener {


  def onAddData(data: Any, metadata: Any): Unit = {
    //      consumer.finish(data.asInstanceOf[NSQMessageWrapper].getMessage)
  }

  def onGenerateBlock(blockId: StreamBlockId): Unit = {
  }

  def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {
    // Store block and commit the blocks offset
    super.onPushBlock(blockId,arrayBuffer)
  }

  def onError(message: String, throwable: Throwable): Unit = {
    reportError(message, throwable)
  }
}