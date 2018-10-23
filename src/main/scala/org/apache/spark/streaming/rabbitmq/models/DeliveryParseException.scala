package org.apache.spark.streaming.rabbitmq.models

final case class DeliveryParseException(private val message: String = "",
                                        private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
