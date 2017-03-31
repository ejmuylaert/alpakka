/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.scaladsl

import akka.NotUsed
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.Source
import akka.util.ByteString

object AmqpCommittableSource {

  /**
   * Scala API: Creates an [[AmqpSource]] with given settings and buffer size.
   */
  def apply(settings: AmqpSourceSettings, bufferSize: Int): Source[CommittableMessage[ByteString], NotUsed] =
    Source.fromGraph(new AmqpCommittableSourceStage(settings, bufferSize))

}
