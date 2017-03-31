/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.javadsl

import akka.NotUsed
import akka.stream.alpakka.amqp._
import akka.stream.javadsl.Source
import akka.util.ByteString

object AmqpCommittableSource {

  /**
   * Java API: Creates an [[AmqpSource]] with given settings and buffer size.
   */
  def create(settings: AmqpSourceSettings, bufferSize: Int): Source[CommittableMessage[ByteString], NotUsed] =
    Source.fromGraph(new AmqpCommittableSourceStage(settings, bufferSize))

}
