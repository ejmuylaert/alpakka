package akka.stream.alpakka.amqp.javadsl

import akka.NotUsed
import akka.stream.alpakka.amqp._
import akka.stream.javadsl.Source

object AmqpCommittableSource {

  /**
    * Java API: Creates an [[AmqpSource]] with given settings and buffer size.
    */
  def create(settings: AmqpSourceSettings, bufferSize: Int): Source[CommittableMessage, NotUsed] =
    Source.fromGraph(new AmqpCommittableSourceStage(settings, bufferSize))

}
