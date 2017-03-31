/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Channel, DefaultConsumer, Envelope, ShutdownSignalException}

import scala.collection.mutable

final class CommittableMessage[T](val bytes: T,
                               val envelope: Envelope,
                               val properties: BasicProperties,
                               channel: Channel) {

  def commitScalaDsl(): Unit =
    channel.basicAck(envelope.getDeliveryTag, false)
}

object CommittableMessage {
  def apply[T](bytes: T, envelope: Envelope, properties: BasicProperties, channel: Channel): CommittableMessage[T] =
    new CommittableMessage(bytes, envelope, properties, channel)
}

final class AmqpCommittableSourceStage(settings: AmqpSourceSettings, bufferSize: Int)
    extends GraphStage[SourceShape[CommittableMessage[ByteString]]]
    with AmqpConnector { stage =>

  val out = Outlet[CommittableMessage[ByteString]]("AmqpCommittableSource.out")

  override def shape: SourceShape[CommittableMessage[ByteString]] = SourceShape.of(out)

  override protected def initialAttributes: Attributes = Attributes.name("AmqpCommittableSource")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with AmqpConnectorLogic {

      override val settings = stage.settings

      override def connectionFactoryFrom(settings: AmqpConnectionSettings) = stage.connectionFactoryFrom(settings)

      private val queue = mutable.Queue[CommittableMessage[ByteString]]()

      override def whenConnected(): Unit = {
        import scala.collection.JavaConverters._
        // we have only one consumer per connection so global is ok
        channel.basicQos(bufferSize, true)
        val consumerCallback = getAsyncCallback(handleDelivery)
        val shutdownCallback = getAsyncCallback[Option[ShutdownSignalException]] {
          case Some(ex) => failStage(ex)
          case None => completeStage()
        }

        val amqpSourceConsumer = new DefaultConsumer(channel) {
          override def handleDelivery(consumerTag: String,
                                      envelope: Envelope,
                                      properties: BasicProperties,
                                      body: Array[Byte]): Unit =
            consumerCallback.invoke(CommittableMessage(ByteString(body), envelope, properties, channel))

          override def handleCancel(consumerTag: String): Unit =
            // non consumer initiated cancel, for example happens when the queue has been deleted.
            shutdownCallback.invoke(None)

          override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
            // "Called when either the channel or the underlying connection has been shut down."
            shutdownCallback.invoke(Option(sig))
        }

        def setupNamedQueue(settings: NamedQueueSourceSettings): Unit =
          channel.basicConsume(
            settings.queue,
            false, // never auto-ack
            settings.consumerTag, // consumer tag
            settings.noLocal,
            settings.exclusive,
            settings.arguments.asJava,
            amqpSourceConsumer
          )

        def setupTemporaryQueue(settings: TemporaryQueueSourceSettings): Unit = {
          // this is a weird case that required dynamic declaration, the queue name is not known
          // up front, it is only useful for sources, so that's why it's not placed in the AmqpConnectorLogic
          val queueName = channel.queueDeclare().getQueue
          channel.queueBind(queueName, settings.exchange, settings.routingKey.getOrElse(""))
          channel.basicConsume(
            queueName,
            amqpSourceConsumer
          )
        }

        settings match {
          case settings: NamedQueueSourceSettings => setupNamedQueue(settings)
          case settings: TemporaryQueueSourceSettings => setupTemporaryQueue(settings)
        }

      }

      def handleDelivery(message: CommittableMessage[ByteString]): Unit =
        if (isAvailable(out)) {
          push(out, message)
        } else {
          if (queue.size + 1 > bufferSize) {
            failStage(new RuntimeException(s"Reached maximum buffer size $bufferSize"))
          } else {
            queue.enqueue(message)
          }
        }

      setHandler(out,
        new OutHandler {
        override def onPull(): Unit =
          if (queue.nonEmpty) {
            push(out, queue.dequeue())
          }

      })
    }
}
