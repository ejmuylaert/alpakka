/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.scaladsl

import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.util.ByteString

class AmqpCommittableSourceSpec extends AmqpSpec {

  "The Committable AMQP Source" should {

    "produce committable messages" in {
      val queueName = "amqp-conn-it-spec-work-queues-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(DefaultAmqpConnection).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )
      val amqpSource = AmqpCommittableSource(
        NamedQueueSourceSettings(DefaultAmqpConnection, queueName).withDeclarations(queueDeclaration),
        1
      )
      val subscriber = TestSubscriber.probe[CommittableMessage]()
      amqpSource.runWith(Sink.fromSubscriber(subscriber))
      subscriber.ensureSubscription()

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)

      subscriber.request(4)
      subscriber.expectNext().bytes.utf8String shouldEqual "one"
    }

    "non commitable source acks requested messages" in {
      val queueName = "amqp-conn-it-spec-work-queues-2" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(DefaultAmqpConnection).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )
      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(DefaultAmqpConnection, queueName).withDeclarations(queueDeclaration),
        1
      )
      val subscriber = TestSubscriber.probe[IncomingMessage]()
      amqpSource.runWith(Sink.fromSubscriber(subscriber))
      subscriber.ensureSubscription()

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)

      subscriber.request(4)
      subscriber.expectNext().bytes.utf8String shouldEqual "one"

      val subscriberNext = TestSubscriber.probe[IncomingMessage]()
      amqpSource.runWith(Sink.fromSubscriber(subscriberNext))
      subscriberNext.ensureSubscription()
      subscriber.request(1)
      subscriber.expectNext().bytes.utf8String shouldEqual "two"
    }

    "should redeliver when message is not commited" in {
      val queueName = "amqp-conn-it-spec-work-queues-3" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(DefaultAmqpConnection).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )
      val amqpSource = AmqpCommittableSource(
        NamedQueueSourceSettings(DefaultAmqpConnection, queueName).withDeclarations(queueDeclaration),
        1
      )
      val subscriber = TestSubscriber.probe[CommittableMessage]()
      amqpSource.runWith(Sink.fromSubscriber(subscriber))
      subscriber.ensureSubscription()

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)

      subscriber.request(4)
      subscriber.expectNext().bytes.utf8String shouldEqual "one"
      subscriber.cancel()

      val subscriberNext = TestSubscriber.probe[CommittableMessage]()
      amqpSource.runWith(Sink.fromSubscriber(subscriberNext))
      subscriberNext.ensureSubscription()
      subscriberNext.request(4)
      subscriberNext.expectNext().bytes.utf8String shouldEqual "one"
    }

    "should deliver next message when commited" in {
      val queueName = "amqp-conn-it-spec-work-queues-3" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(DefaultAmqpConnection).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )
      val amqpSource = AmqpCommittableSource(
        NamedQueueSourceSettings(DefaultAmqpConnection, queueName).withDeclarations(queueDeclaration),
        1
      )
      val subscriber = TestSubscriber.probe[CommittableMessage]()
      amqpSource
        .map(msg => {
          msg.commitScalaDsl()
          msg
        })
        .runWith(Sink.fromSubscriber(subscriber))
      subscriber.ensureSubscription()

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)

      subscriber.request(4)
      subscriber.expectNext().bytes.utf8String shouldEqual "one"
      subscriber.cancel()

      val subscriberNext = TestSubscriber.probe[CommittableMessage]()
      amqpSource.runWith(Sink.fromSubscriber(subscriberNext))
      subscriberNext.ensureSubscription()
      subscriberNext.request(4)
      subscriberNext.expectNext().bytes.utf8String shouldEqual "two"
    }

  }
}
