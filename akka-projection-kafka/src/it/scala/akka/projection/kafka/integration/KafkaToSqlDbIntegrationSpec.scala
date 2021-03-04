package akka.projection.kafka.integration

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.kafka.scaladsl.Producer
import akka.projection.jdbc.internal.{JdbcOffsetStore, JdbcSettings}
import akka.projection.jdbc.scaladsl.{JdbcHandler, JdbcProjection}
import akka.projection.jdbc.{JdbcProjectionSpec, JdbcSession}
import akka.projection.kafka.KafkaSpecBase
import akka.projection.kafka.integration.Model._
import akka.projection.kafka.integration.Repo._
import akka.projection.kafka.scaladsl.KafkaSourceProvider
import akka.projection.scaladsl.{ExactlyOnceProjection, SourceProvider}
import akka.projection.slick.internal.{SlickOffsetStore, SlickSettings}
import akka.projection.slick.{SlickHandler, SlickProjection, SlickProjectionSpec}
import akka.projection.{HandlerRecoveryStrategy, MergeableOffset, ProjectionId}
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.H2Profile

import java.lang.{Long => JLong}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object KafkaToSqlDbIntegrationSpec {

  sealed trait ProjectionSpec extends OffsetsAndEventsAssertions {

    def name: String

    def beforeAll(): Unit

    def createProjection[Offset](projectionId: ProjectionId, sourceProvider: SourceProvider[Offset, ConsumerRecord[String, String]]): ExactlyOnceProjection[Offset, ConsumerRecord[String, String]]

    def repository: EventTypeCountRepository[Any]

    def withFailingRepository(doTransientFailure: String => Boolean): ProjectionSpec
  }

  class SlickSpec(val name: String = "Slick")
                 (implicit system: ActorSystem[_]) extends ProjectionSpec {

    import system.executionContext

    val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig(SlickSettings.configPath, system.settings.config)
    val offsetStore = new SlickOffsetStore(system, dbConfig.db, dbConfig.profile, SlickSettings(system))
    val repository: EventTypeCountRepository[DBIO] = new SlickRepository(dbConfig)

    override def beforeAll(): Unit = {
      val done = for {
        _ <- SlickProjection.createOffsetTableIfNotExists(dbConfig)
        _ <- repository.createTableIfNotExists()
      } yield ()
      Await.result(done, 5.seconds)
    }

    override def createProjection[Offset](projectionId: ProjectionId, sourceProvider: SourceProvider[Offset, ConsumerRecord[String, String]]): ExactlyOnceProjection[Offset, ConsumerRecord[String, String]] = {
      SlickProjection.exactlyOnce(
        projectionId,
        sourceProvider,
        dbConfig,
        () =>
          SlickHandler[ConsumerRecord[String, String]] { envelope =>
            val userId = envelope.key()
            val eventType = envelope.value()
            val userEvent = UserEvent(userId, eventType)
            // do something with the record, payload in record.value
            repository.incrementCount(projectionId, userEvent.eventType)
          })

    }

    override def findByEventType(projectionId: ProjectionId, eventType: String): Option[UserEventCount] = {
      dbConfig.db.run(repository.findByEventType(projectionId, eventType)).futureValue
    }

    override def readOffset(projectionId: ProjectionId): Future[Option[MergeableOffset[Long]]] = offsetStore.readOffset(projectionId)

    override def withFailingRepository(doTransientFailure: String => Boolean): ProjectionSpec = {
      new SlickSpec(name)(system) {
        override val repository = new SlickRepository(dbConfig, doTransientFailure)
      }
    }
  }


  class JdbcSpec(val name: String = "Jdbc")
                (implicit system: ActorSystem[_]) extends ProjectionSpec {

    import JdbcProjectionSpec.jdbcSessionFactory

    val jdbcSettings = JdbcSettings(system)
    val offsetStore = new JdbcOffsetStore(system, jdbcSettings, jdbcSessionFactory)
    val repository: EventTypeCountRepository[CalledWithSession] = new JdbcRepository(sessionFactory = jdbcSessionFactory)

    override def beforeAll(): Unit = {
      Await.result(JdbcProjection.createOffsetTableIfNotExists(jdbcSessionFactory), 5.seconds)
      repository.createTableIfNotExists()
    }

    override def createProjection[Offset](projectionId: ProjectionId, sourceProvider: SourceProvider[Offset, ConsumerRecord[String, String]]): ExactlyOnceProjection[Offset, ConsumerRecord[String, String]] = {
      JdbcProjection.exactlyOnce(
        projectionId,
        sourceProvider,
        jdbcSessionFactory,
        () =>
          JdbcHandler[JdbcSession, ConsumerRecord[String, String]] { (session, envelope) =>
            val userId = envelope.key()
            val eventType = envelope.value()
            val userEvent = UserEvent(userId, eventType)
            // do something with the record, payload in record.value
            repository.incrementCount(projectionId, userEvent.eventType).callWith(session)
          })

    }

    override def findByEventType(projectionId: ProjectionId, eventType: String): Option[UserEventCount] = {
      repository.findByEventType(projectionId, eventType).callWith(jdbcSessionFactory())
    }

    override def readOffset(projectionId: ProjectionId): Future[Option[MergeableOffset[Long]]] = offsetStore.readOffset(projectionId)

    override def withFailingRepository(doTransientFailure: String => Boolean): ProjectionSpec = {
      new JdbcSpec(name)(system) {
        override val repository = new JdbcRepository(doTransientFailure, jdbcSessionFactory)
      }
    }
  }

}

class KafkaToSqlDbIntegrationSpec extends KafkaSpecBase(SlickProjectionSpec.config.withFallback(JdbcProjectionSpec.config).withFallback(ConfigFactory.load())) {

  import KafkaToSqlDbIntegrationSpec._

  val projectionSpecs = List(
    new SlickSpec(),
    new JdbcSpec()
  )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    projectionSpecs.foreach(_.beforeAll())
  }


  projectionSpecs.foreach { spec =>

    s"KafkaSourceProvider with ${spec.name}" must {

      s"project a model and Kafka offset map to a db exactly once" in {
        val topicName = createTopic(suffix = 0, partitions = 3, replication = 1)
        val groupId = createGroupId()
        val projectionId = ProjectionId(groupId, "UserEventCountProjection-1")

        produceEvents(topicName)

        val kafkaSourceProvider: SourceProvider[MergeableOffset[JLong], ConsumerRecord[String, String]] =
          KafkaSourceProvider(
            system.toTyped,
            consumerDefaults
              .withGroupId(groupId),
            Set(topicName))


        val projection = spec.createProjection(projectionId, kafkaSourceProvider)
        projectionTestKit.run(projection, remainingOrDefault) {
          spec.assertEventTypeCount(projectionId)
          spec.assertAllOffsetsObserved(projectionId, topicName)
        }
      }

      s"project a model and Kafka offset map to a db exactly once with a recovery strategy" in {
        val topicName = createTopic(suffix = 1, partitions = 3, replication = 1)
        val groupId = createGroupId()
        val projectionId = ProjectionId(groupId, "UserEventCountProjection-1")

        produceEvents(topicName)

        val kafkaSourceProvider: SourceProvider[MergeableOffset[JLong], ConsumerRecord[String, String]] =
          KafkaSourceProvider(
            system.toTyped,
            consumerDefaults
              .withGroupId(groupId),
            Set(topicName))

        // repository will fail to insert the "AddToCart" event type once only
        val failedOnce = new AtomicBoolean
        val failingSpec = spec.withFailingRepository { eventType =>
          if (!failedOnce.get && eventType == EventType.AddToCart) {
            failedOnce.set(true)
            true
          } else false
        }

        val resilientProjection = failingSpec.createProjection(projectionId, kafkaSourceProvider)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(retries = 1, delay = 0.millis))
        projectionTestKit.run(resilientProjection, remainingOrDefault) {
          failingSpec.assertEventTypeCount(projectionId)
          failingSpec.assertAllOffsetsObserved(projectionId, topicName)
        }
      }

      // https://github.com/akka/akka-projection/issues/382
      "resume a projection from the last saved offset plus one" in {
        val topicName = createTopic(suffix = 2, partitions = 4, replication = 1)
        val groupId = createGroupId()
        val projectionId = ProjectionId(groupId, "UserEventCountProjection-1")

        produceEvents(topicName)

        val kafkaSourceProvider: SourceProvider[MergeableOffset[JLong], ConsumerRecord[String, String]] =
          KafkaSourceProvider(
            system.toTyped,
            consumerDefaults
              .withGroupId(groupId),
            Set(topicName))


        projectionTestKit.run(spec.createProjection(projectionId, kafkaSourceProvider), remainingOrDefault) {
          spec.assertEventTypeCount(projectionId)
          spec.assertAllOffsetsObserved(projectionId, topicName)
        }

        println(s"Pre OFFSETS ${spec.readOffset(projectionId).futureValue}")
        // publish some more events
        Source(
          List(UserEvent(user1, EventType.Login), UserEvent(user4, EventType.Login), UserEvent(user4, EventType.Search)))
          .map(e => new ProducerRecord(topicName, partitionForUser(e.userId), e.userId, e.eventType))
          .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))
          .futureValue

        // re-run it in order to read back persisted offsets
        projectionTestKit.run(spec.createProjection(projectionId, kafkaSourceProvider), remainingOrDefault) {
          val count = spec.findByEventType(projectionId, EventType.Login).value.count
          count shouldBe (userEvents.count(_.eventType == EventType.Login) + 2)

          val offset = spec.readOffset(projectionId).futureValue.value
          offset shouldBe MergeableOffset(
            Map(
              s"$topicName-0" -> (userEvents.count(_.userId == user1) + 1 - 1), // one more for user1
              s"$topicName-1" -> (userEvents.count(_.userId == user2) - 1),
              s"$topicName-2" -> (userEvents.count(_.userId == user3) - 1),
              s"$topicName-3" -> (2 - 1) // two new for user4
            ))
        }

        println(s"Post OFFSETS ${spec.readOffset(projectionId).futureValue}")
      }
    }
  }

  def produceEvents(topic: String, range: immutable.Seq[UserEvent], partition: Int = 0): Future[Done] =
    Source(range)
      .map(e => new ProducerRecord(topic, partition, e.userId, e.eventType))
      .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

  def partitionForUser(userId: String): Int = {
    userId match { // deterministically produce events across available partitions
      case `user1` => 0
      case `user2` => 1
      case `user3` => 2
      case `user4` => 3
    }
  }

  def produceEvents(topicName: String): Unit = {
    val produceFs = for {
      (userId, events) <- userEvents.groupBy(_.userId)
      partition = partitionForUser(userId)
    } yield produceEvents(topicName, events, partition)
    Await.result(Future.sequence(produceFs), remainingOrDefault)
  }
}
