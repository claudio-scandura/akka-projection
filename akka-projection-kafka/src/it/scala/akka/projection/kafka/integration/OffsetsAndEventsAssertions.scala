package akka.projection.kafka.integration

import akka.projection.{MergeableOffset, ProjectionId}
import org.scalatest.{Assertion, OptionValues}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

trait OffsetsAndEventsAssertions extends Matchers with OptionValues with ScalaFutures {

  import Model._

  def findByEventType(projectionId: ProjectionId, eventType: String): Option[UserEventCount]

  def readOffset(projectionId: ProjectionId): Future[Option[MergeableOffset[Long]]]

  def assertEventTypeCount(id: ProjectionId): Assertion = {
    def assertEventTypeCount(eventType: String): Assertion = {
      val count = findByEventType(id, eventType).value.count
      count shouldBe userEvents.count(_.eventType == eventType)
    }

    withClue("check - all event type counts are correct") {
      assertEventTypeCount(EventType.Login)
      assertEventTypeCount(EventType.Search)
      assertEventTypeCount(EventType.AddToCart)
      assertEventTypeCount(EventType.CheckoutCart)
      assertEventTypeCount(EventType.Logout)
    }
  }

  def assertAllOffsetsObserved(projectionId: ProjectionId, topicName: String) = {
    def offsetForUser(userId: String) = userEvents.count(_.userId == userId) - 1

    withClue("check - all offsets were seen") {
      val offset = readOffset(projectionId).futureValue.value
      offset shouldBe MergeableOffset(
        Map(
          s"$topicName-0" -> offsetForUser(user1),
          s"$topicName-1" -> offsetForUser(user2),
          s"$topicName-2" -> offsetForUser(user3)))
    }
  }
}