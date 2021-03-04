package akka.projection.kafka.integration

import scala.collection.immutable.Seq

object Model {

  object EventType {
    val Login = "Login"
    val Search = "Search"
    val AddToCart = "AddToCart"
    val CheckoutCart = "CheckoutCart"
    val Logout = "Logout"
  }

  final case class UserEvent(userId: String, eventType: String)

  final case class UserEventCount(projectionName: String, eventType: String, count: Long)

  val user1 = "user-id-1"
  val user2 = "user-id-2"
  val user3 = "user-id-3"
  val user4 = "user-id-4"

  val userEvents = Seq(
    UserEvent(user1, EventType.Login),
    UserEvent(user1, EventType.Search),
    UserEvent(user1, EventType.Logout),
    UserEvent(user2, EventType.Login),
    UserEvent(user2, EventType.Search),
    UserEvent(user2, EventType.AddToCart),
    UserEvent(user2, EventType.Logout),
    UserEvent(user3, EventType.Login),
    UserEvent(user3, EventType.Search),
    UserEvent(user3, EventType.AddToCart),
    UserEvent(user3, EventType.Search),
    UserEvent(user3, EventType.AddToCart),
    UserEvent(user3, EventType.CheckoutCart),
    UserEvent(user3, EventType.Logout))

}




