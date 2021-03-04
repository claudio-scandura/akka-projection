package akka.projection.kafka.integration

import akka.Done
import akka.projection.ProjectionId
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.internal.JdbcSessionUtil
import slick.basic.DatabaseConfig
import slick.jdbc.H2Profile

import scala.concurrent.{ExecutionContext, Future}


object Repo {

  import Model._

  trait EventTypeCountRepository[+F[_]] {

    def incrementCount(id: ProjectionId, eventType: String): F[Done]

    def findByEventType(id: ProjectionId, eventType: String): F[Option[UserEventCount]]

    def createTableIfNotExists(): Future[Unit]
  }

  class SlickRepository(
                         val dbConfig: DatabaseConfig[H2Profile],
                         doTransientFailure: String => Boolean = _ => false)
                       (implicit ec: ExecutionContext) extends EventTypeCountRepository[slick.dbio.DBIO] {

    import dbConfig.profile.api._

    private val table = "SLICK_EVENTS_TYPE_COUNT"

    private class UserEventCountTable(tag: Tag) extends Table[UserEventCount](tag, table) {
      def projectionName = column[String]("PROJECTION_NAME")

      def eventType = column[String]("EVENT_TYPE")

      def count = column[Long]("COUNT")

      def pk = primaryKey("PK_PROJECTION_EVENT_TYPE", (projectionName, eventType))

      def * = (projectionName, eventType, count).mapTo[UserEventCount]
    }

    private val userEventCountTable = TableQuery[UserEventCountTable]

    def incrementCount(id: ProjectionId, eventType: String): DBIO[Done] = {
      val updateCount =
        sqlu"UPDATE SLICK_EVENTS_TYPE_COUNT SET COUNT = COUNT + 1 WHERE PROJECTION_NAME = ${id.name} AND EVENT_TYPE = $eventType"
      updateCount.flatMap {
        case 0 =>
          // The update statement updated no records so insert a seed record instead. If this insert fails because
          // another projection inserted it in the meantime then the envelope will be processed again based on the
          // retry policy of the `SlickHandler`
          val insert = userEventCountTable += UserEventCount(id.name, eventType, 1)
          if (doTransientFailure(eventType))
            DBIO.failed(new RuntimeException(s"Failed to insert event type: $eventType"))
          else
            insert.map(_ => Done)
        case _ => DBIO.successful(Done)
      }
    }

    def findByEventType(id: ProjectionId, eventType: String): DBIO[Option[UserEventCount]] =
      userEventCountTable.filter(t => t.projectionName === id.name && t.eventType === eventType).result.headOption

    def createTableIfNotExists(): Future[Unit] =
      dbConfig.db.run(userEventCountTable.schema.createIfNotExists)
  }

  trait CalledWithSession[+T] {
    def callWith(session: JdbcSession): T
  }

  class JdbcRepository(doTransientFailure: String => Boolean = _ => false,
                       sessionFactory: () => JdbcSession) extends EventTypeCountRepository[CalledWithSession] {


    private val table = "JDBC_EVENTS_TYPE_COUNT"

    def createTableIfNotExists(): Future[Unit] = {

      val createTableStatement =
        s"""
       create table if not exists "$table" (
        "PROJECTION_NAME" VARCHAR(255) NOT NULL,
        "EVENT_TYPE" VARCHAR(255) NOT NULL,
        "COUNT" INT NOT NULL
       );
       """

      val alterTableStatement =
        s"""alter table "$table" add constraint "PK_ID" primary key("PROJECTION_NAME", "EVENT_TYPE");"""

      sessionFactory().withConnection { conn =>
        JdbcSessionUtil.tryWithResource(conn.createStatement()) { stmt =>
          stmt.execute(createTableStatement)
          stmt.execute(alterTableStatement)
          Future.successful(())
        }
      }

    }

    def incrementCount(id: ProjectionId, eventType: String): CalledWithSession[Done] = { session =>
      val updateStatement = s"""UPDATE $table SET COUNT = COUNT + 1 WHERE PROJECTION_NAME = ? AND EVENT_TYPE = ?"""
      val insertStatement = s"""INSERT INTO $table VALUES(?, ?, ?)"""

      session.withConnection { conn =>
        val updated = JdbcSessionUtil.tryWithResource(conn.prepareStatement(updateStatement)) { stmt =>
          stmt.setString(1, id.name)
          stmt.setString(2, eventType)
          stmt.executeUpdate()
        }
        if (updated == 0) {
          if (doTransientFailure(eventType)) {
            new RuntimeException(s"Failed to insert event type: $eventType")
          }
          JdbcSessionUtil.tryWithResource(conn.prepareStatement(insertStatement)) { stmt =>
            stmt.setString(1, id.name)
            stmt.setString(2, eventType)
            stmt.setInt(3, 1)
            stmt.executeUpdate()
          }
        }
        Done
      }
    }

    def findByEventType(id: ProjectionId, eventType: String): CalledWithSession[Option[UserEventCount]] = { session =>
      val statement = s"""SELECT * from $table SET WHERE PROJECTION_NAME = ? AND EVENT_TYPE = ? LIMIT 1"""
      session.withConnection { conn =>
        JdbcSessionUtil.tryWithResource(conn.prepareStatement(statement)) { stmt =>
          stmt.setString(1, id.name)
          stmt.setString(2, eventType)
          val resultSet = stmt.executeQuery()
          if (!resultSet.next()) {
            None
          } else {
            Some(UserEventCount(
              resultSet.getString("PROJECTION_NAME"),
              resultSet.getString("EVENT_TYPE"),
              resultSet.getLong("COUNT")
            ))
          }
        }
      }
    }
  }

}