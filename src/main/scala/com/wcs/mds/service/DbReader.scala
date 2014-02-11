package com.wcs.mds.service

import akka.actor.Actor
import akka.actor.ActorLogging
import com.wcs.mds.model._
import slick.driver.PostgresDriver.simple._

class DbReader extends Actor with ActorLogging {

  val conf = context.system.settings.config
  val db = Database.forURL(url = conf.getString("jdbc.url"), driver = conf.getString("jdbc.driver"))

  def receive = {
    case msg @ GetRawTabZhrMds001 =>
      log.info(s"Received $msg.")
      try {
        db.withSession { implicit session =>
          val res: List[TabZhrMds001r] = TableQuery[TabZhrMds001s].sortBy(_.pernr).list.map { TabZhrMds001c.toRecord _ }
          sender ! res
        }
        log.info(s"Processed $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ GetRawTabZhrMds002 =>
      log.info(s"Received $msg.")
      try {
        db.withSession { implicit session =>
          val res: List[TabZhrMds002r] = TableQuery[TabZhrMds002s].sortBy(_.objid).list.map { TabZhrMds002c.toRecord _ }
          sender ! res
        }
        log.info(s"Processed $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ GetRawTabZhrMds003 =>
      log.info(s"Received $msg.")
      try {
        db.withSession { implicit session =>
          val res: List[TabZhrMds003r] = TableQuery[TabZhrMds003s].sortBy(_.objid).list.map { TabZhrMds003c.toRecord _ }
          sender ! res
        }
        log.info(s"Processed $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg =>
      log.error(s"Unrecognized message: $msg.")
  }

}