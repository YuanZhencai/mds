package com.wcs.mds.service

import akka.actor.Actor
import akka.actor.ActorLogging
import com.wcs.mds.model._
import com.wcs.mds.model.SyncLog
import slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

class DbConnector extends Actor with ActorLogging {

  val conf = context.system.settings.config
  val db = Database.forURL(url = conf.getString("jdbc.url"), driver = conf.getString("jdbc.driver"))

  def receive = {
    case msg @ TabZhrMds001(_, _, pernr, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      log.info(s"Received $msg.")
      try {
        db.withDynTransaction {
          val tabZhrMds001s = TableQuery[TabZhrMds001s]
          tabZhrMds001s.filter(_.pernr === pernr).delete
          tabZhrMds001s.insert(msg)
        }
        log.info(s"Saved $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ TabZhrMds002(_, _, otype, objid, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      log.info(s"Received $msg.")
      try {
        db.withDynTransaction {
          val tabZhrMds002s = TableQuery[TabZhrMds002s]
          tabZhrMds002s.filter(_.otype === otype).filter(_.objid === objid).delete
          tabZhrMds002s.insert(msg)
        }
        log.info(s"Saved $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ TabZhrMds003(_, _, otype, objid, relat, sclas, sobid, _, _, _, _, _, _, _, _) =>
      log.info(s"Received $msg.")
      try {
        db.withDynTransaction {
          val tabZhrMds003s = TableQuery[TabZhrMds003s]
          tabZhrMds003s.filter(_.otype === otype).filter(_.objid === objid).filter(_.relat === relat).filter(_.sclas === sclas).filter(_.sobid === sobid).delete
          tabZhrMds003s.insert(msg)
        }
        log.info(s"Saved $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ SyncLog(lastDate, processedAt) =>
      log.info(s"Received $msg.")
      try {
        db.withDynTransaction {
          val syncLogs = TableQuery[SyncLogs]
          syncLogs.insert(msg)
        }
        log.info(s"Saved $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg =>
      log.error(s"Unrecognized message: $msg.")
  }

}