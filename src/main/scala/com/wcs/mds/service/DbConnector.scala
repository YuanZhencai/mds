package com.wcs.mds.service

import akka.actor.Actor
import akka.actor.ActorLogging
import com.wcs.mds.model._
import com.wcs.mds.model.SyncLog
import slick.driver.PostgresDriver.simple._
import scala.slick.session.Database
import Database.threadLocalSession
import scala.slick.lifted.Query

class DbConnector extends Actor with ActorLogging {

  val conf = context.system.settings.config
  val db = Database.forURL(url = conf.getString("jdbc.url"), driver = conf.getString("jdbc.driver"))

  override def preStart = {
    super.preStart()
    db.withSession {
      import com.wcs.mds.model._
      val ddl = TabZhrMds001s.ddl ++ TabZhrMds002s.ddl ++ TabZhrMds003s.ddl ++ SyncLogs.ddl
      ddl.dropStatements foreach { sql =>
        log.info(sql)
      }
      ddl.createStatements foreach { sql =>
        log.info(sql)
      }
    }
  }

  def receive = {
    case msg @ TabZhrMds001(_, _, pernr, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      log.info(s"Received $msg.")
      try {
        db.withTransaction {
          Query(TabZhrMds001s).filter(_.pernr === pernr).delete
          TabZhrMds001s.insert(TabZhrMds001.unapply(msg).get)
        }
        log.info(s"Saved $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ TabZhrMds002(_, _, otype, objid, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      log.info(s"Received $msg.")
      try {
        db.withTransaction {
          Query(TabZhrMds002s).filter(_.otype === otype).filter(_.objid === objid).delete
          TabZhrMds002s.insert(TabZhrMds002.unapply(msg).get)
        }
        log.info(s"Saved $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ TabZhrMds003(_, _, otype, objid, relat, sclas, sobid, _, _, _, _, _, _, _, _) =>
      log.info(s"Received $msg.")
      try {
        db.withTransaction {
          Query(TabZhrMds003s).filter(_.otype === otype).filter(_.objid === objid).filter(_.relat === relat).filter(_.sclas === sclas).filter(_.sobid === sobid).delete
          TabZhrMds003s.insert(TabZhrMds003.unapply(msg).get)
        }
        log.info(s"Saved $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ SyncLog(lastDate, processedAt) =>
      log.info(s"Received $msg.")
      try {
        db.withTransaction {
          SyncLogs.insert(lastDate, processedAt)
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