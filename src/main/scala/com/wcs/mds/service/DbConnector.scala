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
    case msg @ TabZhrMds001(begda, endda, pernr, nachnm, name2, gesch, icnum, usridLong, usrid, werks, bukrs, persg, ptext, stat2, kostl, kostx, aedtm, refda, flag1) =>
      log.info(s"Received $msg.")
      try {
        db.withTransaction {
          Query(TabZhrMds001s).filter(_.pernr === pernr).delete
          TabZhrMds001s.insert(begda, endda, pernr, nachnm, name2, gesch, icnum, usridLong, usrid, werks, bukrs, persg, ptext, stat2, kostl, kostx, aedtm, refda, flag1)
        }
        log.info(s"Saved $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ TabZhrMds002(begda, endda, otype, objid, short, stext, zcjid, zcjidms, zhrzzdwid, zhrzzdwms, zhrbzzwid, zhrbzzwmc, zhrzyxid, zhrzyxmc, zhrbzbmid, zhrbzbmmc, zhrtxxlid, zhrtxxlms, zhrjstdid, aedtm, refda, flag1) =>
      log.info(s"Received $msg.")
      try {
        db.withTransaction {
          Query(TabZhrMds002s).filter(_.otype === otype).filter(_.objid === objid).delete
          TabZhrMds002s.insert(begda, endda, otype, objid, short, stext, zcjid, zcjidms, zhrzzdwid, zhrzzdwms, zhrbzzwid, zhrbzzwmc, zhrzyxid, zhrzyxmc, zhrbzbmid, zhrbzbmmc, zhrtxxlid, zhrtxxlms, zhrjstdid, aedtm, refda, flag1)
        }
        log.info(s"Saved $msg.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Failed $msg.")
      }
    case msg @ TabZhrMds003(begda, endda, otype, objid, relat, sclas, sobid, prozt, priox, backup1, backup2, backup3, backup4, aedtm, flag1) =>
      log.info(s"Received $msg.")
      try {
        db.withTransaction {
          Query(TabZhrMds003s).filter(_.otype === otype).filter(_.objid === objid).filter(_.relat === relat).filter(_.sclas === sclas).filter(_.sobid === sobid).delete
          TabZhrMds003s.insert(begda, endda, otype, objid, relat, sclas, sobid, prozt, priox, backup1, backup2, backup3, backup4, aedtm, flag1)
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