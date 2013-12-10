package com.wcs.mds.service

import com.wcs.mds.model._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import java.sql.Timestamp
import slick.driver.PostgresDriver.simple._
import scala.slick.session.Database
import Database.threadLocalSession
import scala.slick.lifted.Query

class DataLoader extends Actor with ActorLogging {
  val sapConnector = context.actorOf(Props[SapConnector], "sap-connector")
  def receive = {
    case LoadNewData =>
      log.info("Loading new data...")
      val conf = context.system.settings.config
      sapConnector ! buildSapRequest(Database.forURL(url = conf.getString("jdbc.url"), driver = conf.getString("jdbc.driver")))
    case LoadAllData =>
      log.info("Loading all data...")
      sapConnector ! RequestSapData("0001-01-01", Util.timestampToDate(new Timestamp(System.currentTimeMillis)), null)
    case msg =>
      log.error(s"Unrecognized message: $msg.")
  }

  def buildSapRequest(db: Database): RequestSapData = {
    var lastSuccess = new Timestamp(0L)
    db.withSession {
      val q = Query(SyncLogs).sortBy(_.lastDate.desc).take(1)
      for (r <- q) {
        lastSuccess = r._1
      }
    }
    RequestSapData(Util.timestampToDate(new Timestamp(lastSuccess.getTime - 1000 * 60 * 60 * 24)), Util.timestampToDate(new Timestamp(System.currentTimeMillis)), null)
  }

}