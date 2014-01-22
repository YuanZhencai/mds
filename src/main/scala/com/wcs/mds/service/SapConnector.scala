package com.wcs.mds.service

import akka.actor.{ActorRef, Actor, ActorLogging, Props}
import com.wcs.mds.model._
import com.sap.conn.jco.{JCoFieldIterator, JCoTable, JCoFunction, JCoParameterList}
import com.wcs.mds.jco.JCoService
import scala.collection.mutable.HashMap
import com.wcs.mds.model.TabZhrMds001
import com.wcs.mds.model.TabZhrMds002
import com.wcs.mds.model.TabZhrMds003
import com.wcs.mds.model.RequestSapData
import com.wcs.mds.model.Util._
import java.sql.Timestamp

class SapConnector extends Actor with ActorLogging {

  val dbConnector = context.actorOf(Props[DbConnector], "db-connector")

  def receive = {
    case req @ RequestSapData(beginDate, endDate, readFlag) =>
      log.info(s"Received $req.")
      try {
        val function: JCoFunction = JCoService.getFunction("ZRFC_MDS_DATA_HR")
        val params: JCoParameterList = function.getImportParameterList()
        if (beginDate != null) params.setValue("G_BEGDA", beginDate)
        if (endDate != null) params.setValue("G_ENDDA", endDate)
        if (readFlag != null) params.setValue("G_FLAG", readFlag)
        JCoService.execute(function)
        val data: JCoParameterList = function.getTableParameterList()
        val tb1: JCoTable = data.getTable("TAB_ZHR_MDS001")
        val tb2: JCoTable = data.getTable("TAB_ZHR_MDS002")
        val tb3: JCoTable = data.getTable("TAB_ZHR_MDS003")
        saveTabZhrMds001(tb1, dbConnector)
        saveTabZhrMds002(tb2, dbConnector)
        saveTabZhrMds003(tb3, dbConnector)
        dbConnector ! SyncLog(dateToTimestamp(endDate), new Timestamp(System.currentTimeMillis))
        log.info(s"Processed $req.")
      } catch {
        case ex: Throwable =>
          log.error(ex, s"Exception or error occurred during $req.")
      }
    case msg =>
      log.error(s"Unrecognized message: $msg.")
  }

  def rowAsMap(iter: JCoFieldIterator): Map[String, String] = {
    val rowMap = new HashMap[String, String]()
    while (iter.hasNextField) {
      val field = iter.nextField
      if (field.getValue != null)
        rowMap.put(field.getName, field.getString)
    }
    rowMap.toMap
  }

  def saveTabZhrMds001(table: JCoTable, dbConnector: ActorRef) = {
    log.info(s"[TAB_ZHR_MDS001] has ${table.getNumRows} rows.")
    for (i <- 0 until table.getNumRows) {
      table.setRow(i)
      val mds001 = TabZhrMds001c.fromMap(rowAsMap(table.getFieldIterator))
      dbConnector ! mds001
    }
    log.info("[TAB_ZHR_MDS001] completed.")
  }

  def saveTabZhrMds002(table: JCoTable, dbConnector: ActorRef) = {
    log.info(s"[TAB_ZHR_MDS002] has ${table.getNumRows} rows.")
    for (i <- 0 until table.getNumRows) {
      table.setRow(i)
      val mds002 = TabZhrMds002c.fromMap(rowAsMap(table.getFieldIterator))
      dbConnector ! mds002
    }
    log.info("[TAB_ZHR_MDS002] completed.")
  }

  def saveTabZhrMds003(table: JCoTable, dbConnector: ActorRef) = {
    log.info(s"[TAB_ZHR_MDS003] has ${table.getNumRows} rows.")
    for (i <- 0 until table.getNumRows) {
      table.setRow(i)
      val mds003 = TabZhrMds003c.fromMap(rowAsMap(table.getFieldIterator))
      dbConnector ! mds003
    }
    log.info("[TAB_ZHR_MDS003] completed.")
  }

}