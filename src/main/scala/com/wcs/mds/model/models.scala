package com.wcs.mds.model

import java.sql.Timestamp
import scala.slick.driver.PostgresDriver.simple._
import java.text.SimpleDateFormat

object Util {
  def dateToTimestamp(date: String): Timestamp = {
    if ("0000-00-00" != date) {
      val formatter = new SimpleDateFormat("yyyy-MM-dd")
      new Timestamp(formatter.parse(date).getTime)
    } else {
      dateToTimestamp("0001-01-01")
    }
  }
  def timestampToDate(ts: Timestamp): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    formatter.format(ts)
  }
}

case class TabZhrMds001(val begda: Timestamp,
                        val endda: Timestamp,
                        val pernr: String,
                        val nachn: String,
                        val name2: String,
                        val gesch: String,
                        val icnum: String,
                        val usridLong: String,
                        val usrid: String,
                        val werks: String,
                        val bukrs: String,
                        val persg: String,
                        val ptext: String,
                        val stat2: String,
                        val kostl: String,
                        val kostx: String,
                        val aedtm: Timestamp,
                        val refda: Timestamp,
                        val flag1: String)

object TabZhrMds001 {
  import Util._
  def fromMap(map: Map[String,String]): TabZhrMds001 = {
    TabZhrMds001(
      dateToTimestamp(map.getOrElse("BEGDA", "0001-01-01")),
      dateToTimestamp(map.getOrElse("ENDDA", "0001-01-01")),
      map.getOrElse("PERNR", ""),
      map.getOrElse("NACHN", ""),
      map.getOrElse("NAME2", ""),
      map.getOrElse("GESCH", ""),
      map.getOrElse("ICNUM", ""),
      map.getOrElse("USRID_LONG", ""),
      map.getOrElse("USRID", ""),
      map.getOrElse("WERKS", ""),
      map.getOrElse("BUKRS", ""),
      map.getOrElse("PERSG", ""),
      map.getOrElse("PTEXT", ""),
      map.getOrElse("STAT2", ""),
      map.getOrElse("KOSTL", ""),
      map.getOrElse("KOSTX", ""),
      dateToTimestamp(map.getOrElse("AEDTM", "0001-01-01")),
      dateToTimestamp(map.getOrElse("REFDA", "0001-01-01")),
      map.getOrElse("FLAG1", "")
    )
  }
}

object TabZhrMds001s extends Table[(Timestamp, Timestamp, String, String, String, String, String, String, String, String, String, String, String, String, String, String, Timestamp, Timestamp, String)]("tab_zhr_mds_001") {
  def begda = column[Timestamp]("begda", O.NotNull)
  def endda = column[Timestamp]("endda", O.NotNull)
  def pernr = column[String]("pernr", O.PrimaryKey)
  def nachn = column[String]("nachn")
  def name2 = column[String]("name2")
  def gesch = column[String]("gesch")
  def icnum = column[String]("icnum")
  def usridLong = column[String]("usrid_long")
  def usrid = column[String]("usrid")
  def werks = column[String]("werks")
  def bukrs = column[String]("bukrs")
  def persg = column[String]("persg")
  def ptext = column[String]("ptext")
  def stat2 = column[String]("stat2")
  def kostl = column[String]("kostl")
  def kostx = column[String]("kostx")
  def aedtm = column[Timestamp]("aedtm")
  def refda = column[Timestamp]("refda")
  def flag1 = column[String]("flag1")
  def * = begda ~ endda ~ pernr ~ nachn ~ name2 ~ gesch ~ icnum ~ usridLong ~ usrid ~ werks ~ bukrs ~ persg ~ ptext ~ stat2 ~ kostl ~ kostx ~ aedtm ~ refda ~ flag1
}

case class TabZhrMds002(val begda: Timestamp,
                        val endda: Timestamp,
                        val otype: String,
                        val objid: String,
                        val short: String,
                        val stext: String,
                        val zcjid: String,
                        val zcjidms: String,
                        val zhrzzdwid: String,
                        val zhrzzdwms: String,
                        val zhrbzzwid: String,
                        val zhrbzzwmc: String,
                        val zhrzyxid: String,
                        val zhrzyxmc: String,
                        val zhrbzbmid: String,
                        val zhrbzbmmc: String,
                        val zhrtxxlid: String,
                        val zhrtxxlms: String,
                        val zhrjstdid: String,
                        val aedtm: Timestamp,
                        val refda: Timestamp,
                        val flag1: String)

object TabZhrMds002 {
  import Util._
  def fromMap(map: Map[String,String]): TabZhrMds002 = {
    TabZhrMds002(
      dateToTimestamp(map.getOrElse("BEGDA", "0001-01-01")),
      dateToTimestamp(map.getOrElse("ENDDA", "0001-01-01")),
      map.getOrElse("OTYPE", ""),
      map.getOrElse("OBJID", ""),
      map.getOrElse("SHORT", ""),
      map.getOrElse("STEXT", ""),
      map.getOrElse("ZCJID", ""),
      map.getOrElse("ZCJIDMS", ""),
      map.getOrElse("ZHRZZDWID", ""),
      map.getOrElse("ZHRZZDWMS", ""),
      map.getOrElse("ZHRBZZWID", ""),
      map.getOrElse("ZHRBZZWMC", ""),
      map.getOrElse("ZHRZYXID", ""),
      map.getOrElse("ZHRZYXMC", ""),
      map.getOrElse("ZHRBZBMID", ""),
      map.getOrElse("ZHRBZBMMC", ""),
      map.getOrElse("ZHRTXXLID", ""),
      map.getOrElse("ZHRTXXLMS", ""),
      map.getOrElse("ZHRJSTDID", ""),
      dateToTimestamp(map.getOrElse("AEDTM", "0001-01-01")),
      dateToTimestamp(map.getOrElse("REFDA", "0001-01-01")),
      map.getOrElse("FLAG1", "")
    )
  }
}

object TabZhrMds002s extends Table[(Timestamp, Timestamp, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, Timestamp, Timestamp, String)]("tab_zhr_mds_002") {
  def begda = column[Timestamp]("begda", O.NotNull)
  def endda = column[Timestamp]("endda", O.NotNull)
  def otype = column[String]("otype", O.NotNull)
  def objid = column[String]("objid", O.NotNull)
  def short = column[String]("short")
  def stext = column[String]("stext")
  def zcjid = column[String]("zcjid")
  def zcjidms = column[String]("zcjidms")
  def zhrzzdwid = column[String]("zhrzzdwid")
  def zhrzzdwms = column[String]("zhrzzdwms")
  def zhrbzzwid = column[String]("zhrbzzwid")
  def zhrbzzwmc = column[String]("zhrbzzwmc")
  def zhrzyxid = column[String]("zhrzyxid")
  def zhrzyxmc = column[String]("zhrzyxmc")
  def zhrbzbmid = column[String]("zhrbzbmid")
  def zhrbzbmmc = column[String]("zhrbzbmmc")
  def zhrtxxlid = column[String]("zhrtxxlid")
  def zhrtxxlms = column[String]("zhrtxxlms")
  def zhrjstdid = column[String]("zhrjstdid")
  def aedtm = column[Timestamp]("aedtm")
  def refda = column[Timestamp]("refda")
  def flag1 = column[String]("flag1")
  def pk = primaryKey("pk002", (otype, objid))
  def * = begda ~ endda ~ otype ~ objid ~ short ~ stext ~ zcjid ~ zcjidms ~ zhrzzdwid ~ zhrzzdwms ~ zhrbzzwid ~ zhrbzzwmc ~ zhrzyxid ~ zhrzyxmc ~ zhrbzbmid ~ zhrbzbmmc ~ zhrtxxlid ~ zhrtxxlms ~ zhrjstdid ~ aedtm ~ refda ~ flag1
}

case class TabZhrMds003(val begda: Timestamp,
                        val endda: Timestamp,
                        val otype: String,
                        val objid: String,
                        val relat: String,
                        val sclas: String,
                        val sobid: String,
                        val prozt: String,
                        val priox: String,
                        val backup1: String,
                        val backup2: String,
                        val backup3: String,
                        val backup4: String,
                        val aedtm: Timestamp,
                        val flag1: String)

object TabZhrMds003 {
  import Util._
  def fromMap(map: Map[String,String]): TabZhrMds003 = {
    TabZhrMds003(
      dateToTimestamp(map.getOrElse("BEGDA", "0001-01-01")),
      dateToTimestamp(map.getOrElse("ENDDA", "0001-01-01")),
      map.getOrElse("OTYPE", ""),
      map.getOrElse("OBJID", ""),
      map.getOrElse("RELAT", ""),
      map.getOrElse("SCLAS", ""),
      map.getOrElse("SOBID", ""),
      map.getOrElse("PROZT", ""),
      map.getOrElse("PRIOX", ""),
      map.getOrElse("BACKUP1", ""),
      map.getOrElse("BACKUP2", ""),
      map.getOrElse("BACKUP3", ""),
      map.getOrElse("BACKUP4", ""),
      dateToTimestamp(map.getOrElse("AEDTM", "0001-01-01")),
      map.getOrElse("FLAG1", "")
    )
  }
}

object TabZhrMds003s extends Table[(Timestamp, Timestamp, String, String, String, String, String, String, String, String, String, String, String, Timestamp, String)]("tab_zhr_mds_003") {
  def begda = column[Timestamp]("begda", O.NotNull)
  def endda = column[Timestamp]("endda", O.NotNull)
  def otype = column[String]("otype", O.NotNull)
  def objid = column[String]("objid", O.NotNull)
  def relat = column[String]("relat", O.NotNull)
  def sclas = column[String]("sclas", O.NotNull)
  def sobid = column[String]("sobid", O.NotNull)
  def prozt = column[String]("prozt")
  def priox = column[String]("priox")
  def backup1 = column[String]("backup1")
  def backup2 = column[String]("backup2")
  def backup3 = column[String]("backup3")
  def backup4 = column[String]("backup4")
  def aedtm = column[Timestamp]("aedtm")
  def flag1 = column[String]("flag1")
  def pk = primaryKey("pk003", (otype, objid, relat, sclas, sobid))
  def * = begda ~ endda ~ otype ~ objid ~ relat ~ sclas ~ sobid ~ prozt ~ priox ~ backup1 ~ backup2 ~ backup3 ~ backup4 ~ aedtm ~ flag1
}

case class SyncLog(val lastDate: Timestamp, val processedAt: Timestamp)

object SyncLogs extends Table[(Timestamp, Timestamp)]("sync_log") {
  def lastDate = column[Timestamp]("last_date")
  def processedAt = column[Timestamp]("processed_at")
  def pk = primaryKey("pk_sync", (lastDate, processedAt))
  def * = lastDate ~ processedAt
}

object MyTest {
  import slick.driver.PostgresDriver.simple._
  import scala.slick.session.Database
  import Database.threadLocalSession
  import scala.slick.lifted.Query
  import Util._

  def testSlickWithCaseClass() {
    val db = Database.forURL("jdbc:postgresql://localhost:5432/test?user=test&password=test", "org.postgresql.Driver")
    val ts = new Timestamp(System.currentTimeMillis)
    val sl = SyncLog(ts, ts)
    case class AlternativeSyncLog(ts1: Timestamp, ts2: Timestamp)
    val as = AlternativeSyncLog(dateToTimestamp("2014-01-01"), ts)
    db.withSession {
      SyncLogs.insert(SyncLog.unapply(sl).get)
      SyncLogs.insert(AlternativeSyncLog.unapply(as).get)
    }
  }
}