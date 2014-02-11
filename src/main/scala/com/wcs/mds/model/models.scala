package com.wcs.mds.model

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.slick.driver.PostgresDriver.simple._

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
  def timestampToEpoc(ts: Timestamp): String = {
    ts.getTime.toString
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

case class TabZhrMds001r(val begda: String,
                         val endda: String,
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
                         val aedtm: String,
                         val refda: String,
                         val flag1: String)

object TabZhrMds001c {
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
  def toRecord(data: TabZhrMds001): TabZhrMds001r = {
    TabZhrMds001r(
      timestampToEpoc(data.begda),
      timestampToEpoc(data.endda),
      data.pernr,
      data.nachn,
      data.name2,
      data.gesch,
      data.icnum,
      data.usridLong,
      data.usrid,
      data.werks,
      data.bukrs,
      data.persg,
      data.ptext,
      data.stat2,
      data.kostl,
      data.kostx,
      timestampToEpoc(data.aedtm),
      timestampToEpoc(data.refda),
      data.flag1
    )
  }
}

class TabZhrMds001s(tag: Tag) extends Table[TabZhrMds001](tag, "tab_zhr_mds_001") {
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
  def * = (begda, endda, pernr, nachn, name2, gesch, icnum, usridLong, usrid, werks, bukrs, persg, ptext, stat2, kostl, kostx, aedtm, refda, flag1) <> (TabZhrMds001.tupled, TabZhrMds001.unapply)
}
//val tabZhrMds001s = TableQuery[TabZhrMds001s]

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

case class TabZhrMds002r(val begda: String,
                         val endda: String,
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
                         val aedtm: String,
                         val refda: String,
                         val flag1: String)

object TabZhrMds002c {
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
  def toRecord(data: TabZhrMds002): TabZhrMds002r = {
    TabZhrMds002r(
      timestampToEpoc(data.begda),
      timestampToEpoc(data.endda),
      data.otype,
      data.objid,
      data.short,
      data.stext,
      data.zcjid,
      data.zcjidms,
      data.zhrzzdwid,
      data.zhrzzdwms,
      data.zhrbzzwid,
      data.zhrbzzwmc,
      data.zhrzyxid,
      data.zhrzyxmc,
      data.zhrbzbmid,
      data.zhrbzbmmc,
      data.zhrtxxlid,
      data.zhrtxxlms,
      data.zhrjstdid,
      timestampToEpoc(data.aedtm),
      timestampToEpoc(data.refda),
      data.flag1
    )
  }
}

class TabZhrMds002s(tag: Tag) extends Table[TabZhrMds002](tag, "tab_zhr_mds_002") {
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
  def * = (begda, endda, otype, objid, short, stext, zcjid, zcjidms, zhrzzdwid, zhrzzdwms, zhrbzzwid, zhrbzzwmc, zhrzyxid, zhrzyxmc, zhrbzbmid, zhrbzbmmc, zhrtxxlid, zhrtxxlms, zhrjstdid, aedtm, refda, flag1) <> (TabZhrMds002.tupled, TabZhrMds002.unapply)
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

case class TabZhrMds003r(val begda: String,
                         val endda: String,
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
                         val aedtm: String,
                         val flag1: String)

object TabZhrMds003c {
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
  def toRecord(data: TabZhrMds003): TabZhrMds003r = {
    TabZhrMds003r(
      timestampToEpoc(data.begda),
      timestampToEpoc(data.endda),
      data.otype,
      data.objid,
      data.relat,
      data.sclas,
      data.sobid,
      data.prozt,
      data.priox,
      data.backup1,
      data.backup2,
      data.backup3,
      data.backup4,
      timestampToEpoc(data.aedtm),
      data.flag1
    )
  }
}

class TabZhrMds003s(tag: Tag) extends Table[TabZhrMds003](tag, "tab_zhr_mds_003") {
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
  def * = (begda, endda, otype, objid, relat, sclas, sobid, prozt, priox, backup1, backup2, backup3, backup4, aedtm, flag1) <> (TabZhrMds003.tupled, TabZhrMds003.unapply)
}

case class SyncLog(val lastDate: Timestamp, val processedAt: Timestamp)

class SyncLogs(tag: Tag) extends Table[SyncLog](tag, "sync_log") {
  def lastDate = column[Timestamp]("last_date")
  def processedAt = column[Timestamp]("processed_at")
  def pk = primaryKey("pk_sync", (lastDate, processedAt))
  def * = (lastDate, processedAt) <> (SyncLog.tupled, SyncLog.unapply)
}

object JsonCodecs {
  import argonaut._, Argonaut._
  import spray.http.HttpEntity
  import spray.http.MediaTypes._
  import spray.httpx.marshalling._
  import spray.httpx.unmarshalling._
  implicit def TabZhrMds001rCodec = casecodec19(TabZhrMds001r.apply, TabZhrMds001r.unapply)("begda", "endda", "pernr", "nachn", "name2", "gesch", "icnum", "usridLong", "usrid", "werks", "bukrs", "persg", "ptext", "stat2", "kostl", "kostx", "aedtm", "refda", "flag1")
  implicit def TabZhrMds002rCodec = casecodec22(TabZhrMds002r.apply, TabZhrMds002r.unapply)("begda", "endda", "otype", "objid", "short", "stext", "zcjid", "zcjidms", "zhrzzdwid", "zhrzzdwms", "zhrbzzwid", "zhrbzzwmc", "zhrzyxid", "zhrzyxmc", "zhrbzbmid", "zhrbzbmmc", "zhrtxxlid", "zhrtxxlms", "zhrjstdid", "aedtm", "refda", "flag1")
  implicit def TabZhrMds003rCodec = casecodec15(TabZhrMds003r.apply, TabZhrMds003r.unapply)("begda", "endda", "otype", "objid", "relat", "sclas", "sobid", "prozt", "priox", "backup1", "backup2", "backup3", "backup4", "aedtm", "flag1")
  implicit val TabZhrMds001rListMarshaller = Marshaller.of[List[TabZhrMds001r]](`application/json`) { (value, contentType, ctx) => ctx.marshalTo(HttpEntity(contentType, value.asJson.toString)) }
  implicit val TabZhrMds002rListMarshaller = Marshaller.of[List[TabZhrMds002r]](`application/json`) { (value, contentType, ctx) => ctx.marshalTo(HttpEntity(contentType, value.asJson.toString)) }
  implicit val TabZhrMds003rListMarshaller = Marshaller.of[List[TabZhrMds003r]](`application/json`) { (value, contentType, ctx) => ctx.marshalTo(HttpEntity(contentType, value.asJson.toString)) }
}


object MyTest {
  import Util._

  def testSlickWithCaseClass() {
    val db = Database.forURL("jdbc:postgresql://localhost:5432/test?user=test&password=test", "org.postgresql.Driver")
    val ts = new Timestamp(System.currentTimeMillis)
    val sl = SyncLog(ts, ts)
    db.withSession { implicit session =>
      val syncLogs = TableQuery[SyncLogs]
      syncLogs.insert(sl)
    }
  }
}