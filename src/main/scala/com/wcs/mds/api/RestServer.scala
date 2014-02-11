package com.wcs.mds.api

import akka.actor.Actor
import akka.pattern.ask
import spray.routing._
import spray.routing.authentication.BasicAuth
import spray.http.MediaTypes._
import shapeless.HNil
import scala.reflect.ClassTag
import com.wcs.mds.model._
import com.wcs.mds.model.JsonCodecs._

class RestActor extends Actor with RestServer {
  def actorRefFactory = context
  def receive = runRoute(route)
}

trait RestServer extends HttpService {
  implicit def executionContext = actorRefFactory.dispatcher
  implicit val timeout = akka.util.Timeout(15000)
  val dataLoader = actorRefFactory.actorSelection("../data-loader")
  val dbReader = actorRefFactory.actorSelection("../db-reader")
  val route = {
    get {
      authenticate(BasicAuth()) { user =>
        respondWithMediaType(`application/json`) {
          path("raw" / "tab_zhr_mds_001") {
            complete {
              (dbReader ? GetRawTabZhrMds001).mapTo[List[TabZhrMds001r]]
            }
          } ~
          path("raw" / "tab_zhr_mds_002") {
            complete {
              (dbReader ? GetRawTabZhrMds002).mapTo[List[TabZhrMds002r]]
            }
          } ~
          path("raw" / "tab_zhr_mds_003") {
            complete {
              (dbReader ? GetRawTabZhrMds003).mapTo[List[TabZhrMds003r]]
            }
          }
        } ~
        respondWithMediaType(`text/plain`) {
          path("test") {
            complete {
              "test"
            }
          } ~
          path("admin" / "load") {
            complete {
              dataLoader ! LoadNewData
              "Command LoadNewData sent."
            }
          } ~
          path("admin" / "load-all") {
            complete {
              dataLoader ! LoadAllData
              "Command LoadAllData sent."
            }
          }
        }
      }
    }
  }
}
