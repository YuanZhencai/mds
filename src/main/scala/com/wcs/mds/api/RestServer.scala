package com.wcs.mds.api

import akka.actor.Actor
import akka.pattern.ask
import spray.routing._
import spray.routing.authentication.BasicAuth
import spray.http.MediaTypes._
import shapeless.HNil
import scala.reflect.ClassTag
import com.wcs.mds.model.{LoadAllData, LoadNewData}

class RestActor extends Actor with RestServer {
  def actorRefFactory = context
  def receive = runRoute(route)
}

trait RestServer extends HttpService {
  implicit def executionContext = actorRefFactory.dispatcher
  implicit val timeout = akka.util.Timeout(5000)
  val dataLoader = actorRefFactory.actorSelection("../data-loader")
  val route = {
    get {
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
