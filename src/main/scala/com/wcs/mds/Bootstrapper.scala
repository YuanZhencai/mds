package com.wcs.mds

import akka.kernel.Bootable
import spray.can.Http
import com.typesafe.config.ConfigFactory
import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import com.wcs.mds.api.RestActor
import com.wcs.mds.model.LoadNewData
import scala.concurrent.duration.DurationInt
import com.wcs.mds.service.DataLoader

class Bootstrapper extends Bootable {

  val config = ConfigFactory.load()
  implicit val system = ActorSystem("mds2")

  def startup = {
    val server = system.actorOf(Props[RestActor], "server")
    IO(Http) ! Http.Bind(server, config.getString("rest.listening"), config.getInt("rest.port"))
    val dataLoader = system.actorOf(Props[DataLoader], "data-loader")
    import system.dispatcher
    system.scheduler.schedule(5.seconds, 3600.seconds, dataLoader, LoadNewData)
  }

  def shutdown = {
    system.shutdown()
  }
}
