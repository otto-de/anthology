package de.otto.anthology.http

import de.otto.anthology.http.routes.Health
import sttp.tapir.server.netty.sync.NettySyncServer

case class Server(routes: List[Route]):
    private[http] lazy val underlying = NettySyncServer()
        .host("0.0.0.0")
        .port(8080)
        .addEndpoints(routes)

    def start(): Unit =
        underlying
            .startAndWait()

object Server:
    def apply(): Server = Server(List(Health.route))
