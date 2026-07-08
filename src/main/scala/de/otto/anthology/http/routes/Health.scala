package de.otto.anthology.http.routes

import de.otto.anthology.http.Route
import sttp.tapir.*

object Health:
    val route: Route =
        endpoint //
            .get
            .in("health")
            .out(stringBody)
            .handleSuccess(_ => "Anthology is up and running")
            .description("Anthology Health Check Endpoint")
