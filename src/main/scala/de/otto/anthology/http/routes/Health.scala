package de.otto.anthology.http.routes

import de.otto.anthology.http.Route
import sttp.tapir.*

object Health:
    val route: Route = endpoint.get
        .in("health")
        .out(stringBody)
        .handleSuccess(_ => "anthology is healthy")
        .description("Anthology health check endpoint")
