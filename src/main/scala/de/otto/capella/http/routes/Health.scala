package de.otto.capella.http.routes

import de.otto.capella.http.Route
import sttp.tapir.*

object Health:
    val route: Route =
        endpoint //
            .get
            .in("health")
            .out(stringBody)
            .handleSuccess(_ => "Capella is up and running")
            .description("Capella Health Check Endpoint")
