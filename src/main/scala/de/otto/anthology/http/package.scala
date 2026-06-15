package de.otto.anthology.http

import sttp.capabilities.WebSockets
import sttp.shared.Identity
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.netty.sync.OxStreams

type Route = ServerEndpoint[OxStreams & WebSockets, Identity]
