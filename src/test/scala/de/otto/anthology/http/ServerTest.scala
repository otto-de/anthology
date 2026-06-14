package de.otto.anthology.http

import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sttp.client4.*
import sttp.client4.testing.WebSocketStreamBackendStub
import sttp.tapir.server.stub4.TapirWebSocketStreamStubInterpreter

class ServerTest extends AnyFlatSpec, Matchers, Diagrams:

    private val backendStub = TapirWebSocketStreamStubInterpreter(WebSocketStreamBackendStub.synchronous)
        .whenServerEndpointsRunLogic(Server().underlying.serverEndpoints.toList)
        .backend()

    "Server" should "start and server health endpoint" in:
        val response = basicRequest
            .get(uri"http://localhost:8080/health")
            .send(backendStub)

        assert(response.code.code == 200)
        assert(response.body == Right("anthology is healthy"))
