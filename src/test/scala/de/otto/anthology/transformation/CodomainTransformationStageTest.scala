package de.otto.anthology.transformation

import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.transformation.CodomainTransformationConfig
import de.otto.anthology.transformation.CodomainTransformationStage.transformCodomainMessages
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class CodomainTransformationStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "CodomainTransformationStage" should "transform codomain aggregates" in:
        // given
        val aggIdX = MessageId("abc-def-123")
        val aggX = Some(Message(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar"}""")))
        val passX = mockedEmptyKafkaRecord("abc-def-123")
        val config = Some(CodomainTransformationConfig("transform-codomain.json"))

        // when
        val src = Flow.fromValues(
            (Seq((aggIdX, aggX)), Seq(passX))
        )
        val result = src.transformCodomainMessages(config).runToList()

        // then
        val aggXExpected = Some(Message(mapper.readTree("""{"foo-legacy":"baaar"}""")))
        assert(result.size == 1)
        assert(result(0)._1.head._1 == MessageId("abc-def-123"))
        assert(result(0)._1.head._2 == aggXExpected)
        assert(result(0)._2 == Seq(passX))
