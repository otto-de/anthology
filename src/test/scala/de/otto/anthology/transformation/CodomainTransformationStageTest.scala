package de.otto.anthology.transformation

import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.transformation.CodomainTransformationConfig
import de.otto.anthology.transformation.CodomainTransformationStage.transformCodomainAggregates
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class CodomainTransformationStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "CodomainTransformationStage" should "transform codomain aggregates" in:
        // given
        val aggIdX = AggregateId("abc-def-123")
        val aggX = Some(Aggregate(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar"}""")))
        val passX = mockedEmptyKafkaRecord("abc-def-123")
        val config = Some(CodomainTransformationConfig("transform-codomain.json"))

        // when
        val src = Flow.fromValues(
            (Seq((aggIdX, aggX)), Seq(passX))
        )
        val result = src.transformCodomainAggregates(config).runToList()

        // then
        val aggXExpected = Some(Aggregate(mapper.readTree("""{"foo-legacy":"baaar"}""")))
        assert(result.size == 1)
        assert(result(0)._1.head._1 == AggregateId("abc-def-123"))
        assert(result(0)._1.head._2 == aggXExpected)
        assert(result(0)._2 == Seq(passX))
