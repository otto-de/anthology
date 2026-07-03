package de.otto.anthology.filtering

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.filtering.CodomainFilteringConfig
import de.otto.anthology.filtering.CodomainFilteringStage.filterCodomainMessages
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class CodomainFilteringStageTest extends AnyFlatSpec, Matchers, Diagrams:
    "CodomainFilteringStage" should "filter Codomain Aggregates" in:
        // given
        val aggIdX1 = MessageId("abc-def-123")
        val aggX1 = Message(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar", "status": 1}"""))
        val passX1 = mockedEmptyKafkaRecord(aggIdX1.toString)
        val aggIdX2 = MessageId("abc-def-789")
        val aggX2 = Message(mapper.readTree("""{"foo":"bar2", "foo-legacy":"baaar", "status": 3}"""))
        val passX2 = mockedEmptyKafkaRecord(aggIdX2.toString)
        val config =
            CodomainFilteringConfig(Seq(JsonPath.compile("$[?(@.status > 2)]")))

        // when
        val src = Flow.fromValues(
            (Seq((aggIdX1, Some(aggX1))), Seq(passX1)),
            (Seq((aggIdX2, Some(aggX2))), Seq(passX2))
        )
        val result = src.filterCodomainMessages(Some(config)).runToList()

        // then
        assert(result.size == 2)
        assert(result(0)._1.head._1 == aggIdX1)
        assert(result(0)._1.head._2.isEmpty) // Filtered out 🥳
        assert(result(0)._2.head == passX1)
        assert(result(1)._1.head._1 == aggIdX2)
        assert(result(1)._1.head._2 == Some(aggX2)) // Passed filter 🥳
        assert(result(1)._2.head == passX2)
