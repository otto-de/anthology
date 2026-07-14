package de.otto.anthology.filtering

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.ChannelName
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Message
import de.otto.anthology.MessageFormatName
import de.otto.anthology.MessageId
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.TestUtils.emptyKafkaConfig
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.config.ChannelConfig
import de.otto.anthology.config.ChannelConfigs
import de.otto.anthology.config.MessageFormatConfig
import de.otto.anthology.filtering.DomainFilteringConfig
import de.otto.anthology.filtering.DomainFilteringStage.filterDomainMessages
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class DomainFilteringStageTest extends AnyFlatSpec, Matchers, Diagrams:
    "DomainFilteringStage" should "filter Aggregates depending on their config" in:
        // given
        val domainX = ChannelName("domain-x")
        val aggregateXA = MessageFormatName("A")
        val aggIdX1 = MessageId("abc-def-123")
        val aggX1 = Message(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar", "status": 1}"""))
        val passX1 = mockedEmptyKafkaRecord(aggIdX1.toString)
        val aggIdX2 = MessageId("abc-def-789")
        val aggX2 = Message(mapper.readTree("""{"foo":"bar2", "foo-legacy":"baaar", "status": 3}"""))
        val passX2 = mockedEmptyKafkaRecord(aggIdX2.toString)
        val domainY = ChannelName("domain-y")
        val aggregateYB = MessageFormatName("B")
        val aggIdY = MessageId("a746589")
        val aggY = Message(mapper.readTree("""{"hello":"world"}"""))
        val passY = mockedEmptyKafkaRecord("a746589")
        val configs =
            ChannelConfigs(
                Seq(
                    ChannelConfig(
                        domainX,
                        emptyKafkaConfig,
                        Seq(
                            MessageFormatConfig(
                                aggregateXA,
                                None,
                                Some(DomainFilteringConfig(Seq(JsonPath.compile("$[?(@.status > 2)]")))),
                                None,
                                None,
                                None
                            )
                        )
                    ),
                    ChannelConfig(
                        domainY,
                        emptyKafkaConfig,
                        Seq(
                            MessageFormatConfig(
                                aggregateYB,
                                None,
                                None,
                                None,
                                None,
                                None
                            )
                        )
                    )
                )
            )

        // when
        val src = Flow.fromValues(
            (Some((QualifiedMessageId(domainX, aggregateXA, aggIdX1), Some(aggX1))), passX1),
            (Some((QualifiedMessageId(domainY, aggregateYB, aggIdY), Some(aggY))), passY),
            (Some((QualifiedMessageId(domainX, aggregateXA, aggIdX2), Some(aggX2))), passX2)
        )
        val result = src.filterDomainMessages(configs).runToList()

        // then
        assert(result.size == 3)
        assert(result(0)._1.get._1.channelName == domainX)
        assert(result(0)._1.get._1.id == aggIdX1)
        assert(result(0)._1.get._2.isEmpty) // Filtered out 🥳
        assert(result(0)._2 == passX1)
        assert(result(1)._1.get._1.channelName == domainY)
        assert(result(1)._1.get._1.id == aggIdY)
        assert(result(1)._1.get._2 == Some(aggY)) // Passed (no filter config)
        assert(result(1)._2 == passY)
        assert(result(2)._1.get._1.channelName == domainX)
        assert(result(2)._1.get._1.id == aggIdX2)
        assert(result(2)._1.get._2 == Some(aggX2)) // Passed filter 🥳
        assert(result(2)._2 == passX2)

    it should "apply chained filters" in:
        // given
        val domainX = ChannelName("domain-x")
        val aggregateXA = MessageFormatName("A")
        val aggIdX1 = MessageId("abc-def-123")
        val aggX1 =
            Message(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar", "status": 1, "division": "D1"}"""))
        val passX1 = mockedEmptyKafkaRecord(aggIdX1.toString)
        val aggIdX2 = MessageId("abc-def-789")
        val aggX2 =
            Message(mapper.readTree("""{"foo":"bar2", "foo-legacy":"baaar", "status": 3, "division": "D1"}"""))
        val passX2 = mockedEmptyKafkaRecord(aggIdX2.toString)
        val aggIdX3 = MessageId("abc-def-555")
        val aggX3 =
            Message(mapper.readTree("""{"foo":"bar3", "foo-legacy":"baaar", "status": 3, "division": "D2"}"""))
        val passX3 = mockedEmptyKafkaRecord(aggIdX3.toString)

        val configs =
            ChannelConfigs(
                Seq(
                    ChannelConfig(
                        domainX,
                        emptyKafkaConfig,
                        Seq(
                            MessageFormatConfig(
                                aggregateXA,
                                None,
                                Some(
                                    DomainFilteringConfig(
                                        Seq(
                                            JsonPath.compile("$[?(@.status > 2)]"),
                                            JsonPath.compile("$[?(@.division == 'D2')]")
                                        )
                                    )
                                ),
                                None,
                                None,
                                None
                            )
                        )
                    )
                )
            )

        // when
        val src = Flow.fromValues(
            (Some((QualifiedMessageId(domainX, aggregateXA, aggIdX1), Some(aggX1))), passX1),
            (Some((QualifiedMessageId(domainX, aggregateXA, aggIdX2), Some(aggX2))), passX2),
            (Some((QualifiedMessageId(domainX, aggregateXA, aggIdX3), Some(aggX3))), passX3)
        )
        val result = src.filterDomainMessages(configs).runToList()

        // then
        assert(result.size == 3)
        assert(result(0)._1.get._1.channelName == domainX)
        assert(result(0)._1.get._1.id == aggIdX1)
        assert(result(0)._1.get._2.isEmpty) // Filtered out, caused by first filter 🥳
        assert(result(0)._2 == passX1)
        assert(result(1)._1.get._1.channelName == domainX)
        assert(result(1)._1.get._1.id == aggIdX2)
        assert(result(1)._1.get._2.isEmpty) // Filtered out caused by second filter 🥳
        assert(result(1)._2 == passX2)
        assert(result(2)._1.get._1.channelName == domainX)
        assert(result(2)._1.get._1.id == aggIdX3)
        assert(result(2)._1.get._2 == Some(aggX3)) // Passed both filters 🥳
        assert(result(2)._2 == passX3)
