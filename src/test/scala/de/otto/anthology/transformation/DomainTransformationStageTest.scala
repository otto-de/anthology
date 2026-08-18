package de.otto.anthology.transformation

import de.otto.anthology.ChannelName
import de.otto.anthology.JsonSupport
import de.otto.anthology.Message
import de.otto.anthology.MessageFormatName
import de.otto.anthology.MessageId
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.TestUtils.emptyKafkaConfig
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.config.ChannelConfig
import de.otto.anthology.config.ChannelConfigs
import de.otto.anthology.config.MessageFormatConfig
import de.otto.anthology.transformation.DomainMessageIdTransformationConfig
import de.otto.anthology.transformation.DomainMessageTransformationConfig
import de.otto.anthology.transformation.DomainTransformationStage.transformDomainMessageIds
import de.otto.anthology.transformation.DomainTransformationStage.transformDomainMessages
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class DomainTransformationStageTest extends AnyFlatSpec, Matchers, Diagrams:

    private val mapper = JsonSupport.mapper

    "DomainTransformationStage" should "skip transforming Ids without config" in:
        // given
        val domain = ChannelName("domain-x")
        val aggregate = MessageFormatName("A")
        val aggId = MessageId("abc-def-123")
        val agg = None
        val pass = mockedEmptyKafkaRecord("abc-def-123")
        val configs =
            ChannelConfigs(
                Seq(
                    ChannelConfig(
                        domain,
                        emptyKafkaConfig,
                        Seq(
                            MessageFormatConfig(
                                aggregate,
                                None,
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
        val src = Flow.fromValues((Some(QualifiedMessageId(domain, aggregate, aggId), agg), pass))
        val result = src.transformDomainMessageIds(configs).runToList()

        // then
        assert(result.size == 1)
        assert(result.head._1 == Some(QualifiedMessageId(domain, aggregate, aggId), agg))
        assert(result.head._2 == pass)

    it should "transform Ids depending on their config (1)" in:
        // given
        val domainX = ChannelName("domain-x")
        val aggregateXA = MessageFormatName("A")
        val aggIdX = MessageId("ABC-abc-def-123")
        val aggX = None
        val passX = mockedEmptyKafkaRecord("ABC-abc-def-123")

        val domainY = ChannelName("domain-y")
        val aggregateYB = MessageFormatName("B")
        val aggIdY = MessageId("a746589")
        val aggY = None
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
                                None,
                                None,
                                Some(DomainMessageIdTransformationConfig("ABC-([0-9a-zA-Z-]+)".r)),
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
                                None,
                                None
                            )
                        )
                    )
                )
            )

        // when
        val src = Flow.fromValues(
            (Some(QualifiedMessageId(domainX, aggregateXA, aggIdX), aggX), passX),
            (Some(QualifiedMessageId(domainY, aggregateYB, aggIdY), aggY), passY)
        )
        val result = src.transformDomainMessageIds(configs).runToList()

        // then
        assert(result.size == 2)
        assert(result(0)._1 == Some(QualifiedMessageId(domainX, aggregateXA, MessageId("abc-def-123")), aggX))
        assert(result(0)._2 == passX)
        assert(result(1)._1 == Some(QualifiedMessageId(domainY, aggregateYB, aggIdY), aggY))
        assert(result(1)._2 == passY)

    it should "transform Ids depending on their config (2)" in:
        // given
        val domainX = ChannelName("domain-x")
        val aggregateXA = MessageFormatName("A")
        val aggIdX = MessageId("ABC-abc-def-123")
        val aggX = None
        val passX = mockedEmptyKafkaRecord("ABC-abc-def-123")

        val domainY = ChannelName("domain-y")
        val aggregateYB = MessageFormatName("B")
        val aggIdY = MessageId("a746589")
        val aggY = None
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
                                None,
                                None,
                                Some(DomainMessageIdTransformationConfig("ABC-([0-9a-zA-Z-]+)".r)),
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
                                Some(DomainMessageIdTransformationConfig("[a-z]*([0-9]+)".r)),
                                None,
                                None
                            )
                        )
                    )
                )
            )

        // when
        val src = Flow.fromValues(
            (Some(QualifiedMessageId(domainX, aggregateXA, aggIdX), aggX), passX),
            (Some(QualifiedMessageId(domainY, aggregateYB, aggIdY), aggY), passY)
        )
        val result = src.transformDomainMessageIds(configs).runToList()

        // then
        assert(result.size == 2)
        assert(result(0)._1 == Some(QualifiedMessageId(domainX, aggregateXA, MessageId("abc-def-123")), aggX))
        assert(result(0)._2 == passX)
        assert(result(1)._1 == Some(QualifiedMessageId(domainY, aggregateYB, MessageId("746589")), aggY))
        assert(result(1)._2 == passY)

    it should "skip transforming Aggregates without config" in:
        // given
        val domain = ChannelName("domain-x")
        val aggregate = MessageFormatName("A")
        val aggId = MessageId("abc-def-123")
        val agg = Some(Message(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar"}""")))
        val pass = mockedEmptyKafkaRecord("abc-def-123")
        val configs = ChannelConfigs(Seq.empty)

        // when
        val src = Flow.fromValues((Some(QualifiedMessageId(domain, aggregate, aggId), agg), pass))
        val result = src.transformDomainMessages(configs).runToList()

        // then
        assert(result.size == 1)
        assert(result.head._1 == Some(QualifiedMessageId(domain, aggregate, aggId), agg), pass)
        assert(result.head._2 == pass)

    it should "transform Aggregates depending on their config" in:
        // given
        val domainX = ChannelName("domain-x")
        val aggregateXA = MessageFormatName("A")
        val aggIdX = MessageId("abc-def-123")
        val aggX = Some(Message(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar"}""")))
        val passX = mockedEmptyKafkaRecord("abc-def-123")
        val domainY = ChannelName("domain-y")
        val aggregateYB = MessageFormatName("B")
        val aggIdY = MessageId("a746589")
        val aggY = Some(Message(mapper.readTree("""{"hello":"world", "foo-legacy":"baaar"}""")))
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
                                None,
                                None,
                                None,
                                Some(DomainMessageTransformationConfig("transform-domain-x.json")),
                                None
                            )
                        )
                    )
                )
            )

        // when
        val src = Flow.fromValues(
            (Some(QualifiedMessageId(domainX, aggregateXA, aggIdX), aggX), passX),
            (Some(QualifiedMessageId(domainY, aggregateYB, aggIdY), aggY), passY)
        )
        val result = src.transformDomainMessages(configs).runToList()

        // then
        val aggXExpected = Some(Message(mapper.readTree("""{"foo":"bar1"}""")))
        assert(result.size == 2)
        assert(result(0)._1.get._1.channelName == domainX)
        assert(result(0)._1.get._1.id == MessageId("abc-def-123"))
        assert(result(0)._1.get._2 == aggXExpected)
        assert(result(0)._2 == passX)
        assert(result(1)._1 == Some(QualifiedMessageId(domainY, aggregateYB, aggIdY), aggY), passY)
        assert(result(1)._2 == passY)
