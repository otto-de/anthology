package de.otto.anthology.transformation

import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.JsonSupport
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.TestUtils.emptyKafkaConfig
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.config.AggregateConfig
import de.otto.anthology.config.DomainConfig
import de.otto.anthology.config.DomainConfigs
import de.otto.anthology.transformation.DomainAggregateIdTransformationConfig
import de.otto.anthology.transformation.DomainAggregateTransformationConfig
import de.otto.anthology.transformation.DomainTransformationStage.transformDomainAggregateIds
import de.otto.anthology.transformation.DomainTransformationStage.transformDomainAggregates
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class DomainTransformationStageTest extends AnyFlatSpec, Matchers, Diagrams:

    private val mapper = JsonSupport.mapper

    "DomainTransformationStage" should "skip transforming Ids without config" in:
        // given
        val domain = DomainName("domain-x")
        val aggregate = AggregateName("A")
        val aggId = AggregateId("abc-def-123")
        val agg = None
        val pass = mockedEmptyKafkaRecord("abc-def-123")
        val configs =
            DomainConfigs(
                Seq(
                    DomainConfig(
                        domain,
                        emptyKafkaConfig,
                        Seq(
                            AggregateConfig(
                                aggregate,
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
        val src = Flow.fromValues((Some(QualifiedAggregateId(domain, aggregate, aggId), agg), pass))
        val result = src.transformDomainAggregateIds(configs).runToList()

        // then
        assert(result.size == 1)
        assert(result.head._1 == Some(QualifiedAggregateId(domain, aggregate, aggId), agg))
        assert(result.head._2 == pass)

    it should "transform Ids depending on their config (1)" in:
        // given
        val domainX = DomainName("domain-x")
        val aggregateXA = AggregateName("A")
        val aggIdX = AggregateId("ABC-abc-def-123")
        val aggX = None
        val passX = mockedEmptyKafkaRecord("ABC-abc-def-123")

        val domainY = DomainName("domain-y")
        val aggregateYB = AggregateName("B")
        val aggIdY = AggregateId("a746589")
        val aggY = None
        val passY = mockedEmptyKafkaRecord("a746589")
        val configs =
            DomainConfigs(
                Seq(
                    DomainConfig(
                        domainX,
                        emptyKafkaConfig,
                        Seq(
                            AggregateConfig(
                                aggregateXA,
                                None,
                                None,
                                Some(DomainAggregateIdTransformationConfig("ABC-([0-9a-zA-Z-]+)".r)),
                                None
                            )
                        )
                    ),
                    DomainConfig(
                        domainY,
                        emptyKafkaConfig,
                        Seq(
                            AggregateConfig(
                                aggregateYB,
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
            (Some(QualifiedAggregateId(domainX, aggregateXA, aggIdX), aggX), passX),
            (Some(QualifiedAggregateId(domainY, aggregateYB, aggIdY), aggY), passY)
        )
        val result = src.transformDomainAggregateIds(configs).runToList()

        // then
        assert(result.size == 2)
        assert(result(0)._1 == Some(QualifiedAggregateId(domainX, aggregateXA, AggregateId("abc-def-123")), aggX))
        assert(result(0)._2 == passX)
        assert(result(1)._1 == Some(QualifiedAggregateId(domainY, aggregateYB, aggIdY), aggY))
        assert(result(1)._2 == passY)

    it should "transform Ids depending on their config (2)" in:
        // given
        val domainX = DomainName("domain-x")
        val aggregateXA = AggregateName("A")
        val aggIdX = AggregateId("ABC-abc-def-123")
        val aggX = None
        val passX = mockedEmptyKafkaRecord("ABC-abc-def-123")

        val domainY = DomainName("domain-y")
        val aggregateYB = AggregateName("B")
        val aggIdY = AggregateId("a746589")
        val aggY = None
        val passY = mockedEmptyKafkaRecord("a746589")
        val configs =
            DomainConfigs(
                Seq(
                    DomainConfig(
                        domainX,
                        emptyKafkaConfig,
                        Seq(
                            AggregateConfig(
                                aggregateXA,
                                None,
                                None,
                                Some(DomainAggregateIdTransformationConfig("ABC-([0-9a-zA-Z-]+)".r)),
                                None
                            )
                        )
                    ),
                    DomainConfig(
                        domainY,
                        emptyKafkaConfig,
                        Seq(
                            AggregateConfig(
                                aggregateYB,
                                None,
                                None,
                                Some(DomainAggregateIdTransformationConfig("[a-z]*([0-9]+)".r)),
                                None
                            )
                        )
                    )
                )
            )

        // when
        val src = Flow.fromValues(
            (Some(QualifiedAggregateId(domainX, aggregateXA, aggIdX), aggX), passX),
            (Some(QualifiedAggregateId(domainY, aggregateYB, aggIdY), aggY), passY)
        )
        val result = src.transformDomainAggregateIds(configs).runToList()

        // then
        assert(result.size == 2)
        assert(result(0)._1 == Some(QualifiedAggregateId(domainX, aggregateXA, AggregateId("abc-def-123")), aggX))
        assert(result(0)._2 == passX)
        assert(result(1)._1 == Some(QualifiedAggregateId(domainY, aggregateYB, AggregateId("746589")), aggY))
        assert(result(1)._2 == passY)

    it should "skip transforming Aggregates without config" in:
        // given
        val domain = DomainName("domain-x")
        val aggregate = AggregateName("A")
        val aggId = AggregateId("abc-def-123")
        val agg = Some(Aggregate(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar"}""")))
        val pass = mockedEmptyKafkaRecord("abc-def-123")
        val configs = DomainConfigs(Seq.empty)

        // when
        val src = Flow.fromValues((Some(QualifiedAggregateId(domain, aggregate, aggId), agg), pass))
        val result = src.transformDomainAggregates(configs).runToList()

        // then
        assert(result.size == 1)
        assert(result.head._1 == Some(QualifiedAggregateId(domain, aggregate, aggId), agg), pass)
        assert(result.head._2 == pass)

    it should "transform Aggregates depending on their config" in:
        // given
        val domainX = DomainName("domain-x")
        val aggregateXA = AggregateName("A")
        val aggIdX = AggregateId("abc-def-123")
        val aggX = Some(Aggregate(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar"}""")))
        val passX = mockedEmptyKafkaRecord("abc-def-123")
        val domainY = DomainName("domain-y")
        val aggregateYB = AggregateName("B")
        val aggIdY = AggregateId("a746589")
        val aggY = Some(Aggregate(mapper.readTree("""{"hello":"world", "foo-legacy":"baaar"}""")))
        val passY = mockedEmptyKafkaRecord("a746589")
        val configs =
            DomainConfigs(
                Seq(
                    DomainConfig(
                        domainX,
                        emptyKafkaConfig,
                        Seq(
                            AggregateConfig(
                                aggregateXA,
                                None,
                                None,
                                None,
                                Some(DomainAggregateTransformationConfig("transform-domain-x.json"))
                            )
                        )
                    )
                )
            )

        // when
        val src = Flow.fromValues(
            (Some(QualifiedAggregateId(domainX, aggregateXA, aggIdX), aggX), passX),
            (Some(QualifiedAggregateId(domainY, aggregateYB, aggIdY), aggY), passY)
        )
        val result = src.transformDomainAggregates(configs).runToList()

        // then
        val aggXExpected = Some(Aggregate(mapper.readTree("""{"foo":"bar1"}""")))
        assert(result.size == 2)
        assert(result(0)._1.get._1.domainName == domainX)
        assert(result(0)._1.get._1.id == AggregateId("abc-def-123"))
        assert(result(0)._1.get._2 == aggXExpected)
        assert(result(0)._2 == passX)
        assert(result(1)._1 == Some(QualifiedAggregateId(domainY, aggregateYB, aggIdY), aggY), passY)
        assert(result(1)._2 == passY)
