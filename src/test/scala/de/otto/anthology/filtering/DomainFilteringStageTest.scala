package de.otto.anthology.filtering

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.TestUtils.emptyKafkaConfig
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.config.AggregateConfig
import de.otto.anthology.config.DomainConfig
import de.otto.anthology.config.DomainConfigs
import de.otto.anthology.filtering.DomainFilteringConfig
import de.otto.anthology.filtering.DomainFilteringStage.filterDomainAggregates
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class DomainFilteringStageTest extends AnyFlatSpec, Matchers, Diagrams:
    "DomainFilteringStage" should "filter Aggregates depending on their config" in:
        // given
        val domainX = DomainName("domain-x")
        val aggregateXA = AggregateName("A")
        val aggIdX1 = AggregateId("abc-def-123")
        val aggX1 = Aggregate(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar", "status": 1}"""))
        val passX1 = mockedEmptyKafkaRecord(aggIdX1.toString)
        val aggIdX2 = AggregateId("abc-def-789")
        val aggX2 = Aggregate(mapper.readTree("""{"foo":"bar2", "foo-legacy":"baaar", "status": 3}"""))
        val passX2 = mockedEmptyKafkaRecord(aggIdX2.toString)
        val domainY = DomainName("domain-y")
        val aggregateYB = AggregateName("B")
        val aggIdY = AggregateId("a746589")
        val aggY = Aggregate(mapper.readTree("""{"hello":"world"}"""))
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
                                Some(DomainFilteringConfig(Seq(JsonPath.compile("$[?(@.status > 2)]")))),
                                None,
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
            (Some((QualifiedAggregateId(domainX, aggregateXA, aggIdX1), Some(aggX1))), passX1),
            (Some((QualifiedAggregateId(domainY, aggregateYB, aggIdY), Some(aggY))), passY),
            (Some((QualifiedAggregateId(domainX, aggregateXA, aggIdX2), Some(aggX2))), passX2)
        )
        val result = src.filterDomainAggregates(configs).runToList()

        // then
        assert(result.size == 3)
        assert(result(0)._1.get._1.domainName == domainX)
        assert(result(0)._1.get._1.id == aggIdX1)
        assert(result(0)._1.get._2.isEmpty) // Filtered out 🥳
        assert(result(0)._2 == passX1)
        assert(result(1)._1.get._1.domainName == domainY)
        assert(result(1)._1.get._1.id == aggIdY)
        assert(result(1)._1.get._2 == Some(aggY)) // Passed (no filter config)
        assert(result(1)._2 == passY)
        assert(result(2)._1.get._1.domainName == domainX)
        assert(result(2)._1.get._1.id == aggIdX2)
        assert(result(2)._1.get._2 == Some(aggX2)) // Passed filter 🥳
        assert(result(2)._2 == passX2)

    it should "apply chained filters" in:
        // given
        val domainX = DomainName("domain-x")
        val aggregateXA = AggregateName("A")
        val aggIdX1 = AggregateId("abc-def-123")
        val aggX1 =
            Aggregate(mapper.readTree("""{"foo":"bar1", "foo-legacy":"baaar", "status": 1, "division": "D1"}"""))
        val passX1 = mockedEmptyKafkaRecord(aggIdX1.toString)
        val aggIdX2 = AggregateId("abc-def-789")
        val aggX2 =
            Aggregate(mapper.readTree("""{"foo":"bar2", "foo-legacy":"baaar", "status": 3, "division": "D1"}"""))
        val passX2 = mockedEmptyKafkaRecord(aggIdX2.toString)
        val aggIdX3 = AggregateId("abc-def-555")
        val aggX3 =
            Aggregate(mapper.readTree("""{"foo":"bar3", "foo-legacy":"baaar", "status": 3, "division": "D2"}"""))
        val passX3 = mockedEmptyKafkaRecord(aggIdX3.toString)

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
                                Some(
                                    DomainFilteringConfig(
                                        Seq(
                                            JsonPath.compile("$[?(@.status > 2)]"),
                                            JsonPath.compile("$[?(@.division == 'D2')]")
                                        )
                                    )
                                ),
                                None,
                                None
                            )
                        )
                    )
                )
            )

        // when
        val src = Flow.fromValues(
            (Some((QualifiedAggregateId(domainX, aggregateXA, aggIdX1), Some(aggX1))), passX1),
            (Some((QualifiedAggregateId(domainX, aggregateXA, aggIdX2), Some(aggX2))), passX2),
            (Some((QualifiedAggregateId(domainX, aggregateXA, aggIdX3), Some(aggX3))), passX3)
        )
        val result = src.filterDomainAggregates(configs).runToList()

        // then
        assert(result.size == 3)
        assert(result(0)._1.get._1.domainName == domainX)
        assert(result(0)._1.get._1.id == aggIdX1)
        assert(result(0)._1.get._2.isEmpty) // Filtered out, caused by first filter 🥳
        assert(result(0)._2 == passX1)
        assert(result(1)._1.get._1.domainName == domainX)
        assert(result(1)._1.get._1.id == aggIdX2)
        assert(result(1)._1.get._2.isEmpty) // Filtered out caused by second filter 🥳
        assert(result(1)._2 == passX2)
        assert(result(2)._1.get._1.domainName == domainX)
        assert(result(2)._1.get._1.id == aggIdX3)
        assert(result(2)._1.get._2 == Some(aggX3)) // Passed both filters 🥳
        assert(result(2)._2 == passX3)
