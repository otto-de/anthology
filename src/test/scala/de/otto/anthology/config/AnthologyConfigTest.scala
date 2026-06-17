package de.otto.anthology.config

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.config.AnthologyConfigFactory
import de.otto.anthology.config.ManyToOneConfig
import de.otto.anthology.config.OneToManyConfig
import de.otto.anthology.headerpropagation.GenerateConstant
import de.otto.anthology.headerpropagation.GenerateUUID
import de.otto.anthology.statestore.RocksDBConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class AnthologyConfigTest extends AnyFlatSpec, Matchers:

    "anthology config" should "be successfully loaded" in:
        val config = AnthologyConfigFactory()
        config.domains.map(_.name) should contain theSameElementsAs Set(
            DomainName("example-domain-a"),
            DomainName("example-domain-b"),
            DomainName("example-domain-c")
        )

        val domA = config.domainsByName(DomainName("example-domain-a"))
        domA.kafka.topic.toString shouldEqual "example-domain-a-topic"

        val aggA = domA.aggregatesByName(AggregateName("AggregateA"))
        aggA.filtering shouldEqual None
        aggA.idTransformation shouldEqual None
        aggA.transformation shouldEqual None
        val domB = config.domainsByName(DomainName("example-domain-b"))
        domB.kafka.topic.toString shouldEqual "example-domain-b-topic"
        val aggB = domB.aggregatesByName(AggregateName("AggregateB"))
        aggB.filtering.nonEmpty shouldEqual true
        aggB.filtering.map(_.filterPaths.head.getPath) shouldEqual Some(JsonPath.compile("$[?(@.status < 4)]").getPath)
        aggB.idTransformation.nonEmpty shouldEqual true
        aggB.idTransformation.map(_.pattern.toString) shouldEqual Some("FooBar.Prefix_([0-9]+)")
        aggB.transformation.nonEmpty shouldEqual true
        aggB.transformation.map(_.specFile) shouldEqual Some("transform-a.json")

        val domC = config.domainsByName(DomainName("example-domain-c"))
        domC.kafka.topic.toString shouldEqual "example-domain-c-topic"
        val aggC1 = domC.aggregatesByName(AggregateName("AggregateC1"))
        aggC1.filtering shouldEqual None
        val aggC2 = domC.aggregatesByName(AggregateName("AggregateC2"))
        aggC2.filtering shouldEqual None
        val aggC3 = domC.aggregatesByName(AggregateName("AggregateC3"))
        aggC3.filtering shouldEqual None

        val relAB = config.domainRelations(0)
        relAB shouldBe a[OneToManyConfig]
        relAB.from shouldEqual (DomainName("example-domain-a"), AggregateName("AggregateA"))
        relAB.to shouldEqual (DomainName("example-domain-b"), AggregateName("AggregateB"))
        relAB.asInstanceOf[OneToManyConfig].fromAggregatePath.getPath shouldEqual JsonPath.compile("$.top.key").getPath

        val relBC = config.domainRelations(1)
        relBC shouldBe a[ManyToOneConfig]
        relBC.from shouldEqual (DomainName("example-domain-b"), AggregateName("AggregateB"))
        relBC.to shouldEqual (DomainName("example-domain-c"), AggregateName("AggregateC1"))
        relBC.asInstanceOf[ManyToOneConfig].toAggregatePath.getPath shouldEqual JsonPath.compile("$.top.key").getPath

        val codomain = config.codomain
        codomain.deduplication.get.batchSize shouldEqual 20
        codomain.deduplication.get.batchingDuration shouldEqual 30.seconds
        codomain.filtering.map(_.filterPaths.head.getPath) shouldEqual Some(
            JsonPath.compile("$[?(@.status < 4)]").getPath
        )
        codomain.transformation.map(_.specFile) shouldEqual Some("transform-co.json")

        codomain.headerPropagation shouldBe defined
        val headerPropagations = codomain.headerPropagation.get
        headerPropagations.size shouldEqual 2
        headerPropagations(0).name shouldEqual "foo"
        headerPropagations(0).asInstanceOf[GenerateConstant].value shouldEqual "bar"
        headerPropagations(1).asInstanceOf[GenerateUUID].name shouldEqual "fuu"
        codomain.kafka.cluster.toString shouldEqual "sink-cluster"
        codomain.kafka.topic.toString shouldEqual "target-topic"

        config.kafkaClusters(1).name shouldEqual "example-cluster"
        config.kafkaClusters(1).bootstrapServers shouldEqual "localhost:9092"

        config.rocksDB.cacheSizeMb shouldEqual 512L
        config.rocksDB.writeBufferSizeMb shouldEqual 64L

        config.parallelism shouldEqual 12

    "RocksDBConfig" should "not load without DB path" in:
        intercept(RocksDBConfig()) shouldBe a[IllegalArgumentException]
