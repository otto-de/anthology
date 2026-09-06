package de.otto.capella.config

import com.jayway.jsonpath.JsonPath
import de.otto.capella.ChannelName
import de.otto.capella.MessageFormatName
import de.otto.capella.config.CapellaConfigFactory
import de.otto.capella.config.ManyToOne
import de.otto.capella.config.OneToMany
import de.otto.capella.headerpropagation.GenerateConstant
import de.otto.capella.headerpropagation.GenerateTimestamp
import de.otto.capella.headerpropagation.GenerateUUID
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class CapellaConfigTest extends AnyFlatSpec, Matchers:

    "capella config" should "be successfully loaded" in:
        val config = CapellaConfigFactory()
        config.domain.channels.map(_.name) should contain theSameElementsAs Set(
            ChannelName("example-domain-a"),
            ChannelName("example-domain-b"),
            ChannelName("example-domain-c")
        )

        val domA = config.domain.channelsByName(ChannelName("example-domain-a"))
        domA.kafka.topic.toString shouldEqual "example-domain-a-topic"
        domA.kafka.additionalConsumerPropertiesAsMap.get("propA1") shouldEqual Some("foo")
        domA.kafka.additionalConsumerPropertiesAsMap.get("propA2") shouldEqual Some("bar")

        val aggA = domA.messageFormatsByName(MessageFormatName("AggregateA"))
        aggA.filtering shouldEqual None
        aggA.idTransformation shouldEqual None
        aggA.transformation shouldEqual None
        val domB = config.domain.channelsByName(ChannelName("example-domain-b"))
        domB.kafka.topic.toString shouldEqual "example-domain-b-topic"
        domB.kafka.additionalConsumerPropertiesAsMap.isEmpty shouldEqual true
        val aggB = domB.messageFormatsByName(MessageFormatName("AggregateB"))
        aggB.filtering.nonEmpty shouldEqual true
        aggB.filtering.map(_.filterPaths.head.getPath) shouldEqual Some(JsonPath.compile("$[?(@.status < 4)]").getPath)
        aggB.idTransformation.nonEmpty shouldEqual true
        aggB.idTransformation.map(_.pattern.toString) shouldEqual Some("FooBar.Prefix_([0-9]+)")
        aggB.transformation.nonEmpty shouldEqual true
        aggB.transformation.map(_.specFile) shouldEqual Some("transform-a.json")

        val domC = config.domain.channelsByName(ChannelName("example-domain-c"))
        domC.kafka.topic.toString shouldEqual "example-domain-c-topic"
        val aggC1 = domC.messageFormatsByName(MessageFormatName("AggregateC1"))
        aggC1.filtering shouldEqual None
        val aggC2 = domC.messageFormatsByName(MessageFormatName("AggregateC2"))
        aggC2.filtering shouldEqual None
        val aggC3 = domC.messageFormatsByName(MessageFormatName("AggregateC3"))
        aggC3.filtering shouldEqual None

        val relAB = config.domain.relations(0)
        relAB shouldBe a[OneToMany]
        relAB.relFrom shouldEqual (ChannelName("example-domain-a"), MessageFormatName("AggregateA"))
        relAB.relTo shouldEqual (ChannelName("example-domain-b"), MessageFormatName("AggregateB"))
        relAB.asInstanceOf[OneToMany].refFromManyToOnePath.getPath shouldEqual JsonPath.compile("$.top.key").getPath

        val relBC = config.domain.relations(1)
        relBC shouldBe a[ManyToOne]
        relBC.relFrom shouldEqual (ChannelName("example-domain-b"), MessageFormatName("AggregateB"))
        relBC.relTo shouldEqual (ChannelName("example-domain-c"), MessageFormatName("AggregateC1"))
        relBC.asInstanceOf[ManyToOne].refFromManyToOnePath.getPath shouldEqual JsonPath.compile("$.top.key").getPath

        val codomain = config.codomain
        codomain.deduplication.get.batchSize shouldEqual 20
        codomain.deduplication.get.batchingDuration shouldEqual 30.seconds
        codomain.filtering.map(_.filterPaths.head.getPath) shouldEqual Some(
            JsonPath.compile("$[?(@.status < 4)]").getPath
        )
        codomain.transformation.map(_.specFile) shouldEqual Some("transform-co.json")

        codomain.headerPropagation shouldBe defined
        val headerPropagations = codomain.headerPropagation.get
        headerPropagations.size shouldEqual 3
        headerPropagations(0).name shouldEqual "specversion"
        headerPropagations(0).asInstanceOf[GenerateConstant].value shouldEqual "1.0"
        headerPropagations(1).asInstanceOf[GenerateUUID].name shouldEqual "id"
        headerPropagations(2).asInstanceOf[GenerateTimestamp].name shouldEqual "time"
        codomain.kafka.cluster.toString shouldEqual "sink-cluster"
        codomain.kafka.topic.toString shouldEqual "target-topic"
        codomain.kafka.additionalProducerPropertiesAsMap.get("propC1") shouldEqual Some("hello")
        codomain.kafka.additionalProducerPropertiesAsMap.get("propC2") shouldEqual Some("world")

        config.kafkaClusters(1).name shouldEqual "example-cluster"
        config.kafkaClusters(1).bootstrapServers shouldEqual "localhost:9092"

        config.rocksDB.cacheSizeMb shouldEqual 512L
        config.rocksDB.writeBufferSizeMb shouldEqual 64L
        config.rocksDB.bestEffortsRecovery shouldEqual true
