package de.otto.capella

import de.otto.capella.ChannelName
import de.otto.capella.CodomainDeduplicationConfig
import de.otto.capella.CodomainDeduplicationStage.deduplicateCodomainMessages
import de.otto.capella.MessageFormatName
import de.otto.capella.MessageId
import de.otto.capella.QualifiedMessageId
import de.otto.capella.TestUtils.mockedEmptyKafkaRecord
import de.otto.capella.kafka.Passthrough
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

import scala.concurrent.duration.*

class CodomainDeduplicationStageTest extends AnyFlatSpec, Matchers, Diagrams:

    val domainX = ChannelName("domain-x")

    val aggregateXA = MessageFormatName("A")
    val aggregateIdXA1 = MessageId("d9708e05-e6a6-4d38-b452-44dd00856374")
    val aggregateIdXA2 = MessageId("cc5b5bd9-cded-4357-91b2-f8fdd4a3e0c9")
    val aggregateIdXA3 = MessageId("789023e5-2974-4dc3-877d-9f975305235b")
    val qaidXA1 = QualifiedMessageId(domainX, aggregateXA, aggregateIdXA1)
    val qaidXA2 = QualifiedMessageId(domainX, aggregateXA, aggregateIdXA2)
    val qaidXA3 = QualifiedMessageId(domainX, aggregateXA, aggregateIdXA3)
    val passXA11 = mockedEmptyKafkaRecord(aggregateIdXA1.toString, domainX.toString, 1, 1)
    val passXA12 = mockedEmptyKafkaRecord(aggregateIdXA1.toString, domainX.toString, 1, 7)
    val passXA13 = mockedEmptyKafkaRecord(aggregateIdXA1.toString, domainX.toString, 1, 13)
    val passXA21 = mockedEmptyKafkaRecord(aggregateIdXA2.toString, domainX.toString, 1, 2)
    val passXA22 = mockedEmptyKafkaRecord(aggregateIdXA2.toString, domainX.toString, 1, 8)
    val passXA23 = mockedEmptyKafkaRecord(aggregateIdXA2.toString, domainX.toString, 1, 14)
    val passXA31 = mockedEmptyKafkaRecord(aggregateIdXA3.toString, domainX.toString, 1, 3)
    val passXA32 = mockedEmptyKafkaRecord(aggregateIdXA3.toString, domainX.toString, 1, 9)
    val passXA33 = mockedEmptyKafkaRecord(aggregateIdXA3.toString, domainX.toString, 1, 15)
    // for the sake of simplicity lets assume domain name == topic name here

    val aggregateXB = MessageFormatName("B")
    val aggregateIdXB1 = MessageId("a00e158e-6c15-45f9-8db7-734435dcaf20")
    val aggregateIdXB2 = MessageId("e1bbd848-da71-4779-a930-e785ff3eb02b")
    val aggregateIdXB3 = MessageId("a9e872aa-3c13-4942-8f67-7ab5c18e7d01")
    val qaidXB1 = QualifiedMessageId(domainX, aggregateXB, aggregateIdXB1)
    val qaidXB2 = QualifiedMessageId(domainX, aggregateXB, aggregateIdXB2)
    val qaidXB3 = QualifiedMessageId(domainX, aggregateXB, aggregateIdXB3)
    val passXB11 = mockedEmptyKafkaRecord(aggregateIdXB1.toString, domainX.toString, 1, 4)
    val passXB12 = mockedEmptyKafkaRecord(aggregateIdXB1.toString, domainX.toString, 1, 10)
    val passXB13 = mockedEmptyKafkaRecord(aggregateIdXB1.toString, domainX.toString, 1, 16)
    val passXB21 = mockedEmptyKafkaRecord(aggregateIdXB2.toString, domainX.toString, 1, 5)
    val passXB22 = mockedEmptyKafkaRecord(aggregateIdXB2.toString, domainX.toString, 1, 11)
    val passXB23 = mockedEmptyKafkaRecord(aggregateIdXB2.toString, domainX.toString, 1, 17)
    val passXB31 = mockedEmptyKafkaRecord(aggregateIdXB3.toString, domainX.toString, 1, 6)
    val passXB32 = mockedEmptyKafkaRecord(aggregateIdXB3.toString, domainX.toString, 1, 12)
    val passXB33 = mockedEmptyKafkaRecord(aggregateIdXB3.toString, domainX.toString, 1, 18)

    val domainY = ChannelName("domain-y")

    val aggregateYA = MessageFormatName("A") // same name as above is intended
    val aggregateIdYA1 = MessageId("ce02add2-0932-448d-b753-acb3ed116663")
    val aggregateIdYA2 = MessageId("c433a2d5-ed0c-412b-957b-72f61db9f24d")
    val aggregateIdYA3 = MessageId("affe28af-8cf3-4e67-be73-0463fcae84b7")
    val qaidYA1 = QualifiedMessageId(domainY, aggregateYA, aggregateIdYA1)
    val qaidYA2 = QualifiedMessageId(domainY, aggregateYA, aggregateIdYA2)
    val qaidYA3 = QualifiedMessageId(domainY, aggregateYA, aggregateIdYA3)
    val passYA1 = mockedEmptyKafkaRecord(aggregateIdYA1.toString, domainY.toString, 1, 1)
    val passYA2 = mockedEmptyKafkaRecord(aggregateIdYA2.toString, domainY.toString, 1, 2)
    val passYA3 = mockedEmptyKafkaRecord(aggregateIdYA3.toString, domainY.toString, 1, 3)

    val coId01 = MessageId("c556f4bf-297c-4ded-8310-2dc8ffcab47a")
    val coId02 = MessageId("77dc2f1c-6c27-4acc-bf0e-bec8e144e188")
    val coId03 = MessageId("16c2a62a-6bd8-4076-8e43-f1c88f3402ec")
    val coId04 = MessageId("8ca84c5b-7f37-4207-b0cd-4ffc39f56edf")
    val coId05 = MessageId("63c57b7f-c46e-43a3-b8f3-e545a91ac4df")
    val coId06 = MessageId("7d53edad-8cac-43df-84a9-ad30701582cd")
    val coId07 = MessageId("e7d98ef3-1cf9-40a7-8276-65566de45907")
    val coId08 = MessageId("fe08907b-386b-4209-ad7e-f38a1d2d4081")
    val coId09 = MessageId("5e656953-e6dd-446b-8fcd-5ead06560dbe")
    val coId10 = MessageId("17a12bc5-652f-470f-97a5-f40c1595bc63")
    val coId11 = MessageId("c1afaca0-c7c5-468f-9f03-e041215008ad")
    val coId12 = MessageId("5b4aeac4-8593-4bd0-914c-fdcb65211c5f")
    val coId13 = MessageId("cf1c0ef4-e3b0-43f6-9535-d2448c330660")
    val coId14 = MessageId("a640019c-5337-4c7f-ad32-c00e24971ee4")
    val coId15 = MessageId("08404916-0f86-4b76-8da6-cc28722f2c56")

    "CodomainDeduplicationStageTest" should "do not deduplicate when batch size = 1" in:

        // given
        val config = CodomainDeduplicationConfig(1, 100.millis)

        val src: Flow[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
            Flow.fromValues(
                (Some((qaidXA1, Set(coId01))), passXA11),
                (Some((qaidXA2, Set(coId01))), passXA21),
                (Some((qaidXA3, Set(coId01))), passXA31)
            )

        // when
        val result: Seq[(Seq[(QualifiedMessageId, Seq[MessageId])], Seq[Passthrough])] =
            src
                .deduplicateCodomainMessages(Some(config))
                .runToList()

        // then
        assert(result.size == 3)
        val result0 = result(0)
        val result1 = result(1)
        val result2 = result(2)
        assert(result0._1 == Seq((qaidXA1, Seq(coId01))))
        assert(result1._1 == Seq((qaidXA2, Seq(coId01))))
        assert(result2._1 == Seq((qaidXA3, Seq(coId01))))
        assert(result0._2 == Seq(passXA11))
        assert(result1._2 == Seq(passXA21))
        assert(result2._2 == Seq(passXA31))

    it should "batch but not deduplicate when aggregate ids are distinct" in:

        // given
        val config = CodomainDeduplicationConfig(3, 100.millis)

        val src: Flow[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
            Flow.fromValues(
                (Some((qaidXA1, Set(coId01))), passXA11),
                (Some((qaidXA2, Set(coId01))), passXA21),
                (Some((qaidXA3, Set(coId01))), passXA31)
            )

        // when
        val result: Seq[(Seq[(QualifiedMessageId, Seq[MessageId])], Seq[Passthrough])] =
            src
                .deduplicateCodomainMessages(Some(config))
                .runToList()

        // then
        assert(result.size == 1)
        result(0)._1 should contain theSameElementsAs // no ordering guarantees!
            Seq(
                (qaidXA1, Seq(coId01)),
                (qaidXA2, Seq(coId01)),
                (qaidXA3, Seq(coId01))
            )
        result(0)._2 should contain theSameElementsAs Seq(passXA11, passXA21, passXA31)

    it should "batch and deduplicate in one window" in:

        // given
        val config = CodomainDeduplicationConfig(15, 100.millis)

        val src: Flow[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
            Flow.fromValues(
                (Some((qaidXA1, Set(coId01))), passXA11),
                (Some((qaidXA2, Set(coId01))), passXA21),
                (Some((qaidXA3, Set(coId01))), passXA31),
                (Some((qaidXB1, Set(coId01))), passXB11),
                (Some((qaidXB2, Set(coId01))), passXB21),
                (Some((qaidXB3, Set(coId01))), passXB31),
                (Some((qaidYA1, Set(coId01))), passYA1),
                (Some((qaidYA2, Set(coId01))), passYA2),
                (Some((qaidYA3, Set(coId01))), passYA3),
                (Some((qaidXA1, Set(coId01))), passXA12),
                (Some((qaidXA2, Set(coId01))), passXA22),
                (Some((qaidXA3, Set(coId01))), passXA32),
                (Some((qaidXB1, Set(coId01))), passXB12),
                (Some((qaidXB2, Set(coId01))), passXB22),
                (Some((qaidXB3, Set(coId01))), passXB32)
            )

        // when
        val result: Seq[(Seq[(QualifiedMessageId, Seq[MessageId])], Seq[Passthrough])] =
            src
                .deduplicateCodomainMessages(Some(config))
                .runToList()

        // then
        assert(result.size == 1)
        result(0)._1 should contain theSameElementsAs // no ordering guarantees!
            Seq(
                (qaidXA1, Seq(coId01)),
                (qaidXA2, Seq(coId01)),
                (qaidXA3, Seq(coId01)),
                (qaidXB1, Seq(coId01)),
                (qaidXB2, Seq(coId01)),
                (qaidXB3, Seq(coId01)),
                (qaidYA1, Seq(coId01)),
                (qaidYA2, Seq(coId01)),
                (qaidYA3, Seq(coId01))
            )
        result(0)._2 should contain theSameElementsInOrderAs // ordered by topic, then partition and then offset
            Seq(
                passXA11,
                passXA21,
                passXA31,
                passXB11,
                passXB21,
                passXB31,
                passXA12,
                passXA22,
                passXA32,
                passXB12,
                passXB22,
                passXB32,
                passYA1,
                passYA2,
                passYA3
            )

    it should "batch and deduplicate in multiple windows" in:

        // given
        val config = CodomainDeduplicationConfig(4, 100.millis)

        val src: Flow[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
            Flow.fromValues(
                (Some((qaidXA1, Set(coId01))), passXA11),
                (Some((qaidXA2, Set(coId01))), passXA21),
                (Some((qaidXA3, Set(coId01))), passXA31),
                (Some((qaidXB1, Set(coId01))), passXB11),
                (Some((qaidXB2, Set(coId01))), passXB21),
                (Some((qaidXB3, Set(coId01))), passXB31),
                (Some((qaidYA1, Set(coId01))), passYA1),
                (Some((qaidYA2, Set(coId01))), passYA2),
                (Some((qaidYA3, Set(coId01))), passYA3),
                (Some((qaidXA1, Set(coId01))), passXA12),
                (Some((qaidXA2, Set(coId01))), passXA22),
                (Some((qaidXA3, Set(coId01))), passXA32),
                (Some((qaidXB1, Set(coId01))), passXB12),
                (Some((qaidXB2, Set(coId01))), passXB22),
                (Some((qaidXB3, Set(coId01))), passXB32)
            )

        // when
        val result: Seq[(Seq[(QualifiedMessageId, Seq[MessageId])], Seq[Passthrough])] =
            src
                .deduplicateCodomainMessages(Some(config))
                .runToList()

        // then
        assert(result.size == 4)
        result(0)._1 should contain theSameElementsAs
            Seq(
                (qaidXA1, Seq(coId01)),
                (qaidXA2, Seq(coId01)),
                (qaidXA3, Seq(coId01)),
                (qaidXB1, Seq(coId01))
            )

        result(0)._2 should contain theSameElementsInOrderAs
            Seq(
                passXA11,
                passXA21,
                passXA31,
                passXB11
            )

        result(1)._1 should contain theSameElementsAs
            Seq(
                (qaidXB2, Seq(coId01)),
                (qaidXB3, Seq(coId01)),
                (qaidYA1, Seq(coId01)),
                (qaidYA2, Seq(coId01))
            )

        result(1)._2 should contain theSameElementsInOrderAs
            Seq(
                passXB21,
                passXB31,
                passYA1,
                passYA2
            )

        result(2)._1 should contain theSameElementsAs
            Seq(
                (qaidYA3, Seq(coId01)),
                (qaidXA1, Seq(coId01)),
                (qaidXA2, Seq(coId01)),
                (qaidXA3, Seq(coId01))
            )

        result(2)._2 should contain theSameElementsInOrderAs
            Seq(
                passXA12,
                passXA22,
                passXA32,
                passYA3
            )

        result(3)._1 should contain theSameElementsAs
            Seq(
                (qaidXB1, Seq(coId01)),
                (qaidXB2, Seq(coId01)),
                (qaidXB3, Seq(coId01))
            )

        result(3)._2 should contain theSameElementsInOrderAs
            Seq(
                passXB12,
                passXB22,
                passXB32
            )

    it should "batch and deduplicate in multiple codomains" in:

        // given
        val config = CodomainDeduplicationConfig(18, 100.millis)

        val src: Flow[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
            Flow.fromValues(
                (Some((qaidXA1, Set(coId01))), passXA11),
                (Some((qaidXA2, Set(coId01))), passXA21),
                (Some((qaidXA3, Set(coId01))), passXA31),
                (Some((qaidXB1, Set(coId01))), passXB11),
                (Some((qaidXB2, Set(coId01))), passXB21),
                (Some((qaidXB3, Set(coId01))), passXB31),
                (Some((qaidXA1, Set(coId02))), passXA12),
                (Some((qaidXA2, Set(coId02))), passXA22),
                (Some((qaidXA3, Set(coId02))), passXA32),
                (Some((qaidXB1, Set(coId02))), passXB12),
                (Some((qaidXB2, Set(coId02))), passXB22),
                (Some((qaidXB3, Set(coId02))), passXB32),
                (Some((qaidXA1, Set(coId03))), passXA13),
                (Some((qaidXA2, Set(coId03))), passXA23),
                (Some((qaidXA3, Set(coId03))), passXA33),
                (Some((qaidXB1, Set(coId03))), passXB13),
                (Some((qaidXB2, Set(coId03))), passXB23),
                (Some((qaidXB3, Set(coId03))), passXB33)
            )

        // when
        val result: Seq[(Seq[(QualifiedMessageId, Seq[MessageId])], Seq[Passthrough])] =
            src
                .deduplicateCodomainMessages(Some(config))
                .runToList()

        // then
        assert(result.size == 1)
        result(0)._1 should contain theSameElementsAs
            Seq(
                (qaidXA1, Seq(coId01, coId02, coId03)),
                (qaidXA2, Seq(coId01, coId02, coId03)),
                (qaidXA3, Seq(coId01, coId02, coId03)),
                (qaidXB1, Seq(coId01, coId02, coId03)),
                (qaidXB2, Seq(coId01, coId02, coId03)),
                (qaidXB3, Seq(coId01, coId02, coId03))
            )

        result(0)._2 should contain theSameElementsInOrderAs
            Seq(
                passXA11,
                passXA21,
                passXA31,
                passXB11,
                passXB21,
                passXB31,
                passXA12,
                passXA22,
                passXA32,
                passXB12,
                passXB22,
                passXB32,
                passXA13,
                passXA23,
                passXA33,
                passXB13,
                passXB23,
                passXB33
            )
