package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import de.otto.anthology.CodomainCompositionStage.composeCodomainMessages
import de.otto.anthology.CodomainDeduplicationConfig
import de.otto.anthology.CodomainDeduplicationStage.deduplicateCodomainMessages
import de.otto.anthology.CodomainInliningStage.inlineDomainMessages
import de.otto.anthology.CodomainTriggeringStage.triggerAffectedCodomainMessages
import de.otto.anthology.DomainLinkingStage.linkDomainMessages
import de.otto.anthology.DomainPersistenceStage.persistDomainMessages
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.TestData.*
import de.otto.anthology.TestUtils.InMemoryStateStore
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.TestUtils.mockedKafkaRecord
import de.otto.anthology.kafka.Passthrough
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

import scala.concurrent.duration.*

class CodomainInliningStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "CodomainInliningStage" should "create aggregate root document in nested structure" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryIdFantasy: MessageId = setupCategoryIdFantasy
        val categoryFantasyQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryIdFantasy)
        val categoryFantasy: Message = setupCategoryFantasy
        val categoryPassFantasy: Passthrough = mockedKafkaRecord(categoryIdFantasy.toString, categoryFantasy.toJson)

        val categoryIdMystery: MessageId = setupCategoryIdMystery
        val categoryMysteryQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryIdMystery)
        val categoryMystery: Message = setupCategoryMystery
        val categoryPassMystery: Passthrough = mockedKafkaRecord(categoryIdMystery.toString, categoryMystery.toJson)

        val categoryIdScientific: MessageId = setupCategoryIdScientific
        val categoryScientificQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryIdScientific)
        val categoryScientific: Message = setupCategoryScientific
        val categoryPassScientific: Passthrough =
            mockedKafkaRecord(categoryIdScientific.toString, categoryScientific.toJson)

        val authorIdTolkien: MessageId = setupAuthorIdTolkien
        val authorTolkienQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorIdTolkien)
        val authorTolkien: Message = setupAuthorTolkien
        val authorPassTolkien: Passthrough = mockedKafkaRecord(authorIdTolkien.toString, authorTolkien.toJson)

        val authorIdChristie: MessageId = setupAuthorIdChristie
        val authorChristieQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorIdChristie)
        val authorChristie: Message = setupAuthorCristie
        val authorPassChristie: Passthrough = mockedKafkaRecord(authorIdChristie.toString, authorChristie.toJson)

        val authorIdKnuth: MessageId = setupAuthorIdKnuth
        val authorKnuthQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorIdKnuth)
        val authorKnuth: Message = setupAuthorKnuth
        val authorPassKnuth: Passthrough = mockedKafkaRecord(authorIdKnuth.toString, authorKnuth.toJson)

        val authorIdFeynman: MessageId = setupAuthorIdFeynman
        val authorFeynmanQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorIdFeynman)
        val authorFeynman: Message = setupAuthorFeynman
        val authorPassFeynman: Passthrough = mockedKafkaRecord(authorIdFeynman.toString, authorFeynman.toJson)

        val bookIdSilmarillion: MessageId = setupBookIdSilmarillion
        val bookSilmarillionQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookIdSilmarillion)
        val bookSilmarillion: Message = setupBookSilmarillion
        val bookPassSilmarillion: Passthrough = mockedKafkaRecord(bookIdSilmarillion.toString, bookSilmarillion.toJson)

        val bookIdHobbit: MessageId = setupBookIdHobbit
        val bookHobbitQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookIdHobbit)
        val bookHobbit: Message = setupBookHobbit
        val bookPassHobbit: Passthrough = mockedKafkaRecord(bookIdHobbit.toString, bookHobbit.toJson)

        val bookIdRings: MessageId = setupBookIdRings
        val bookRingsQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookIdRings)
        val bookRings: Message = setupBookRings()
        val bookPassRings: Passthrough = mockedKafkaRecord(bookIdRings.toString, bookRings.toJson)

        val bookIdOrient: MessageId = setupBookIdOrientExpress
        val bookOrientQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookIdOrient)
        val bookOrient: Message = setupBookOrientExpress
        val bookPassOrient: Passthrough = mockedKafkaRecord(bookIdOrient.toString, bookOrient.toJson)

        val bookIdNile: MessageId = setupBookIdNile
        val bookNileQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookIdNile)
        val bookNile: Message = setupBookNile
        val bookPassNile: Passthrough = mockedKafkaRecord(bookIdNile.toString, bookNile.toJson)

        val bookIdComputer: MessageId = setupBookIdComputer
        val bookComputerQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookIdComputer)
        val bookComputer: Message = setupBookComputer
        val bookPassComputer: Passthrough = mockedKafkaRecord(bookIdComputer.toString, bookComputer.toJson)

        val bookIdEasy: MessageId = setupBookIdEasy
        val bookEasyQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookIdEasy)
        val bookEasy: Message = setupBookEasy
        val bookPassEasy: Passthrough = mockedKafkaRecord(bookIdEasy.toString, bookEasy.toJson)

        val shelfIdFiction: MessageId = setupShelfIdFiction
        val shelfFictionQaid = QualifiedMessageId(storageChannel, shelfMessageFormat, shelfIdFiction)
        val shelfFiction: Message = setupShelfFiction
        val shelfPassFiction: Passthrough = mockedKafkaRecord(shelfIdFiction.toString, shelfFiction.toJson)

        val shelfIdNonFiction: MessageId = setupShelfIdNonFiction
        val shelfNonFictionQaid = QualifiedMessageId(storageChannel, shelfMessageFormat, shelfIdNonFiction)
        val shelfNonFiction: Message = setupShelfNonFiction
        val shelfPassNonFiction: Passthrough = mockedKafkaRecord(shelfIdNonFiction.toString, shelfNonFiction.toJson)

        val receiptIdS1: MessageId = setupReceiptIdSilmarillion1
        val receiptS1Qaid = QualifiedMessageId(storageChannel, receiptMessageFormat, receiptIdS1)
        val receiptS1: Message = setupReceiptSilmarillion1
        val receiptPassS1: Passthrough = mockedKafkaRecord(receiptIdS1.toString, receiptS1.toJson)

        val receiptIdS2: MessageId = setupReceiptIdSilmarillion2
        val receiptS2Qaid = QualifiedMessageId(storageChannel, receiptMessageFormat, receiptIdS2)
        val receiptS2: Message = setupReceiptSilmarillion2
        val receiptPassS2: Passthrough = mockedKafkaRecord(receiptIdS2.toString, receiptS2.toJson)

        val receiptIdE: MessageId = setupReceiptIdEasy
        val receiptEQaid = QualifiedMessageId(storageChannel, receiptMessageFormat, receiptIdE)
        val receiptE: Message = setupReceiptEasy
        val receiptPassE: Passthrough = mockedKafkaRecord(receiptIdE.toString, receiptE.toJson)

        val relationsConfig = setupRelationsConfig()

        { // when
            val src = Flow.fromValues(
                (Some(categoryFantasyQaid, Some(categoryFantasy)), categoryPassFantasy),
                (Some(categoryMysteryQaid, Some(categoryMystery)), categoryPassMystery),
                (Some(categoryScientificQaid, Some(categoryScientific)), categoryPassScientific),
                (Some(authorTolkienQaid, Some(authorTolkien)), authorPassTolkien),
                (Some(authorChristieQaid, Some(authorChristie)), authorPassChristie),
                (Some(authorKnuthQaid, Some(authorKnuth)), authorPassKnuth),
                (Some(authorFeynmanQaid, Some(authorFeynman)), authorPassFeynman),
                (Some(bookSilmarillionQaid, Some(bookSilmarillion)), bookPassSilmarillion),
                (Some(bookHobbitQaid, Some(bookHobbit)), bookPassHobbit),
                (Some(bookRingsQaid, Some(bookRings)), bookPassRings),
                (Some(bookOrientQaid, Some(bookOrient)), bookPassOrient),
                (Some(bookNileQaid, Some(bookNile)), bookPassNile),
                (Some(bookComputerQaid, Some(bookComputer)), bookPassComputer),
                (Some(bookEasyQaid, Some(bookEasy)), bookPassEasy),
                (Some(shelfFictionQaid, Some(shelfFiction)), shelfPassFiction),
                (Some(shelfNonFictionQaid, Some(shelfNonFiction)), shelfPassNonFiction),
                (Some(receiptS1Qaid, Some(receiptS1)), receiptPassS1),
                (Some(receiptS2Qaid, Some(receiptS2)), receiptPassS2),
                (Some(receiptEQaid, Some(receiptE)), receiptPassE)
            )

            src.persistDomainMessages(stateStore)
                .linkDomainMessages(relationsConfig, stateStore)
                .triggerAffectedCodomainMessages(relationsConfig, stateStore)
                .deduplicateCodomainMessages(Some(CodomainDeduplicationConfig(1, 50.millis)))
                .composeCodomainMessages(stateStore)
                .runDrain()

            val out =
                Flow.fromValues((Seq(bookIdSilmarillion), Seq(bookPassSilmarillion)))
                    .inlineDomainMessages(relationsConfig, stateStore)
                    .runToList()
                    .flatMap(_._1)
                    .map(_._2)

            // then
            val expectedNestedDocument: JsonNode = mapper.readTree("""
                {
                    "id": "f998258d-5081-4b20-b41d-865134b80eb2",
                    "categoryId": "327",
                    "authorId": "f3d2b210-7391-40c1-92bd-370caddd59b6",
                    "title": "The Silmarillion",
                    "isbn13": "9780008537890",
                    "price": {
                        "amount": 56,
                        "currency": "EUR"
                    },
                    "Authors/Author": [
                        {
                            "id": "f3d2b210-7391-40c1-92bd-370caddd59b6",
                            "name": "J. R. R. Tolkien",
                            "dateOfBirth": "1892-01-03"
                        }
                    ],
                    "Categories/Category": [
                        {
                            "id": "327",
                            "name": "Fantasy",
                            "shelfId": "FI",
                            "Storage/Shelf": [
                                {
                                    "id": "FI",
                                    "name": "Fictional Literature"
                                }
                            ]
                        }
                    ],
                    "Storage/Receipt": [
                        {
                            "id": "cdc22a3c-2e16-4751-8c21-2534119cd692",
                            "bookId": "f998258d-5081-4b20-b41d-865134b80eb2",
                            "shelfId": "FI",
                            "qty": 10,
                            "Storage/Shelf": [
                                {
                                    "id": "FI",
                                    "name": "Fictional Literature"
                                }
                            ]
                        },
                        {
                            "id": "25a0198e-9d0b-4139-8283-6b01490843b4",
                            "bookId": "f998258d-5081-4b20-b41d-865134b80eb2",
                            "shelfId": "FI",
                            "qty": 5,
                            "Storage/Shelf": [
                                {
                                    "id": "FI",
                                    "name": "Fictional Literature"
                                }
                            ]
                        }
                    ]
                }
            """)

            assert(out.head.map(_.toJson) == Some(expectedNestedDocument))
        }

        { // when
            val out =
                Flow.fromValues((Seq(bookIdComputer), Seq(bookPassComputer)))
                    .inlineDomainMessages(relationsConfig, stateStore)
                    .runToList()
                    .flatMap(_._1)
                    .map(_._2)

            // then
            val expectedNestedDocument: JsonNode = mapper.readTree("""
                {
                    "id": "e92c5192-9093-43de-b92f-d68725b0eb9d",
                    "categoryId": "100",
                    "authorId": "dc20395a-dca9-407b-8478-70287c51253b",
                    "title": "The Art of Computer Programming",
                    "isbn13": "978-0137935109",
                    "price": {
                        "amount": 276.99,
                        "currency": "USD"
                    },
                    "Authors/Author": [
                        {
                            "id": "dc20395a-dca9-407b-8478-70287c51253b",
                            "name": "Donald E. Knuth",
                            "dateOfBirth": "1938-01-10"
                        }
                    ],
                    "Categories/Category": [
                        {
                            "id": "100",
                            "name": "Scientific",
                            "shelfId": "NF",
                            "Storage/Shelf": [
                                {
                                    "id": "NF",
                                    "name": "Nonfictional Literature"
                                }
                            ]
                        }
                    ]
                }
            """)

            assert(out.head.map(_.toJson) == Some(expectedNestedDocument))
        }

        { // when
            val out =
                Flow.fromValues((Seq.empty, Seq(mockedEmptyKafkaRecord(id = "123"))))
                    .inlineDomainMessages(relationsConfig, stateStore)
                    .runToList()
                    .flatMap(_._1)
                    .map(_._2)

            // then
            assert(out.isEmpty)
        }

    it should "not create aggregate root document in nested structure when root document is missing" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryIdFantasy: MessageId = setupCategoryIdFantasy
        val categoryFantasyQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryIdFantasy)
        val categoryFantasy: Message = setupCategoryFantasy
        val categoryPassFantasy: Passthrough = mockedKafkaRecord(categoryIdFantasy.toString, categoryFantasy.toJson)

        val categoryIdMystery: MessageId = setupCategoryIdMystery
        val categoryMysteryQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryIdMystery)
        val categoryMystery: Message = setupCategoryMystery
        val categoryPassMystery: Passthrough = mockedKafkaRecord(categoryIdMystery.toString, categoryMystery.toJson)

        val categoryIdScientific: MessageId = setupCategoryIdScientific
        val categoryScientificQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryIdScientific)
        val categoryScientific: Message = setupCategoryScientific
        val categoryPassScientific: Passthrough =
            mockedKafkaRecord(categoryIdScientific.toString, categoryScientific.toJson)

        val authorIdTolkien: MessageId = setupAuthorIdTolkien
        val authorTolkienQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorIdTolkien)
        val authorTolkien: Message = setupAuthorTolkien
        val authorPassTolkien: Passthrough = mockedKafkaRecord(authorIdTolkien.toString, authorTolkien.toJson)

        val authorIdChristie: MessageId = setupAuthorIdChristie
        val authorChristieQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorIdChristie)
        val authorChristie: Message = setupAuthorCristie
        val authorPassChristie: Passthrough = mockedKafkaRecord(authorIdChristie.toString, authorChristie.toJson)

        val authorIdKnuth: MessageId = setupAuthorIdKnuth
        val authorKnuthQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorIdKnuth)
        val authorKnuth: Message = setupAuthorKnuth
        val authorPassKnuth: Passthrough = mockedKafkaRecord(authorIdKnuth.toString, authorKnuth.toJson)

        val authorIdFeynman: MessageId = setupAuthorIdFeynman
        val authorFeynmanQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorIdFeynman)
        val authorFeynman: Message = setupAuthorFeynman
        val authorPassFeynman: Passthrough = mockedKafkaRecord(authorIdFeynman.toString, authorFeynman.toJson)

        val bookIdSilmarillion: MessageId = setupBookIdSilmarillion

        val shelfIdFiction: MessageId = setupShelfIdFiction
        val shelfFictionQaid = QualifiedMessageId(storageChannel, shelfMessageFormat, shelfIdFiction)
        val shelfFiction: Message = setupShelfFiction
        val shelfPassFiction: Passthrough = mockedKafkaRecord(shelfIdFiction.toString, shelfFiction.toJson)

        val shelfIdNonFiction: MessageId = setupShelfIdNonFiction
        val shelfNonFictionQaid = QualifiedMessageId(storageChannel, shelfMessageFormat, shelfIdNonFiction)
        val shelfNonFiction: Message = setupShelfNonFiction
        val shelfPassNonFiction: Passthrough = mockedKafkaRecord(shelfIdNonFiction.toString, shelfNonFiction.toJson)

        val receiptIdS1: MessageId = setupReceiptIdSilmarillion1
        val receiptS1Qaid = QualifiedMessageId(storageChannel, receiptMessageFormat, receiptIdS1)
        val receiptS1: Message = setupReceiptSilmarillion1
        val receiptPassS1: Passthrough = mockedKafkaRecord(receiptIdS1.toString, receiptS1.toJson)

        val receiptIdS2: MessageId = setupReceiptIdSilmarillion2
        val receiptS2Qaid = QualifiedMessageId(storageChannel, receiptMessageFormat, receiptIdS2)
        val receiptS2: Message = setupReceiptSilmarillion2
        val receiptPassS2: Passthrough = mockedKafkaRecord(receiptIdS2.toString, receiptS2.toJson)

        val receiptIdE: MessageId = setupReceiptIdEasy
        val receiptEQaid = QualifiedMessageId(storageChannel, receiptMessageFormat, receiptIdE)
        val receiptE: Message = setupReceiptEasy
        val receiptPassE: Passthrough = mockedKafkaRecord(receiptIdE.toString, receiptE.toJson)

        val relationsConfig = setupRelationsConfig()

        // when
        val src = Flow.fromValues(
            (Some(categoryFantasyQaid, Some(categoryFantasy)), categoryPassFantasy),
            (Some(categoryMysteryQaid, Some(categoryMystery)), categoryPassMystery),
            (Some(categoryScientificQaid, Some(categoryScientific)), categoryPassScientific),
            (Some(authorTolkienQaid, Some(authorTolkien)), authorPassTolkien),
            (Some(authorChristieQaid, Some(authorChristie)), authorPassChristie),
            (Some(authorKnuthQaid, Some(authorKnuth)), authorPassKnuth),
            (Some(authorFeynmanQaid, Some(authorFeynman)), authorPassFeynman),
            (Some(shelfFictionQaid, Some(shelfFiction)), shelfPassFiction),
            (Some(shelfNonFictionQaid, Some(shelfNonFiction)), shelfPassNonFiction),
            (Some(receiptS1Qaid, Some(receiptS1)), receiptPassS1),
            (Some(receiptS2Qaid, Some(receiptS2)), receiptPassS2),
            (Some(receiptEQaid, Some(receiptE)), receiptPassE)
        )

        src.persistDomainMessages(stateStore)
            .linkDomainMessages(relationsConfig, stateStore)
            .triggerAffectedCodomainMessages(relationsConfig, stateStore)
            .deduplicateCodomainMessages(Some(CodomainDeduplicationConfig(1, 50.millis)))
            .composeCodomainMessages(stateStore)
            .runDrain()

        val out =
            Flow.fromValues((Seq(bookIdSilmarillion), Seq(receiptPassS1)))
                .inlineDomainMessages(relationsConfig, stateStore)
                .runToList()
                .map(_._1)
                .head

        assert(out == Seq(bookIdSilmarillion -> None))
