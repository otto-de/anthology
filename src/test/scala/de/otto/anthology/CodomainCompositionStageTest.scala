package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.CodomainCompositionStage.composeCodomainAggregates
import de.otto.anthology.CodomainDeduplicationConfig
import de.otto.anthology.CodomainDeduplicationStage.deduplicateCodomainAggregates
import de.otto.anthology.CodomainTriggeringStage.triggerAffectedCodomainAggregates
import de.otto.anthology.DomainLinkingStage.linkDomainAggregates
import de.otto.anthology.DomainPersistenceStage.persistDomainAggregates
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.TestData.*
import de.otto.anthology.TestUtils.InMemoryStateStore
import de.otto.anthology.TestUtils.mockedKafkaRecord
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStoreSection
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

import scala.concurrent.duration.*

class CodomainCompositionStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "CodomainCompositionStage" should "compose aggregate root staging document" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryIdFantasy: AggregateId = setupCategoryIdFantasy
        val categoryFantasyQaid = QualifiedAggregateId(categoriesDomain, categoryAggregate, categoryIdFantasy)
        val categoryFantasy: Aggregate = setupCategoryFantasy
        val categoryPassFantasy: Passthrough = mockedKafkaRecord(categoryIdFantasy.toString, categoryFantasy.toJson)

        val categoryIdMystery: AggregateId = setupCategoryIdMystery
        val categoryMysteryQaid = QualifiedAggregateId(categoriesDomain, categoryAggregate, categoryIdMystery)
        val categoryMystery: Aggregate = setupCategoryMystery
        val categoryPassMystery: Passthrough = mockedKafkaRecord(categoryIdMystery.toString, categoryMystery.toJson)

        val categoryIdScientific: AggregateId = setupCategoryIdScientific
        val categoryScientificQaid = QualifiedAggregateId(categoriesDomain, categoryAggregate, categoryIdScientific)
        val categoryScientific: Aggregate = setupCategoryScientific
        val categoryPassScientific: Passthrough =
            mockedKafkaRecord(categoryIdScientific.toString, categoryScientific.toJson)

        val authorIdTolkien: AggregateId = setupAuthorIdTolkien
        val authorTolkienQaid = QualifiedAggregateId(authorsDomain, authorAggregate, authorIdTolkien)
        val authorTolkien: Aggregate = setupAuthorTolkien
        val authorPassTolkien: Passthrough = mockedKafkaRecord(authorIdTolkien.toString, authorTolkien.toJson)

        val authorIdChristie: AggregateId = setupAuthorIdChristie
        val authorChristieQaid = QualifiedAggregateId(authorsDomain, authorAggregate, authorIdChristie)
        val authorChristie: Aggregate = setupAuthorCristie
        val authorPassChristie: Passthrough = mockedKafkaRecord(authorIdChristie.toString, authorChristie.toJson)

        val authorIdKnuth: AggregateId = setupAuthorIdKnuth
        val authorKnuthQaid = QualifiedAggregateId(authorsDomain, authorAggregate, authorIdKnuth)
        val authorKnuth: Aggregate = setupAuthorKnuth
        val authorPassKnuth: Passthrough = mockedKafkaRecord(authorIdKnuth.toString, authorKnuth.toJson)

        val authorIdFeynman: AggregateId = setupAuthorIdFeynman
        val authorFeynmanQaid = QualifiedAggregateId(authorsDomain, authorAggregate, authorIdFeynman)
        val authorFeynman: Aggregate = setupAuthorFeynman
        val authorPassFeynman: Passthrough = mockedKafkaRecord(authorIdFeynman.toString, authorFeynman.toJson)

        val bookIdSilmarillion: AggregateId = setupBookIdSilmarillion
        val bookSilmarillionQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookIdSilmarillion)
        val bookSilmarillion: Aggregate = setupBookSilmarillion
        val bookPassSilmarillion: Passthrough = mockedKafkaRecord(bookIdSilmarillion.toString, bookSilmarillion.toJson)

        val bookIdHobbit: AggregateId = setupBookIdHobbit
        val bookHobbitQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookIdHobbit)
        val bookHobbit: Aggregate = setupBookHobbit
        val bookPassHobbit: Passthrough = mockedKafkaRecord(bookIdHobbit.toString, bookHobbit.toJson)

        val bookIdRings: AggregateId = setupBookIdRings
        val bookRingsQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookIdRings)
        val bookRings: Aggregate = setupBookRings()
        val bookPassRings: Passthrough = mockedKafkaRecord(bookIdRings.toString, bookRings.toJson)

        val bookIdOrient: AggregateId = setupBookIdOrientExpress
        val bookOrientQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookIdOrient)
        val bookOrient: Aggregate = setupBookOrientExpress
        val bookPassOrient: Passthrough = mockedKafkaRecord(bookIdOrient.toString, bookOrient.toJson)

        val bookIdNile: AggregateId = setupBookIdNile
        val bookNileQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookIdNile)
        val bookNile: Aggregate = setupBookNile
        val bookPassNile: Passthrough = mockedKafkaRecord(bookIdNile.toString, bookNile.toJson)

        val bookIdComputer: AggregateId = setupBookIdComputer
        val bookComputerQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookIdComputer)
        val bookComputer: Aggregate = setupBookComputer
        val bookPassComputer: Passthrough = mockedKafkaRecord(bookIdComputer.toString, bookComputer.toJson)

        val bookIdEasy: AggregateId = setupBookIdEasy
        val bookEasyQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookIdEasy)
        val bookEasy: Aggregate = setupBookEasy
        val bookPassEasy: Passthrough = mockedKafkaRecord(bookIdEasy.toString, bookEasy.toJson)

        val shelfIdFiction: AggregateId = setupShelfIdFiction
        val shelfFictionQaid = QualifiedAggregateId(storageDomain, shelfAggregate, shelfIdFiction)
        val shelfFiction: Aggregate = setupShelfFiction
        val shelfPassFiction: Passthrough = mockedKafkaRecord(shelfIdFiction.toString, shelfFiction.toJson)

        val shelfIdNonFiction: AggregateId = setupShelfIdNonFiction
        val shelfNonFictionQaid = QualifiedAggregateId(storageDomain, shelfAggregate, shelfIdNonFiction)
        val shelfNonFiction: Aggregate = setupShelfNonFiction
        val shelfPassNonFiction: Passthrough = mockedKafkaRecord(shelfIdNonFiction.toString, shelfNonFiction.toJson)

        val receiptIdS1: AggregateId = setupReceiptIdSilmarillion1
        val receiptS1Qaid = QualifiedAggregateId(storageDomain, receiptAggregate, receiptIdS1)
        val receiptS1: Aggregate = setupReceiptSilmarillion1
        val receiptPassS1: Passthrough = mockedKafkaRecord(receiptIdS1.toString, receiptS1.toJson)

        val receiptIdS2: AggregateId = setupReceiptIdSilmarillion2
        val receiptS2Qaid = QualifiedAggregateId(storageDomain, receiptAggregate, receiptIdS2)
        val receiptS2: Aggregate = setupReceiptSilmarillion2
        val receiptPassS2: Passthrough = mockedKafkaRecord(receiptIdS2.toString, receiptS2.toJson)

        val receiptIdE: AggregateId = setupReceiptIdEasy
        val receiptEQaid = QualifiedAggregateId(storageDomain, receiptAggregate, receiptIdE)
        val receiptE: Aggregate = setupReceiptEasy
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

            val outPLI =
                src.persistDomainAggregates(stateStore)
                    .linkDomainAggregates(relationsConfig, stateStore)
                    .triggerAffectedCodomainAggregates(relationsConfig, stateStore)
                    .deduplicateCodomainAggregates(Some(CodomainDeduplicationConfig(1, 50.millis)))
                    .runToList()

            val out: List[(Seq[AggregateId], Seq[Passthrough])] =
                Flow.fromIterable(outPLI).composeCodomainAggregates(stateStore).runToList()

            // then
            assert(out.size == 19)

            val expectedStageDocument: JsonNode = mapper.readTree("""
                {
                    "Media/Book": {
                        "f998258d-5081-4b20-b41d-865134b80eb2": {
                            "id": "f998258d-5081-4b20-b41d-865134b80eb2",
                            "categoryId": "327",
                            "authorId": "f3d2b210-7391-40c1-92bd-370caddd59b6",
                            "title": "The Silmarillion",
                            "isbn13": "9780008537890",
                            "price": {
                                "amount": 56,
                                "currency": "EUR"
                            }
                        }
                    },
                    "Authors/Author": {
                        "f3d2b210-7391-40c1-92bd-370caddd59b6": {
                            "id": "f3d2b210-7391-40c1-92bd-370caddd59b6",
                            "name": "J. R. R. Tolkien",
                            "dateOfBirth": "1892-01-03"
                        }
                    },
                    "Categories/Category": {
                        "327": {
                            "id": "327",
                            "name": "Fantasy",
                            "shelfId": "FI"
                        }
                    },
                    "Storage/Shelf": {
                        "FI": {
                            "id": "FI",
                            "name": "Fictional Literature"
                        }
                    },
                    "Storage/Receipt": {
                        "cdc22a3c-2e16-4751-8c21-2534119cd692": {
                            "id": "cdc22a3c-2e16-4751-8c21-2534119cd692",
                            "bookId": "f998258d-5081-4b20-b41d-865134b80eb2",
                            "shelfId": "FI",
                            "qty": 10
                        },
                        "25a0198e-9d0b-4139-8283-6b01490843b4": {
                            "id": "25a0198e-9d0b-4139-8283-6b01490843b4",
                            "bookId": "f998258d-5081-4b20-b41d-865134b80eb2",
                            "shelfId": "FI",
                            "qty": 5
                        }
                    }
                }
            """)

            assert(stateStore.getJson(s"${StateStoreSection.STA}/$bookIdSilmarillion").get == expectedStageDocument)

        }

        { // when aggregates are missing
            val src = Flow.fromValues(
                (Some(authorTolkienQaid, None), authorPassTolkien),
                (Some(receiptS1Qaid, None), receiptPassS1)
            )

            val outPLI =
                src.persistDomainAggregates(stateStore)
                    .linkDomainAggregates(relationsConfig, stateStore)
                    .triggerAffectedCodomainAggregates(relationsConfig, stateStore)
                    .deduplicateCodomainAggregates(Some(CodomainDeduplicationConfig(1, 50.millis)))
                    .runToList()

            val out: List[(Seq[AggregateId], Seq[Passthrough])] =
                Flow.fromIterable(outPLI).composeCodomainAggregates(stateStore).runToList()

            // then
            assert(out.size == 2)

            val expectedStageDocument: JsonNode = mapper.readTree("""
                {
                    "Media/Book": {
                        "f998258d-5081-4b20-b41d-865134b80eb2": {
                            "id": "f998258d-5081-4b20-b41d-865134b80eb2",
                            "categoryId": "327",
                            "authorId": "f3d2b210-7391-40c1-92bd-370caddd59b6",
                            "title": "The Silmarillion",
                            "isbn13": "9780008537890",
                            "price": {
                                "amount": 56,
                                "currency": "EUR"
                            }
                        }
                    },
                    "Categories/Category": {
                        "327": {
                            "id": "327",
                            "name": "Fantasy",
                            "shelfId": "FI"
                        }
                    },
                    "Storage/Shelf": {
                        "FI": {
                            "id": "FI",
                            "name": "Fictional Literature"
                        }
                    },
                    "Storage/Receipt": {
                        "25a0198e-9d0b-4139-8283-6b01490843b4": {
                            "id": "25a0198e-9d0b-4139-8283-6b01490843b4",
                            "bookId": "f998258d-5081-4b20-b41d-865134b80eb2",
                            "shelfId": "FI",
                            "qty": 5
                        }
                    }
                }
            """)

            assert(stateStore.getJson(s"${StateStoreSection.STA}/$bookIdSilmarillion").get == expectedStageDocument)
        }
