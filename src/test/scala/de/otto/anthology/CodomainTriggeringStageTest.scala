package de.otto.anthology

import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.CodomainTriggeringStage.triggerAffectedCodomainAggregates
import de.otto.anthology.DomainLinkingStage.linkDomainAggregates
import de.otto.anthology.DomainPersistenceStage.persistDomainAggregates
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.TestData.*
import de.otto.anthology.TestUtils.InMemoryStateStore
import de.otto.anthology.TestUtils.mockedKafkaRecord
import de.otto.anthology.kafka.Passthrough
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class CodomainIdentificationStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "CodomainIdentificationStage" should "identify no affected aggregate roots because aggregate is disconnected from any root" in:

        // given
        val stateStore = InMemoryStateStore()

        val shelfIdFiction: AggregateId = setupShelfIdFiction
        val shelfFictionQaid = QualifiedAggregateId(storageDomain, shelfAggregate, shelfIdFiction)
        val shelfFiction: Aggregate = setupShelfFiction
        val shelfPassFiction: Passthrough = mockedKafkaRecord(shelfIdFiction.toString, shelfFiction.toJson)

        val relationsConfig = setupRelationsConfig()

        // when
        val src = Flow.fromValues((Some(shelfFictionQaid, Some(shelfFiction)), shelfPassFiction))

        val outPL =
            src.persistDomainAggregates(stateStore).linkDomainAggregates(relationsConfig, stateStore).runToList()

        val out: List[(Option[(QualifiedAggregateId, Set[AggregateId])], Passthrough)] =
            Flow.fromIterable(outPL).triggerAffectedCodomainAggregates(relationsConfig, stateStore).runToList()

        // then
        assert(out.size == 1)
        assert(out(0)._1 == Some(shelfFictionQaid, Set.empty))

    it should "identify the incoming aggregates the affected aggregate root" in:

        // given
        val stateStore = InMemoryStateStore()

        val bookIdHobbit: AggregateId = setupBookIdHobbit
        val bookHobbitQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookIdHobbit)
        val bookHobbit: Aggregate = setupBookHobbit
        val bookPassHobbit: Passthrough = mockedKafkaRecord(bookIdHobbit.toString, bookHobbit.toJson)

        val relationsConfig = setupRelationsConfig()

        // when
        val src = Flow.fromValues((Some(bookHobbitQaid, Some(bookHobbit)), bookPassHobbit))

        val outPL =
            src.persistDomainAggregates(stateStore).linkDomainAggregates(relationsConfig, stateStore).runToList()

        val out: List[(Option[(QualifiedAggregateId, Set[AggregateId])], Passthrough)] =
            Flow.fromIterable(outPL).triggerAffectedCodomainAggregates(relationsConfig, stateStore).runToList()

        // then
        assert(out.size == 1)
        assert(out(0)._1 == Some(bookHobbitQaid, Set(bookIdHobbit)))

    it should "identify different root aggregates in a more complex graph" in:

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

            val outPL =
                src.persistDomainAggregates(stateStore).linkDomainAggregates(relationsConfig, stateStore).runToList()

            val out: List[(Option[(QualifiedAggregateId, Set[AggregateId])], Passthrough)] =
                Flow.fromIterable(outPL).triggerAffectedCodomainAggregates(relationsConfig, stateStore).runToList()

            // then
            assert(out.size == 19)

            assert(out(0)._1.get._2 == Set(bookIdSilmarillion, bookIdHobbit, bookIdRings)) // all Fantasy books
            assert(out(1)._1.get._2 == Set(bookIdOrient, bookIdNile)) // all Mystery books
            assert(out(2)._1.get._2 == Set(bookIdComputer, bookIdEasy)) // all Scientific books
            assert(out(3)._1.get._2 == Set(bookIdSilmarillion, bookIdHobbit, bookIdRings)) // all Tolkien books
            assert(out(4)._1.get._2 == Set(bookIdOrient, bookIdNile)) // all Christie books
            assert(out(5)._1.get._2 == Set(bookIdComputer)) // all Knuth books
            assert(out(6)._1.get._2 == Set(bookIdEasy)) // all Feynman books
            assert(out(7)._1.get._2 == Set(bookIdSilmarillion))
            assert(out(8)._1.get._2 == Set(bookIdHobbit))
            assert(out(9)._1.get._2 == Set(bookIdRings))
            assert(out(10)._1.get._2 == Set(bookIdOrient))
            assert(out(11)._1.get._2 == Set(bookIdNile))
            assert(out(12)._1.get._2 == Set(bookIdComputer))
            assert(out(13)._1.get._2 == Set(bookIdEasy))
            assert(
                out(14)._1.get._2 == Set(bookIdSilmarillion, bookIdHobbit, bookIdRings, bookIdOrient, bookIdNile)
            ) // all the books on the fiction shelf
            assert(
                out(15)._1.get._2 == Set(bookIdComputer, bookIdEasy)
            ) // all the books on the non-fiction shelf
            assert(out(16)._1.get._2 == Set(bookIdSilmarillion)) // book referenced by receipt S1
            assert(out(17)._1.get._2 == Set(bookIdSilmarillion)) // book referenced by receipt S2
            assert(out(18)._1.get._2 == Set(bookIdEasy)) // book referenced by receipt E
        }

        { // when
            val src = Flow.fromValues(
                (Some(authorChristieQaid, Some(authorChristie)), authorPassChristie),
                (Some(bookEasyQaid, Some(bookEasy)), bookPassEasy)
            )

            val outPL =
                src.persistDomainAggregates(stateStore).linkDomainAggregates(relationsConfig, stateStore).runToList()

            val out: List[(Option[(QualifiedAggregateId, Set[AggregateId])], Passthrough)] =
                Flow.fromIterable(outPL).triggerAffectedCodomainAggregates(relationsConfig, stateStore).runToList()

            // then
            assert(out.size == 2)

            assert(out(0)._1.get._2 == Set(bookIdOrient, bookIdNile)) // all Christie books
            assert(out(1)._1.get._2 == Set(bookIdEasy))
        }
