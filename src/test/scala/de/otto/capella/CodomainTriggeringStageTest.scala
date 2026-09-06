package de.otto.capella

import de.otto.capella.CodomainTriggeringStage.triggerAffectedCodomainMessages
import de.otto.capella.DomainLinkingStage.linkDomainMessages
import de.otto.capella.DomainPersistenceStage.persistDomainMessages
import de.otto.capella.Message
import de.otto.capella.MessageId
import de.otto.capella.QualifiedMessageId
import de.otto.capella.TestData.*
import de.otto.capella.TestUtils.InMemoryStateStore
import de.otto.capella.TestUtils.mockedKafkaRecord
import de.otto.capella.kafka.Passthrough
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class CodomainIdentificationStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "CodomainIdentificationStage" should "identify no affected aggregate roots because aggregate is disconnected from any root" in:

        // given
        val stateStore = InMemoryStateStore()

        val shelfIdFiction: MessageId = setupShelfIdFiction
        val shelfFictionQaid = QualifiedMessageId(storageChannel, shelfMessageFormat, shelfIdFiction)
        val shelfFiction: Message = setupShelfFiction
        val shelfPassFiction: Passthrough = mockedKafkaRecord(shelfIdFiction.toString, shelfFiction.toJson)

        val relationsConfig = setupRelationsConfig()

        // when
        val src = Flow.fromValues((Some(shelfFictionQaid, Some(shelfFiction)), shelfPassFiction))

        val outPL =
            src.persistDomainMessages(stateStore).linkDomainMessages(relationsConfig, stateStore).runToList()

        val out: List[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
            Flow.fromIterable(outPL).triggerAffectedCodomainMessages(relationsConfig, stateStore).runToList()

        // then
        assert(out.size == 1)
        assert(out(0)._1 == Some(shelfFictionQaid, Set.empty))

    it should "identify the incoming aggregates the affected aggregate root" in:

        // given
        val stateStore = InMemoryStateStore()

        val bookIdHobbit: MessageId = setupBookIdHobbit
        val bookHobbitQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookIdHobbit)
        val bookHobbit: Message = setupBookHobbit
        val bookPassHobbit: Passthrough = mockedKafkaRecord(bookIdHobbit.toString, bookHobbit.toJson)

        val relationsConfig = setupRelationsConfig()

        // when
        val src = Flow.fromValues((Some(bookHobbitQaid, Some(bookHobbit)), bookPassHobbit))

        val outPL =
            src.persistDomainMessages(stateStore).linkDomainMessages(relationsConfig, stateStore).runToList()

        val out: List[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
            Flow.fromIterable(outPL).triggerAffectedCodomainMessages(relationsConfig, stateStore).runToList()

        // then
        assert(out.size == 1)
        assert(out(0)._1 == Some(bookHobbitQaid, Set(bookIdHobbit)))

    it should "identify different root aggregates in a more complex graph" in:

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

            val outPL =
                src.persistDomainMessages(stateStore).linkDomainMessages(relationsConfig, stateStore).runToList()

            val out: List[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
                Flow.fromIterable(outPL).triggerAffectedCodomainMessages(relationsConfig, stateStore).runToList()

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
                src.persistDomainMessages(stateStore).linkDomainMessages(relationsConfig, stateStore).runToList()

            val out: List[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
                Flow.fromIterable(outPL).triggerAffectedCodomainMessages(relationsConfig, stateStore).runToList()

            // then
            assert(out.size == 2)

            assert(out(0)._1.get._2 == Set(bookIdOrient, bookIdNile)) // all Christie books
            assert(out(1)._1.get._2 == Set(bookIdEasy))
        }
