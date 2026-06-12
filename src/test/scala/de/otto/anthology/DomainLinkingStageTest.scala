package de.otto.anthology

import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.DomainLinkingStage.linkDomainAggregates
import de.otto.anthology.DomainPersistenceStage.persistDomainAggregates
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.TestData.*
import de.otto.anthology.TestUtils.InMemoryStateStore
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.TestUtils.mockedKafkaRecord
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStoreSection
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class DomainLinkingStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "DomainLinkingStage" should "compute and persist linking of one Aggregate" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryId = setupCategoryIdFantasy
        val authorId = setupAuthorIdTolkien

        val bookId = setupBookIdRings
        val bookQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookId)
        val book = setupBookRings()
        val bookPass = mockedKafkaRecord(bookId.toString, book.toJson)

        Flow.fromValues((Some(bookQaid, Some(book)), bookPass))
            .persistDomainAggregates(stateStore)
            .runToList()

        val config = setupRelationsConfig()

        // when
        val src: Flow[(Option[(QualifiedAggregateId)], Passthrough)] =
            Flow.fromValues((Some(bookQaid), bookPass))

        val out: List[(Option[(QualifiedAggregateId)], Passthrough)] =
            src.linkDomainAggregates(config, stateStore).runToList()

        // then
        assert(out == List((Some(bookQaid), bookPass)))

        assert(stateStore.store.size == 4)

        val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
        assert(bookLNK.size == 2)
        assert(bookLNK.contains(s"$authorsDomain/$authorAggregate/$authorId"))
        assert(bookLNK.contains(s"$categoriesDomain/$categoryAggregate/$categoryId"))

        val authorBLK = stateStore.getStringSet(s"${StateStoreSection.BLK}/$authorsDomain/$authorAggregate/$authorId")
        assert(authorBLK.size == 1)
        assert(authorBLK.contains(bookQaid.toString))

        val categoriesBLK =
            stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoriesDomain/$categoryAggregate/$categoryId")
        assert(categoriesBLK.size == 1)
        assert(categoriesBLK.contains(bookQaid.toString))

    it should "compute and persist linking of one Aggregate (reference id is a number)" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryId = setupCategoryIdFantasy
        val authorId = setupAuthorIdTolkien

        val bookId = setupBookIdRings
        val bookQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookId)
        val book = Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupBookIdRings",
                "categoryId": 327,
                "authorId": "$setupAuthorIdTolkien",
                "title": "The Lord of the Rings",
                "isbn13": "9783608960358",
                "price": { "amount": 90, "currency": "EUR"}
            }
            """))
        val bookPass = mockedKafkaRecord(bookId.toString, book.toJson)

        Flow.fromValues((Some(bookQaid, Some(book)), bookPass))
            .persistDomainAggregates(stateStore)
            .runToList()

        val config = setupRelationsConfig()

        // when
        val src: Flow[(Option[(QualifiedAggregateId)], Passthrough)] =
            Flow.fromValues((Some(bookQaid), bookPass))

        val out: List[(Option[(QualifiedAggregateId)], Passthrough)] =
            src.linkDomainAggregates(config, stateStore).runToList()

        // then
        assert(out == List((Some(bookQaid), bookPass)))

        assert(stateStore.store.size == 4)

        val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
        assert(bookLNK.size == 2)
        assert(bookLNK.contains(s"$authorsDomain/$authorAggregate/$authorId"))
        assert(bookLNK.contains(s"$categoriesDomain/$categoryAggregate/$categoryId"))

        val authorBLK = stateStore.getStringSet(s"${StateStoreSection.BLK}/$authorsDomain/$authorAggregate/$authorId")
        assert(authorBLK.size == 1)
        assert(authorBLK.contains(bookQaid.toString))

        val categoriesBLK =
            stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoriesDomain/$categoryAggregate/$categoryId")
        assert(categoriesBLK.size == 1)
        assert(categoriesBLK.contains(bookQaid.toString))

    it should "compute and persist linking of Aggregate deletions" in:

        // given
        val stateStore = InMemoryStateStore()

        val shelfIdFi = setupShelfIdFiction

        val categoryId = setupCategoryIdFantasy
        val categoryQaid = QualifiedAggregateId(categoriesDomain, categoryAggregate, categoryId)
        val category = setupCategoryFantasy
        val categoryPass = mockedKafkaRecord(categoryId.toString, category.toJson)

        val authorId = setupAuthorIdTolkien
        val authorQaid = QualifiedAggregateId(authorsDomain, authorAggregate, authorId)
        val author = setupAuthorTolkien
        val authorPass = mockedKafkaRecord(authorId.toString, author.toJson)

        val bookId = setupBookIdRings
        val bookQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookId)
        val book = setupBookRings()
        val bookPass = mockedKafkaRecord(bookId.toString, book.toJson)

        val config = setupRelationsConfig()

        {
            // when
            // 3 aggregates incoming
            val outP = Flow
                .fromValues(
                    (Some(categoryQaid, Some(category)), categoryPass),
                    (Some(authorQaid, Some(author)), authorPass),
                    (Some(bookQaid, Some(book)), bookPass)
                )
                .persistDomainAggregates(stateStore)
                .runToList()

            val src: Flow[(Option[QualifiedAggregateId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedAggregateId], Passthrough)] =
                src.linkDomainAggregates(config, stateStore)
                    .runToList()

            // then link 3 aggregates
            assert(
                out == List(
                    (Some(categoryQaid), categoryPass),
                    (Some(authorQaid), authorPass),
                    (Some(bookQaid), bookPass)
                )
            )

            assert(stateStore.store.size == 8)

            val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
            assert(bookLNK.size == 2)
            assert(bookLNK.contains(authorQaid.toString))
            assert(bookLNK.contains(categoryQaid.toString))

            val authorBLK = stateStore.getStringSet(s"${StateStoreSection.BLK}/$authorQaid")
            assert(authorBLK.size == 1)
            assert(authorBLK.contains(bookQaid.toString))

            val categoriesLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$categoryQaid")
            assert(categoriesLNK.size == 1)
            assert(categoriesLNK.contains(s"$storageDomain/$shelfAggregate/$shelfIdFi"))

            val categoriesBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoryQaid")
            assert(categoriesBLK.size == 1)
            assert(categoriesBLK.contains(bookQaid.toString))

            val shelfBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$storageDomain/$shelfAggregate/$shelfIdFi")
            assert(shelfBLK.size == 1)
            assert(shelfBLK.contains(categoryQaid.toString))
        }

        {
            // when
            // 2 deletions incoming
            val outP = Flow
                .fromValues(
                    (Some(authorQaid, None), authorPass), // author deletion
                    (Some(bookQaid, None), bookPass) // book deletion
                )
                .persistDomainAggregates(stateStore)
                .runToList()

            val src: Flow[(Option[QualifiedAggregateId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedAggregateId], Passthrough)] =
                src.linkDomainAggregates(config, stateStore).runToList()

            // then
            // delete the relations between the deleted aggregates
            assert(
                out == List(
                    (Some(authorQaid), authorPass),
                    (Some(bookQaid), bookPass)
                )
            )

            assert(stateStore.store.size == 5)

            val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
            assert(bookLNK.size == 1)
            assert(bookLNK.contains(categoryQaid.toString))

            val categoriesLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$categoryQaid")
            assert(categoriesLNK.size == 1)
            assert(categoriesLNK.contains(s"$storageDomain/$shelfAggregate/$shelfIdFi"))

            val categoriesBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoryQaid")
            assert(categoriesBLK.size == 1)
            assert(categoriesBLK.contains(s"$bookQaid"))

            val shelfBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$storageDomain/$shelfAggregate/$shelfIdFi")
            assert(shelfBLK.size == 1)
            assert(shelfBLK.contains(categoryQaid.toString))
        }

    it should "compute and persist linking of reference modification" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryIdFantasy = setupCategoryIdFantasy
        val categoryFantasyQaid = QualifiedAggregateId(categoriesDomain, categoryAggregate, categoryIdFantasy)
        val categoryFantasy = setupCategoryFantasy
        val categoryFantasyPass = mockedKafkaRecord(categoryIdFantasy.toString, categoryFantasy.toJson)

        val categoryIdMystery = setupCategoryIdMystery
        val categoryMysteryQaid = QualifiedAggregateId(categoriesDomain, categoryAggregate, categoryIdMystery)
        val categoryMystery = setupCategoryMystery
        val categoryMysteryPass = mockedKafkaRecord(categoryIdMystery.toString, categoryMystery.toJson)

        val authorId = setupAuthorIdTolkien
        val authorQaid = QualifiedAggregateId(authorsDomain, authorAggregate, authorId)
        val author = setupAuthorTolkien
        val authorPass = mockedKafkaRecord(authorId.toString, author.toJson)

        val bookId = setupBookIdRings
        val bookQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookId)
        val book = setupBookRings(categoryId = categoryIdMystery) // intentionally wrong! we will fix it later
        val bookPass = mockedKafkaRecord(bookId.toString, book.toJson)

        val config = setupRelationsConfig()

        {
            // when
            val outP = Flow
                .fromValues(
                    (Some(categoryFantasyQaid, Some(categoryFantasy)), categoryFantasyPass),
                    (Some(categoryMysteryQaid, Some(categoryMystery)), categoryMysteryPass),
                    (Some(authorQaid, Some(author)), authorPass),
                    (Some(bookQaid, Some(book)), bookPass)
                )
                .persistDomainAggregates(stateStore)
                .runToList()

            val src: Flow[(Option[QualifiedAggregateId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedAggregateId], Passthrough)] =
                src.linkDomainAggregates(config, stateStore)
                    .runToList()

            // then
            assert(
                out == List(
                    (Some(categoryFantasyQaid), categoryFantasyPass),
                    (Some(categoryMysteryQaid), categoryMysteryPass),
                    (Some(authorQaid), authorPass),
                    (Some(bookQaid), bookPass)
                )
            )

            assert(stateStore.store.size == 10)

            // book intentionally references Mystery category for now
            val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
            assert(bookLNK.size == 2)
            assert(bookLNK.contains(authorQaid.toString))
            assert(bookLNK.contains(categoryMysteryQaid.toString))

            val authorBLK = stateStore.getStringSet(s"${StateStoreSection.BLK}/$authorQaid")
            assert(authorBLK.size == 1)
            assert(authorBLK.contains(bookQaid.toString))

            val categoriesBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoryMysteryQaid")
            assert(categoriesBLK.size == 1)
            assert(categoriesBLK.contains(bookQaid.toString))
        }

        {
            // when
            // reference modification incoming
            val bookModified = setupBookRings(categoryId = categoryIdFantasy) // fixed reference to category
            val bookModifiedPass = mockedKafkaRecord(bookId.toString, bookModified.toJson)

            val outP = Flow
                .fromValues(
                    (Some(bookQaid, Some(bookModified)), bookModifiedPass)
                )
                .persistDomainAggregates(stateStore) // persist modified aggregate
                .runToList()

            val src: Flow[(Option[QualifiedAggregateId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedAggregateId], Passthrough)] =
                src.linkDomainAggregates(config, stateStore).runToList()

            // then
            // remove old reference and add new reference
            assert(out == List((Some(bookQaid), bookModifiedPass)))
            assert(stateStore.store.size == 10)

            // book references Fantasy category right now
            val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
            assert(bookLNK.size == 2)
            assert(bookLNK.contains(categoryFantasyQaid.toString))
            assert(bookLNK.contains(authorQaid.toString))

            val authorBLK = stateStore.getStringSet(s"${StateStoreSection.BLK}/$authorQaid")
            assert(authorBLK.size == 1)
            assert(authorBLK.contains(bookQaid.toString))

            val categoriesBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoryFantasyQaid")
            assert(categoriesBLK.size == 1)
            assert(categoriesBLK.contains(bookQaid.toString))
        }

    it should "compute and persist linking of reference removal" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryIdFantasy = setupCategoryIdFantasy
        val categoryFantasyQaid = QualifiedAggregateId(categoriesDomain, categoryAggregate, categoryIdFantasy)
        val categoryFantasy = setupCategoryFantasy
        val categoryFantasyPass = mockedKafkaRecord(categoryIdFantasy.toString, categoryFantasy.toJson)

        val authorId = setupAuthorIdTolkien
        val authorQaid = QualifiedAggregateId(authorsDomain, authorAggregate, authorId)
        val author = setupAuthorTolkien
        val authorPass = mockedKafkaRecord(authorId.toString, author.toJson)

        val bookId = setupBookIdRings
        val bookQaid = QualifiedAggregateId(mediaDomain, bookAggregate, bookId)
        val book = setupBookRings(categoryId = categoryIdFantasy)
        val bookPass = mockedKafkaRecord(bookId.toString, book.toJson)

        val config = setupRelationsConfig()

        {
            // when
            val outP = Flow
                .fromValues(
                    (Some(categoryFantasyQaid, Some(categoryFantasy)), categoryFantasyPass),
                    (Some(authorQaid, Some(author)), authorPass),
                    (Some(bookQaid, Some(book)), bookPass)
                )
                .persistDomainAggregates(stateStore)
                .runToList()

            val src: Flow[(Option[QualifiedAggregateId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedAggregateId], Passthrough)] =
                src.linkDomainAggregates(config, stateStore)
                    .runToList()

            // then
            assert(
                out == List(
                    (Some(categoryFantasyQaid), categoryFantasyPass),
                    (Some(authorQaid), authorPass),
                    (Some(bookQaid), bookPass)
                )
            )

            assert(stateStore.store.size == 8)

            val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
            assert(bookLNK.size == 2)
            assert(bookLNK.contains(authorQaid.toString))
            assert(bookLNK.contains(categoryFantasyQaid.toString))

            val authorBLK = stateStore.getStringSet(s"${StateStoreSection.BLK}/$authorQaid")
            assert(authorBLK.size == 1)
            assert(authorBLK.contains(bookQaid.toString))

            val categoriesBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoryFantasyQaid")
            assert(categoriesBLK.size == 1)
            assert(categoriesBLK.contains(bookQaid.toString))
        }

        {
            // when
            // reference removal incoming

            val bookModified =
                Aggregate(mapper.readTree(s"""
                    {
                        "id": "$setupBookIdRings",
                        "authorId": "$setupAuthorIdTolkien",
                        "title": "The Lord of the Rings",
                        "isbn13": "9783608960358",
                        "price": { "amount": 90, "currency": "Euro"}
                    }
                """))

            val bookModifiedPass = mockedKafkaRecord(bookId.toString, bookModified.toJson)

            val outP = Flow
                .fromValues(
                    (Some(bookQaid, Some(bookModified)), bookModifiedPass)
                )
                .persistDomainAggregates(stateStore) // persist modified aggregate
                .runToList()

            val src: Flow[(Option[QualifiedAggregateId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedAggregateId], Passthrough)] =
                src.linkDomainAggregates(config, stateStore).runToList()

            // then
            // remove reference from book to category
            assert(out == List((Some(bookQaid), bookModifiedPass)))

            assert(stateStore.store.size == 7)

            // book references Fantasy category right now
            val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
            assert(bookLNK.size == 1)
            assert(bookLNK.contains(authorQaid.toString))

            val authorBLK = stateStore.getStringSet(s"${StateStoreSection.BLK}/$authorQaid")
            assert(authorBLK.size == 1)
            assert(authorBLK.contains(bookQaid.toString))
        }

    it should "build a more complex graph" in:

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

            val outP = src.persistDomainAggregates(stateStore).runToList()

            val out = Flow.fromIterable(outP).linkDomainAggregates(relationsConfig, stateStore).runToList()

            // then
            assert(out.size == 19)

            assert(stateStore.store.size == 44)

            val bookLNK_Silmarillion =
                stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookSilmarillionQaid")
            assert(bookLNK_Silmarillion.size == 4)
            assert(bookLNK_Silmarillion.contains(authorTolkienQaid.toString))
            assert(bookLNK_Silmarillion.contains(categoryFantasyQaid.toString))
            assert(bookLNK_Silmarillion.contains(receiptS1Qaid.toString))
            assert(bookLNK_Silmarillion.contains(receiptS2Qaid.toString))

            val bookLNK_Hobbit =
                stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookHobbitQaid")
            assert(bookLNK_Hobbit.size == 2)
            assert(bookLNK_Hobbit.contains(authorTolkienQaid.toString))
            assert(bookLNK_Hobbit.contains(categoryFantasyQaid.toString))

            val receiptLNK_S1 =
                stateStore.getStringSet(s"${StateStoreSection.LNK}/$receiptS1Qaid")
            assert(receiptLNK_S1.size == 1)
            assert(receiptLNK_S1.contains(shelfFictionQaid.toString))

            val receiptBLK_S1 =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$receiptS1Qaid")
            assert(receiptBLK_S1.size == 1)
            assert(receiptBLK_S1.contains(bookSilmarillionQaid.toString))

            val receiptLNK_Mystery =
                stateStore.getStringSet(s"${StateStoreSection.LNK}/$categoryMysteryQaid")
            assert(receiptLNK_Mystery.size == 1)
            assert(receiptLNK_Mystery.contains(shelfFictionQaid.toString))

            val receiptBLK_Mystery =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoryMysteryQaid")
            assert(receiptBLK_Mystery.size == 2)
            assert(receiptBLK_Mystery.contains(bookOrientQaid.toString))
            assert(receiptBLK_Mystery.contains(bookNileQaid.toString))

            val shelfBLK_Fi =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$shelfFictionQaid")
            assert(shelfBLK_Fi.size == 4)
            assert(shelfBLK_Fi.contains(receiptS1Qaid.toString))
            assert(shelfBLK_Fi.contains(receiptS2Qaid.toString))
            assert(shelfBLK_Fi.contains(categoryFantasyQaid.toString))
            assert(shelfBLK_Fi.contains(categoryMysteryQaid.toString))
        }

        {
            // when deletion of some aggregates comes in
            val src = Flow.fromValues(
                (Some(receiptS1Qaid, None), mockedEmptyKafkaRecord(receiptIdS1.toString)),
                (Some(receiptEQaid, None), mockedEmptyKafkaRecord(receiptIdE.toString))
            )

            val outP = src.persistDomainAggregates(stateStore).runToList()

            val out = Flow.fromIterable(outP).linkDomainAggregates(relationsConfig, stateStore).runToList()

            // then only the aggregates should be deleted (because the opposite ends of the links are not deleted)
            assert(out.size == 2)
            assert(stateStore.store.size == 42)
            assert(stateStore.getJson(s"${StateStoreSection.DOM}/$receiptS1Qaid").isEmpty)
            assert(stateStore.getJson(s"${StateStoreSection.DOM}/$receiptEQaid").isEmpty)
        }

        {
            // when deletion of an aggregate (Shelf FI) comes in
            val src = Flow.fromValues(
                (Some(shelfFictionQaid, None), mockedEmptyKafkaRecord(shelfIdFiction.toString))
            )

            val outP = src.persistDomainAggregates(stateStore).runToList()

            val out = Flow.fromIterable(outP).linkDomainAggregates(relationsConfig, stateStore).runToList()

            // then the aggregate (Shelf FI) and its relation to the aggregate deleted before (Receipt S1) should be removed
            assert(out.size == 1)
            assert(stateStore.store.size == 40)
            assert(stateStore.getJson(s"${StateStoreSection.DOM}/$receiptS1Qaid").isEmpty)
            assert(stateStore.getJson(s"${StateStoreSection.DOM}/$receiptEQaid").isEmpty)
        }

        {
            // when deletions for all aggregates come in
            val src = Flow.fromValues(
                (Some(categoryFantasyQaid, None), mockedEmptyKafkaRecord(categoryIdFantasy.toString)),
                (Some(categoryMysteryQaid, None), mockedEmptyKafkaRecord(categoryIdMystery.toString)),
                (Some(categoryScientificQaid, None), mockedEmptyKafkaRecord(categoryIdScientific.toString)),
                (Some(authorTolkienQaid, None), mockedEmptyKafkaRecord(authorIdTolkien.toString)),
                (Some(authorChristieQaid, None), mockedEmptyKafkaRecord(authorIdChristie.toString)),
                (Some(authorKnuthQaid, None), mockedEmptyKafkaRecord(authorIdKnuth.toString)),
                (Some(authorFeynmanQaid, None), mockedEmptyKafkaRecord(authorIdFeynman.toString)),
                (Some(bookSilmarillionQaid, None), mockedEmptyKafkaRecord(bookIdSilmarillion.toString)),
                (Some(bookHobbitQaid, None), mockedEmptyKafkaRecord(bookIdHobbit.toString)),
                (Some(bookRingsQaid, None), mockedEmptyKafkaRecord(bookIdRings.toString)),
                (Some(bookOrientQaid, None), mockedEmptyKafkaRecord(bookIdOrient.toString)),
                (Some(bookNileQaid, None), mockedEmptyKafkaRecord(bookIdNile.toString)),
                (Some(bookComputerQaid, None), mockedEmptyKafkaRecord(bookIdComputer.toString)),
                (Some(bookEasyQaid, None), mockedEmptyKafkaRecord(bookIdEasy.toString)),
                (Some(shelfNonFictionQaid, None), mockedEmptyKafkaRecord(shelfIdNonFiction.toString)),
                (Some(receiptS2Qaid, None), mockedEmptyKafkaRecord(receiptIdS2.toString))
            )

            val outP = src.persistDomainAggregates(stateStore).runToList()

            val out = Flow.fromIterable(outP).linkDomainAggregates(relationsConfig, stateStore).runToList()

            // the state store should be emptied
            assert(out.size == 16)
            assert(stateStore.store.isEmpty)
        }
