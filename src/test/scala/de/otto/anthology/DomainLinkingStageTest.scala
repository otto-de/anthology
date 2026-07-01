package de.otto.anthology

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
        val bookQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId)
        val book = setupBookRings()
        val bookPass = mockedKafkaRecord(bookId.toString, book.toJson)

        Flow.fromValues((Some(bookQaid, Some(book)), bookPass))
            .persistDomainMessages(stateStore)
            .runToList()

        val config = setupRelationsConfig()

        // when
        val src: Flow[(Option[(QualifiedMessageId)], Passthrough)] =
            Flow.fromValues((Some(bookQaid), bookPass))

        val out: List[(Option[(QualifiedMessageId)], Passthrough)] =
            src.linkDomainMessages(config, stateStore).runToList()

        // then
        assert(out == List((Some(bookQaid), bookPass)))

        assert(stateStore.store.size == 4)

        val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
        assert(bookLNK.size == 2)
        assert(bookLNK.contains(s"$authorsChannel/$authorMessageFormat/$authorId"))
        assert(bookLNK.contains(s"$categoriesChannel/$categoryMessageFormat/$categoryId"))

        val authorBLK =
            stateStore.getStringSet(s"${StateStoreSection.BLK}/$authorsChannel/$authorMessageFormat/$authorId")
        assert(authorBLK.size == 1)
        assert(authorBLK.contains(bookQaid.toString))

        val categoriesBLK =
            stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoriesChannel/$categoryMessageFormat/$categoryId")
        assert(categoriesBLK.size == 1)
        assert(categoriesBLK.contains(bookQaid.toString))

    it should "compute and persist linking of one Aggregate (reference id is a number)" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryId = setupCategoryIdFantasy
        val authorId = setupAuthorIdTolkien

        val bookId = setupBookIdRings
        val bookQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId)
        val book = Message(mapper.readTree(s"""
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
            .persistDomainMessages(stateStore)
            .runToList()

        val config = setupRelationsConfig()

        // when
        val src: Flow[(Option[(QualifiedMessageId)], Passthrough)] =
            Flow.fromValues((Some(bookQaid), bookPass))

        val out: List[(Option[(QualifiedMessageId)], Passthrough)] =
            src.linkDomainMessages(config, stateStore).runToList()

        // then
        assert(out == List((Some(bookQaid), bookPass)))

        assert(stateStore.store.size == 4)

        val bookLNK = stateStore.getStringSet(s"${StateStoreSection.LNK}/$bookQaid")
        assert(bookLNK.size == 2)
        assert(bookLNK.contains(s"$authorsChannel/$authorMessageFormat/$authorId"))
        assert(bookLNK.contains(s"$categoriesChannel/$categoryMessageFormat/$categoryId"))

        val authorBLK =
            stateStore.getStringSet(s"${StateStoreSection.BLK}/$authorsChannel/$authorMessageFormat/$authorId")
        assert(authorBLK.size == 1)
        assert(authorBLK.contains(bookQaid.toString))

        val categoriesBLK =
            stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoriesChannel/$categoryMessageFormat/$categoryId")
        assert(categoriesBLK.size == 1)
        assert(categoriesBLK.contains(bookQaid.toString))

    it should "compute and persist linking of Aggregate deletions" in:

        // given
        val stateStore = InMemoryStateStore()

        val shelfIdFi = setupShelfIdFiction

        val categoryId = setupCategoryIdFantasy
        val categoryQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryId)
        val category = setupCategoryFantasy
        val categoryPass = mockedKafkaRecord(categoryId.toString, category.toJson)

        val authorId = setupAuthorIdTolkien
        val authorQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorId)
        val author = setupAuthorTolkien
        val authorPass = mockedKafkaRecord(authorId.toString, author.toJson)

        val bookId = setupBookIdRings
        val bookQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId)
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
                .persistDomainMessages(stateStore)
                .runToList()

            val src: Flow[(Option[QualifiedMessageId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedMessageId], Passthrough)] =
                src.linkDomainMessages(config, stateStore)
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
            assert(categoriesLNK.contains(s"$storageChannel/$shelfMessageFormat/$shelfIdFi"))

            val categoriesBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoryQaid")
            assert(categoriesBLK.size == 1)
            assert(categoriesBLK.contains(bookQaid.toString))

            val shelfBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$storageChannel/$shelfMessageFormat/$shelfIdFi")
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
                .persistDomainMessages(stateStore)
                .runToList()

            val src: Flow[(Option[QualifiedMessageId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedMessageId], Passthrough)] =
                src.linkDomainMessages(config, stateStore).runToList()

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
            assert(categoriesLNK.contains(s"$storageChannel/$shelfMessageFormat/$shelfIdFi"))

            val categoriesBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$categoryQaid")
            assert(categoriesBLK.size == 1)
            assert(categoriesBLK.contains(s"$bookQaid"))

            val shelfBLK =
                stateStore.getStringSet(s"${StateStoreSection.BLK}/$storageChannel/$shelfMessageFormat/$shelfIdFi")
            assert(shelfBLK.size == 1)
            assert(shelfBLK.contains(categoryQaid.toString))
        }

    it should "compute and persist linking of reference modification" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryIdFantasy = setupCategoryIdFantasy
        val categoryFantasyQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryIdFantasy)
        val categoryFantasy = setupCategoryFantasy
        val categoryFantasyPass = mockedKafkaRecord(categoryIdFantasy.toString, categoryFantasy.toJson)

        val categoryIdMystery = setupCategoryIdMystery
        val categoryMysteryQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryIdMystery)
        val categoryMystery = setupCategoryMystery
        val categoryMysteryPass = mockedKafkaRecord(categoryIdMystery.toString, categoryMystery.toJson)

        val authorId = setupAuthorIdTolkien
        val authorQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorId)
        val author = setupAuthorTolkien
        val authorPass = mockedKafkaRecord(authorId.toString, author.toJson)

        val bookId = setupBookIdRings
        val bookQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId)
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
                .persistDomainMessages(stateStore)
                .runToList()

            val src: Flow[(Option[QualifiedMessageId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedMessageId], Passthrough)] =
                src.linkDomainMessages(config, stateStore)
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
                .persistDomainMessages(stateStore) // persist modified aggregate
                .runToList()

            val src: Flow[(Option[QualifiedMessageId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedMessageId], Passthrough)] =
                src.linkDomainMessages(config, stateStore).runToList()

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
        val categoryFantasyQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryIdFantasy)
        val categoryFantasy = setupCategoryFantasy
        val categoryFantasyPass = mockedKafkaRecord(categoryIdFantasy.toString, categoryFantasy.toJson)

        val authorId = setupAuthorIdTolkien
        val authorQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorId)
        val author = setupAuthorTolkien
        val authorPass = mockedKafkaRecord(authorId.toString, author.toJson)

        val bookId = setupBookIdRings
        val bookQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId)
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
                .persistDomainMessages(stateStore)
                .runToList()

            val src: Flow[(Option[QualifiedMessageId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedMessageId], Passthrough)] =
                src.linkDomainMessages(config, stateStore)
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
                Message(mapper.readTree(s"""
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
                .persistDomainMessages(stateStore) // persist modified aggregate
                .runToList()

            val src: Flow[(Option[QualifiedMessageId], Passthrough)] =
                Flow.fromIterable(outP)

            val out: List[(Option[QualifiedMessageId], Passthrough)] =
                src.linkDomainMessages(config, stateStore).runToList()

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

            val outP = src.persistDomainMessages(stateStore).runToList()

            val out = Flow.fromIterable(outP).linkDomainMessages(relationsConfig, stateStore).runToList()

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

            val outP = src.persistDomainMessages(stateStore).runToList()

            val out = Flow.fromIterable(outP).linkDomainMessages(relationsConfig, stateStore).runToList()

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

            val outP = src.persistDomainMessages(stateStore).runToList()

            val out = Flow.fromIterable(outP).linkDomainMessages(relationsConfig, stateStore).runToList()

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

            val outP = src.persistDomainMessages(stateStore).runToList()

            val out = Flow.fromIterable(outP).linkDomainMessages(relationsConfig, stateStore).runToList()

            // the state store should be emptied
            assert(out.size == 16)
            assert(stateStore.store.isEmpty)
        }
