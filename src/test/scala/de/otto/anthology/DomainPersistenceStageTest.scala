package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.TextNode
import de.otto.anthology.DomainPersistenceStage.persistDomainMessages
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.TestData.*
import de.otto.anthology.TestUtils.*
import de.otto.anthology.statestore.StateStoreSection
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class DomainPersistenceStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "DomainPersistenceStage" should "persist an incoming Domain Aggregate" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryId = MessageId("327")
        val authorId = MessageId("f3d2b210-7391-40c1-92bd-370caddd59b6")

        val bookId = MessageId("f998258d-5081-4b20-b41d-865134b80eb2")
        val book =
            Message(mapper.readTree(s"""
                {   
                    "id": "$bookId",
                    "categoryId": "$categoryId",
                    "authorId": "$authorId",
                    "title": "The Lord of the Rings",
                    "isbn13": "9783608960358",
                    "price": { "amount": 90, "currency": "Euro"}
                }
            """))
        val pass = mockedKafkaRecord(bookId.toString, book.toJson)

        // when
        val src = Flow.fromValues(
            (Some(QualifiedMessageId(mediaChannel, bookMessageFormat, bookId), Some(book)), pass)
        )
        val out = src.persistDomainMessages(stateStore).runToList()

        // then
        assert(
            stateStore.getJson(s"${StateStoreSection.DOM}/$mediaChannel/$bookMessageFormat/$bookId") == Some(
                book.toJson
            )
        )
        assert(out.size == 1)
        assert(out(0)._1 == Some(QualifiedMessageId(mediaChannel, bookMessageFormat, bookId)))
        assert(out(0)._2 == pass)

    it should "persist multiple incoming Domain Aggregates" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryId = MessageId("327")
        val categoryQaid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryId)
        val category =
            Message(mapper.readTree(s"""
                {   
                    "id": "$categoryId",
                    "name": "Fantasy"
                }
            """))
        val categoryPass = mockedKafkaRecord(categoryId.toString, category.toJson)

        val authorId = MessageId("f3d2b210-7391-40c1-92bd-370caddd59b6")
        val authorQaid = QualifiedMessageId(authorsChannel, authorMessageFormat, authorId)
        val author =
            Message(mapper.readTree(s"""
                {   
                    "id": "$authorId",
                    "name": "J. R. R. Tolkien",
                    "dateOfBirth": "1892-01-03"
                }
            """))
        val authorPass = mockedKafkaRecord(authorId.toString, author.toJson)

        val bookId1 = MessageId("f998258d-5081-4b20-b41d-865134b80eb2")
        val bookQaid1 = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId1)
        val book1 =
            Message(mapper.readTree(s"""
                {   
                    "id": "$bookId1",
                    "categoryId": "$categoryId",
                    "authorId": "$authorId",
                    "title": "The Silmarillion",
                    "isbn13": "9780008537890",
                    "price": { "amount": 56, "currency": "Euro"}
                }
            """))
        val bookPass1 = mockedKafkaRecord(bookId1.toString, book1.toJson)

        val bookId2 = MessageId("31a94690-f53d-421f-8428-a14a06a8081b")
        val bookQaid2 = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId2)
        val book2 =
            Message(mapper.readTree(s"""
                {   
                    "id": "$bookId2",
                    "categoryId": "$categoryId",
                    "authorId": "$authorId",
                    "title": "The Hobbit",
                    "isbn13": "9780261103283",
                    "price": { "amount": 27.50, "currency": "Euro"}
                }
            """))
        val bookPass2 = mockedKafkaRecord(bookId2.toString, book2.toJson)

        val bookId3 = MessageId("8103cf50-e5fd-45b3-9df8-f2554ef011ad")
        val bookQaid3 = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId3)
        val book3 =
            Message(mapper.readTree(s"""
                {   
                    "id": "$bookId3",
                    "categoryId": "$categoryId",
                    "authorId": "$authorId",
                    "title": "The Lord of the Rings",
                    "isbn13": "9783608960358",
                    "price": { "amount": 90, "currency": "Euro"}
                }
            """))
        val bookPass3 = mockedKafkaRecord(bookId3.toString, book3.toJson)

        // when
        val src = Flow.fromValues(
            (Some(categoryQaid, Some(category)), categoryPass),
            (Some(authorQaid, Some(author)), authorPass),
            (Some(bookQaid1, Some(book1)), bookPass1),
            (Some(bookQaid2, Some(book2)), bookPass2),
            (Some(bookQaid3, Some(book3)), bookPass3)
        )
        val out = src.persistDomainMessages(stateStore).runToList()

        // then
        assert(stateStore.getJson(s"${StateStoreSection.DOM}/$categoryQaid") == Some(category.toJson))
        assert(stateStore.getJson(s"${StateStoreSection.DOM}/$authorQaid") == Some(author.toJson))
        assert(stateStore.getJson(s"${StateStoreSection.DOM}/$bookQaid1") == Some(book1.toJson))
        assert(stateStore.getJson(s"${StateStoreSection.DOM}/$bookQaid2") == Some(book2.toJson))
        assert(stateStore.getJson(s"${StateStoreSection.DOM}/$bookQaid3") == Some(book3.toJson))
        assert(out.size == 5)
        assert(out(0)._1 == Some(categoryQaid))
        assert(out(0)._2 == categoryPass)
        assert(out(1)._1 == Some(authorQaid))
        assert(out(1)._2 == authorPass)
        assert(out(2)._1 == Some(bookQaid1))
        assert(out(2)._2 == bookPass1)
        assert(out(3)._1 == Some(bookQaid2))
        assert(out(3)._2 == bookPass2)
        assert(out(4)._1 == Some(bookQaid3))
        assert(out(4)._2 == bookPass3)

    it should "delete Domain Aggregate when deletion arrives" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryId = MessageId("327")
        val authorId = MessageId("f3d2b210-7391-40c1-92bd-370caddd59b6")

        val bookId = MessageId("f998258d-5081-4b20-b41d-865134b80eb2")
        val bookQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId)
        val book =
            Message(mapper.readTree(s"""
                {   
                    "id": "$bookId",
                    "categoryId": "$categoryId",
                    "authorId": "$authorId",
                    "title": "The Lord of the Rings",
                    "isbn13": "9783608960358",
                    "price": { "amount": 90, "currency": "Euro"}
                }
            """))
        val pass = mockedKafkaRecord(bookId.toString, book.toJson)

        // when (1)
        val src = Flow.fromValues(
            (Some(bookQaid, Some(book)), pass)
        )
        src.persistDomainMessages(stateStore).runDrain()

        // then (1)
        assert(stateStore.getJson(s"${StateStoreSection.DOM}/$bookQaid") == Some(book.toJson))

        // when (2)
        val src2 = Flow.fromValues(
            (Some(bookQaid, None), pass)
        )
        val out = src2.persistDomainMessages(stateStore).runToList()

        // then (2)
        assert(stateStore.getJson(s"${StateStoreSection.DOM}/$bookQaid") == None)
        assert(out(0)._1 == Some(bookQaid))
        assert(out(0)._2 == pass)

    it should "do nothing when empty data set arrives (error in previous stage)" in:
        // given
        val stateStore = InMemoryStateStore()

        val categoryId = MessageId("327")
        val authorId = MessageId("f3d2b210-7391-40c1-92bd-370caddd59b6")

        val bookId = MessageId("f998258d-5081-4b20-b41d-865134b80eb2")
        val bookQaid = QualifiedMessageId(mediaChannel, bookMessageFormat, bookId)
        val book =
            Message(mapper.readTree(s"""
                {   
                    "id": "$bookId",
                    "categoryId": "$categoryId",
                    "authorId": "$authorId",
                    "title": "The Lord of the Rings",
                    "isbn13": "9783608960358",
                    "price": { "amount": 90, "currency": "Euro"}
                }
            """))
        val pass = mockedKafkaRecord(bookId.toString, book.toJson)

        // when (1)
        val src = Flow.fromValues(
            (Some(bookQaid, Some(book)), pass)
        )
        src.persistDomainMessages(stateStore).runDrain()

        // then (1)
        assert(stateStore.getJson(s"${StateStoreSection.DOM}/$bookQaid") == Some(book.toJson))

        // when (2)
        val src2 = Flow.fromValues(
            (None, pass)
        )
        val out = src2.persistDomainMessages(stateStore).runToList()

        // then (2)
        assert(stateStore.getJson(s"${StateStoreSection.DOM}/$bookQaid") == Some(book.toJson))
        assert(out(0)._1 == None)
        assert(out(0)._2 == pass)

    it should "validate arriving data sets" in:
        // given
        val stateStore = InMemoryStateStore()

        val categoryInvalidId1 = MessageId("327/999")
        val categoryQaidInvalid1 = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryInvalidId1)
        val passInvalid1 = mockedKafkaRecord(categoryInvalidId1.toString, TextNode("foobar"))
        val categoryValidId = MessageId("327.999")
        val categoryQaidValid = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryValidId)
        val passValid = mockedKafkaRecord(categoryValidId.toString, TextNode("foobar"))
        val categoryInvalidId2 = MessageId("327;999")
        val categoryQaidInvalid2 = QualifiedMessageId(categoriesChannel, categoryMessageFormat, categoryInvalidId2)
        val passInvalid2 = mockedKafkaRecord(categoryInvalidId2.toString, TextNode("foobar"))

        // when
        val src = Flow.fromValues(
            (Some(categoryQaidInvalid1, None), passInvalid1),
            (Some(categoryQaidValid, None), passValid),
            (Some(categoryQaidInvalid2, None), passInvalid2)
        )
        val out = src.persistDomainMessages(stateStore).runToList()

        // then
        assert(out.size == 3)
        assert(out(0)._1 == None) // emits None (validation error)
        assert(out(0)._2 == passInvalid1)
        assert(out(1)._1 == (Some(categoryQaidValid))) // emits Some (no validation error)
        assert(out(1)._2 == passValid)
        assert(out(2)._1 == None) // emits None (validation error)
        assert(out(2)._2 == passInvalid2)
