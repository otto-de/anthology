package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import de.otto.anthology.CodomainPersistenceStage.persistCodomainMessages
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.TestUtils.*
import de.otto.anthology.statestore.StateStoreSection
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class CodomainPersistenceStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "CodomainPersistenceStage" should "persist an outgoing Codomain Aggregate" in:

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
            (Seq((bookId, Some(book))), Seq(pass))
        )
        val out = src.persistCodomainMessages(stateStore).runToList()

        // then
        assert(stateStore.getJson(s"${StateStoreSection.COD}/$bookId") == Some(book.toJson))
        assert(out.size == 1)
        assert(out(0)._1 == Seq((bookId, Some(book))))
        assert(out(0)._2 == Seq(pass))

    it should "persist multiple outgoing Codomain Aggregates" in:

        // given
        val stateStore = InMemoryStateStore()

        val categoryId = MessageId("327")
        val authorId = MessageId("f3d2b210-7391-40c1-92bd-370caddd59b6")

        val bookId1 = MessageId("f998258d-5081-4b20-b41d-865134b80eb2")
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
            (
                Seq((bookId1, Some(book1)), (bookId2, Some(book2)), (bookId3, Some(book3))),
                Seq(bookPass1, bookPass2, bookPass3)
            )
        )
        val out = src.persistCodomainMessages(stateStore).runToList()

        // then
        assert(stateStore.getJson(s"${StateStoreSection.COD}/$bookId1") == Some(book1.toJson))
        assert(stateStore.getJson(s"${StateStoreSection.COD}/$bookId2") == Some(book2.toJson))
        assert(stateStore.getJson(s"${StateStoreSection.COD}/$bookId3") == Some(book3.toJson))
        assert(out.size == 1)
        assert(out(0)._1 == Seq((bookId1, Some(book1)), (bookId2, Some(book2)), (bookId3, Some(book3))))
        assert(out(0)._2 == Seq(bookPass1, bookPass2, bookPass3))

    it should "delete Codomain Aggregate when deletion arrives" in:

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

        // when (1)
        val src = Flow.fromValues(
            (Seq((bookId, Some(book))), Seq(pass))
        )
        src.persistCodomainMessages(stateStore).runDrain()

        // then (1)
        assert(stateStore.getJson(s"${StateStoreSection.COD}/$bookId") == Some(book.toJson))

        // when (2)
        val src2 = Flow.fromValues(
            (Seq((bookId, None)), Seq(pass))
        )
        val out = src2.persistCodomainMessages(stateStore).runToList()

        // then (2)
        assert(stateStore.getJson(s"${StateStoreSection.COD}/$bookId") == None)
        assert(out(0)._1 == Seq((bookId, None)))
        assert(out(0)._2 == Seq(pass))
