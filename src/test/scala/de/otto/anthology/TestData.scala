package de.otto.anthology

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.config.ManyToOne
import de.otto.anthology.config.OneToMany
import de.otto.anthology.config.RelationConfigs

object TestData:

    val mediaChannel = ChannelName("Media")
    val authorsChannel = ChannelName("Authors")
    val storageChannel = ChannelName("Storage")
    val categoriesChannel = ChannelName("Categories")

    val bookMessageFormat = MessageFormatName("Book")
    val authorMessageFormat = MessageFormatName("Author")
    val receiptMessageFormat = MessageFormatName("Receipt")
    val shelfMessageFormat = MessageFormatName("Shelf")
    val categoryMessageFormat = MessageFormatName("Category")

    def setupRelationsConfig(): RelationConfigs =
        val books2authors = ManyToOne(
            relFrom = mediaChannel -> bookMessageFormat,
            relTo = authorsChannel -> authorMessageFormat,
            refFromManyToOnePath = JsonPath.compile("$.authorId")
        )

        val books2receipts = OneToMany(
            relFrom = mediaChannel -> bookMessageFormat,
            relTo = storageChannel -> receiptMessageFormat,
            refFromManyToOnePath = JsonPath.compile("$.bookId")
        )

        val receipts2shelves = ManyToOne(
            relFrom = storageChannel -> receiptMessageFormat,
            relTo = storageChannel -> shelfMessageFormat,
            refFromManyToOnePath = JsonPath.compile("$.shelfId")
        )

        val books2categories = ManyToOne(
            relFrom = mediaChannel -> bookMessageFormat,
            relTo = categoriesChannel -> categoryMessageFormat,
            refFromManyToOnePath = JsonPath.compile("$.categoryId")
        )

        val categories2shelves = ManyToOne(
            relFrom = categoriesChannel -> categoryMessageFormat,
            relTo = storageChannel -> shelfMessageFormat,
            refFromManyToOnePath = JsonPath.compile("$.shelfId")
        )

        RelationConfigs(
            Seq(books2authors, books2receipts, receipts2shelves, books2categories, categories2shelves)
        )

    def setupCategoryIdFantasy: MessageId = MessageId("327")

    def setupCategoryFantasy: Message =
        Message(mapper.readTree(s"""
            {   
                "id": "$setupCategoryIdFantasy",
                "name": "Fantasy",
                "shelfId": "FI"
            }
        """))

    def setupCategoryIdMystery: MessageId = MessageId("425")

    def setupCategoryMystery: Message =
        Message(mapper.readTree(s"""
            {   
                "id": "$setupCategoryIdMystery",
                "name": "Mystery", 
                "shelfId": "FI"
            }
        """))

    def setupCategoryIdScientific: MessageId = MessageId("100")

    def setupCategoryScientific: Message =
        Message(mapper.readTree(s"""
            {   
                "id": "$setupCategoryIdScientific",
                "name": "Scientific",
                "shelfId": "NF"
            }
        """))

    def setupAuthorIdTolkien: MessageId = MessageId("f3d2b210-7391-40c1-92bd-370caddd59b6")

    def setupAuthorTolkien: Message =
        Message(mapper.readTree(s"""
            {   
                "id": "$setupAuthorIdTolkien",
                "name": "J. R. R. Tolkien",
                "dateOfBirth": "1892-01-03"
            }
        """))

    def setupAuthorIdChristie: MessageId = MessageId("5d67fcb5-aab0-4e11-81a1-69c341d63ace")

    def setupAuthorCristie: Message =
        Message(mapper.readTree(s"""
            {   
                "id": "$setupAuthorIdChristie",
                "name": "Agatha Christie",
                "dateOfBirth": "1890-09-15"
            }
        """))

    def setupAuthorIdKnuth: MessageId = MessageId("dc20395a-dca9-407b-8478-70287c51253b")

    def setupAuthorKnuth: Message =
        Message(mapper.readTree(s"""
            {   
                "id": "$setupAuthorIdKnuth",
                "name": "Donald E. Knuth",
                "dateOfBirth": "1938-01-10"
            }
        """))

    def setupAuthorIdFeynman: MessageId = MessageId("ded8fb5c-d5cb-423f-861d-c7cf20d1fef6")

    def setupAuthorFeynman: Message =
        Message(mapper.readTree(s"""
            {   
                "id": "$setupAuthorIdKnuth",
                "name": "Richard P. Feynman",
                "dateOfBirth": "1918-05-11"
            }
        """))

    def setupShelfIdNonFiction: MessageId = MessageId("NF")

    def setupShelfNonFiction: Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupShelfIdNonFiction",
            "name": "Nonfictional Literature"
        }
        """))

    def setupShelfIdFiction: MessageId = MessageId("FI")

    def setupShelfFiction: Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupShelfIdFiction",
            "name": "Fictional Literature"
        }
        """))

    def setupBookIdSilmarillion: MessageId = MessageId("f998258d-5081-4b20-b41d-865134b80eb2")

    def setupBookSilmarillion: Message =
        Message(mapper.readTree(s"""
            {   
                "id": "$setupBookIdSilmarillion",
                "categoryId": "$setupCategoryIdFantasy",
                "authorId": "$setupAuthorIdTolkien",
                "title": "The Silmarillion",
                "isbn13": "9780008537890",
                "price": { "amount": 56, "currency": "EUR"}
            }
        """))

    def setupBookIdHobbit: MessageId = MessageId("31a94690-f53d-421f-8428-a14a06a8081b")

    def setupBookHobbit: Message =
        Message(mapper.readTree(s"""
            {   
                "id": "$setupBookIdHobbit",
                "categoryId": "$setupCategoryIdFantasy",
                "authorId": "$setupAuthorIdTolkien",
                "title": "The Hobbit",
                "isbn13": "9780261103283",
                "price": { "amount": 27.50, "currency": "EUR"}
            }
        """))

    def setupBookIdRings: MessageId = MessageId("610e3d5d-d41c-48b9-8f67-96ad3e383519")

    def setupBookRings(categoryId: MessageId = setupCategoryIdFantasy): Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupBookIdRings",
            "categoryId": "$categoryId",
            "authorId": "$setupAuthorIdTolkien",
            "title": "The Lord of the Rings",
            "isbn13": "9783608960358",
            "price": { "amount": 90, "currency": "EUR"}
        }
        """))

    def setupBookIdOrientExpress: MessageId = MessageId("9bd60be7-8dbe-4d56-8fb1-55ff49548b0f")

    def setupBookOrientExpress: Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupBookIdOrientExpress",
            "categoryId": "$setupCategoryIdMystery",
            "authorId": "$setupAuthorIdChristie",
            "title": "Murder on the Orient Express",
            "isbn13": "9780008226664",
            "price": { "amount": 19, "currency": "EUR"}
        }
        """))

    def setupBookIdNile: MessageId = MessageId("c33f2c1b-d604-4f6f-8fe0-6d827acc28d2")

    def setupBookNile: Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupBookIdNile",
            "categoryId": "$setupCategoryIdMystery",
            "authorId": "$setupAuthorIdChristie",
            "title": "Death on the Nile",
            "isbn13": "9780063375864",
            "price": { "amount": 21.5, "currency": "EUR"}
        }
        """))

    def setupBookIdComputer: MessageId = MessageId("e92c5192-9093-43de-b92f-d68725b0eb9d")

    def setupBookComputer: Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupBookIdComputer",
            "categoryId": "$setupCategoryIdScientific",
            "authorId": "$setupAuthorIdKnuth",
            "title": "The Art of Computer Programming",
            "isbn13": "978-0137935109",
            "price": { "amount": 276.99, "currency": "USD"}
        }
        """))

    def setupBookIdEasy: MessageId = MessageId("31cb1ec2-34d5-4cda-9866-11f8f3921b67")

    def setupBookEasy: Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupBookIdEasy",
            "categoryId": "$setupCategoryIdScientific",
            "authorId": "$setupAuthorIdFeynman",
            "title": "Six Easy Pieces",
            "isbn13": "9780465025275",
            "price": { "amount": 16.50, "currency": "EUR"}
        }
        """))

    def setupReceiptIdSilmarillion1: MessageId = MessageId("cdc22a3c-2e16-4751-8c21-2534119cd692")

    def setupReceiptSilmarillion1: Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupReceiptIdSilmarillion1",
            "bookId": "$setupBookIdSilmarillion",
            "shelfId": "$setupShelfIdFiction",
            "qty": 10
        }
        """))

    def setupReceiptIdSilmarillion2: MessageId = MessageId("25a0198e-9d0b-4139-8283-6b01490843b4")

    def setupReceiptSilmarillion2: Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupReceiptIdSilmarillion2",
            "bookId": "$setupBookIdSilmarillion",
            "shelfId": "$setupShelfIdFiction",
            "qty": 5
        }
        """))

    def setupReceiptIdEasy: MessageId = MessageId("6501d9f4-12a9-40da-89a2-a75da44b275c")

    def setupReceiptEasy: Message =
        Message(mapper.readTree(s"""
        {   
            "id": "$setupReceiptIdEasy",
            "bookId": "$setupBookIdEasy",
            "shelfId": "$setupShelfIdNonFiction",
            "qty": 20
        }
        """))
