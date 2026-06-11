package de.otto.anthology

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.config.DomainRelationConfigs
import de.otto.anthology.config.ManyToOneConfig
import de.otto.anthology.config.OneToManyConfig

object TestData:

    val mediaDomain = DomainName("Media")
    val authorsDomain = DomainName("Authors")
    val storageDomain = DomainName("Storage")
    val categoriesDomain = DomainName("Categories")

    val bookAggregate = AggregateName("Book")
    val authorAggregate = AggregateName("Author")
    val receiptAggregate = AggregateName("Receipt")
    val shelfAggregate = AggregateName("Shelf")
    val categoryAggregate = AggregateName("Category")

    def setupRelationsConfig(): DomainRelationConfigs =
        val books2authors = ManyToOneConfig(
            from = mediaDomain -> bookAggregate,
            to = authorsDomain -> authorAggregate,
            toAggregatePath = JsonPath.compile("$.authorId")
        )

        val books2receipts = OneToManyConfig(
            from = mediaDomain -> bookAggregate,
            to = storageDomain -> receiptAggregate,
            fromAggregatePath = JsonPath.compile("$.bookId")
        )

        val receipts2shelves = ManyToOneConfig(
            from = storageDomain -> receiptAggregate,
            to = storageDomain -> shelfAggregate,
            toAggregatePath = JsonPath.compile("$.shelfId")
        )

        val books2categories = ManyToOneConfig(
            from = mediaDomain -> bookAggregate,
            to = categoriesDomain -> categoryAggregate,
            toAggregatePath = JsonPath.compile("$.categoryId")
        )

        val categories2shelves = ManyToOneConfig(
            from = categoriesDomain -> categoryAggregate,
            to = storageDomain -> shelfAggregate,
            toAggregatePath = JsonPath.compile("$.shelfId")
        )

        DomainRelationConfigs(
            Seq(books2authors, books2receipts, receipts2shelves, books2categories, categories2shelves)
        )

    def setupCategoryIdFantasy: AggregateId = AggregateId("327")

    def setupCategoryFantasy: Aggregate =
        Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupCategoryIdFantasy",
                "name": "Fantasy",
                "shelfId": "FI"
            }
        """))

    def setupCategoryIdMystery: AggregateId = AggregateId("425")

    def setupCategoryMystery: Aggregate =
        Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupCategoryIdMystery",
                "name": "Mystery", 
                "shelfId": "FI"
            }
        """))

    def setupCategoryIdScientific: AggregateId = AggregateId("100")

    def setupCategoryScientific: Aggregate =
        Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupCategoryIdScientific",
                "name": "Scientific",
                "shelfId": "NF"
            }
        """))

    def setupAuthorIdTolkien: AggregateId = AggregateId("f3d2b210-7391-40c1-92bd-370caddd59b6")

    def setupAuthorTolkien: Aggregate =
        Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupAuthorIdTolkien",
                "name": "J. R. R. Tolkien",
                "dateOfBirth": "1892-01-03"
            }
        """))

    def setupAuthorIdChristie: AggregateId = AggregateId("5d67fcb5-aab0-4e11-81a1-69c341d63ace")

    def setupAuthorCristie: Aggregate =
        Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupAuthorIdChristie",
                "name": "Agatha Christie",
                "dateOfBirth": "1890-09-15"
            }
        """))

    def setupAuthorIdKnuth: AggregateId = AggregateId("dc20395a-dca9-407b-8478-70287c51253b")

    def setupAuthorKnuth: Aggregate =
        Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupAuthorIdKnuth",
                "name": "Donald E. Knuth",
                "dateOfBirth": "1938-01-10"
            }
        """))

    def setupAuthorIdFeynman: AggregateId = AggregateId("ded8fb5c-d5cb-423f-861d-c7cf20d1fef6")

    def setupAuthorFeynman: Aggregate =
        Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupAuthorIdKnuth",
                "name": "Richard P. Feynman",
                "dateOfBirth": "1918-05-11"
            }
        """))

    def setupShelfIdNonFiction: AggregateId = AggregateId("NF")

    def setupShelfNonFiction: Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupShelfIdNonFiction",
            "name": "Nonfictional Literature"
        }
        """))

    def setupShelfIdFiction: AggregateId = AggregateId("FI")

    def setupShelfFiction: Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupShelfIdFiction",
            "name": "Fictional Literature"
        }
        """))

    def setupBookIdSilmarillion: AggregateId = AggregateId("f998258d-5081-4b20-b41d-865134b80eb2")

    def setupBookSilmarillion: Aggregate =
        Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupBookIdSilmarillion",
                "categoryId": "$setupCategoryIdFantasy",
                "authorId": "$setupAuthorIdTolkien",
                "title": "The Silmarillion",
                "isbn13": "9780008537890",
                "price": { "amount": 56, "currency": "EUR"}
            }
        """))

    def setupBookIdHobbit: AggregateId = AggregateId("31a94690-f53d-421f-8428-a14a06a8081b")

    def setupBookHobbit: Aggregate =
        Aggregate(mapper.readTree(s"""
            {   
                "id": "$setupBookIdHobbit",
                "categoryId": "$setupCategoryIdFantasy",
                "authorId": "$setupAuthorIdTolkien",
                "title": "The Hobbit",
                "isbn13": "9780261103283",
                "price": { "amount": 27.50, "currency": "EUR"}
            }
        """))

    def setupBookIdRings: AggregateId = AggregateId("610e3d5d-d41c-48b9-8f67-96ad3e383519")

    def setupBookRings(categoryId: AggregateId = setupCategoryIdFantasy): Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupBookIdRings",
            "categoryId": "$categoryId",
            "authorId": "$setupAuthorIdTolkien",
            "title": "The Lord of the Rings",
            "isbn13": "9783608960358",
            "price": { "amount": 90, "currency": "EUR"}
        }
        """))

    def setupBookIdOrientExpress: AggregateId = AggregateId("9bd60be7-8dbe-4d56-8fb1-55ff49548b0f")

    def setupBookOrientExpress: Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupBookIdOrientExpress",
            "categoryId": "$setupCategoryIdMystery",
            "authorId": "$setupAuthorIdChristie",
            "title": "Murder on the Orient Express",
            "isbn13": "9780008226664",
            "price": { "amount": 19, "currency": "EUR"}
        }
        """))

    def setupBookIdNile: AggregateId = AggregateId("c33f2c1b-d604-4f6f-8fe0-6d827acc28d2")

    def setupBookNile: Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupBookIdNile",
            "categoryId": "$setupCategoryIdMystery",
            "authorId": "$setupAuthorIdChristie",
            "title": "Death on the Nile",
            "isbn13": "9780063375864",
            "price": { "amount": 21.5, "currency": "EUR"}
        }
        """))

    def setupBookIdComputer: AggregateId = AggregateId("e92c5192-9093-43de-b92f-d68725b0eb9d")

    def setupBookComputer: Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupBookIdComputer",
            "categoryId": "$setupCategoryIdScientific",
            "authorId": "$setupAuthorIdKnuth",
            "title": "The Art of Computer Programming",
            "isbn13": "978-0137935109",
            "price": { "amount": 276.99, "currency": "USD"}
        }
        """))

    def setupBookIdEasy: AggregateId = AggregateId("31cb1ec2-34d5-4cda-9866-11f8f3921b67")

    def setupBookEasy: Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupBookIdEasy",
            "categoryId": "$setupCategoryIdScientific",
            "authorId": "$setupAuthorIdFeynman",
            "title": "Six Easy Pieces",
            "isbn13": "9780465025275",
            "price": { "amount": 16.50, "currency": "EUR"}
        }
        """))

    def setupReceiptIdSilmarillion1: AggregateId = AggregateId("cdc22a3c-2e16-4751-8c21-2534119cd692")

    def setupReceiptSilmarillion1: Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupReceiptIdSilmarillion1",
            "bookId": "$setupBookIdSilmarillion",
            "shelfId": "$setupShelfIdFiction",
            "qty": 10
        }
        """))

    def setupReceiptIdSilmarillion2: AggregateId = AggregateId("25a0198e-9d0b-4139-8283-6b01490843b4")

    def setupReceiptSilmarillion2: Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupReceiptIdSilmarillion2",
            "bookId": "$setupBookIdSilmarillion",
            "shelfId": "$setupShelfIdFiction",
            "qty": 5
        }
        """))

    def setupReceiptIdEasy: AggregateId = AggregateId("6501d9f4-12a9-40da-89a2-a75da44b275c")

    def setupReceiptEasy: Aggregate =
        Aggregate(mapper.readTree(s"""
        {   
            "id": "$setupReceiptIdEasy",
            "bookId": "$setupBookIdEasy",
            "shelfId": "$setupShelfIdNonFiction",
            "qty": 20
        }
        """))
