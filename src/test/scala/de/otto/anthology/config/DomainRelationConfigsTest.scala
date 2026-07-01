package de.otto.anthology.config

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.TestData.*
import de.otto.anthology.config.ManyToOne
import de.otto.anthology.config.OneToMany
import de.otto.anthology.config.RelationConfigs
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DomainRelationConfigsTest extends AnyFlatSpec, Matchers, Diagrams:

    "DomainRelationConfigs" should "init with one relation" in:

        // given
        val books2authors = ManyToOne(
            relFrom = (mediaChannel, bookMessageFormat),
            relTo = (authorsChannel, authorMessageFormat),
            refFromManyToOnePath = JsonPath.compile("$.authorId")
        )

        // when
        val config = RelationConfigs(Seq(books2authors))

        // then
        assert(config.root == (mediaChannel, bookMessageFormat))
        assert(
            config.manyToOneRelationsStartingFrom.getOrElse((mediaChannel, bookMessageFormat), Set.empty) == Set(
                books2authors
            )
        )
        assert(
            config.manyToOneRelationsStartingFrom
                .getOrElse((authorsChannel, authorMessageFormat), Set.empty) == Set.empty
        )
        assert(config.oneToManyRelationsLeadingTo.getOrElse((mediaChannel, bookMessageFormat), Set.empty) == Set.empty)
        assert(
            config.oneToManyRelationsLeadingTo.getOrElse((authorsChannel, authorMessageFormat), Set.empty) == Set.empty
        )

    it should "init with a complex relation graph" in:

        // given
        val books2authors = ManyToOne(
            relFrom = (mediaChannel, bookMessageFormat),
            relTo = (authorsChannel, authorMessageFormat),
            refFromManyToOnePath = JsonPath.compile("$.authorId")
        )

        val books2stocks = OneToMany(
            relFrom = (mediaChannel, bookMessageFormat),
            relTo = (storageChannel, receiptMessageFormat),
            refFromManyToOnePath = JsonPath.compile("$.bookId")
        )

        val stocks2shelves = ManyToOne(
            relFrom = (storageChannel, receiptMessageFormat),
            relTo = (storageChannel, shelfMessageFormat),
            refFromManyToOnePath = JsonPath.compile("$.shelfId")
        )

        val books2categories = ManyToOne(
            relFrom = (mediaChannel, bookMessageFormat),
            relTo = (categoriesChannel, categoryMessageFormat),
            refFromManyToOnePath = JsonPath.compile("$.categoryId")
        )

        val categories2shelves = ManyToOne(
            relFrom = (categoriesChannel, categoryMessageFormat),
            relTo = (storageChannel, shelfMessageFormat),
            refFromManyToOnePath = JsonPath.compile("$.shelfId")
        )

        // when
        val config = RelationConfigs(
            Seq(books2authors, books2stocks, stocks2shelves, books2categories, categories2shelves)
        )

        // then
        assert(config.root == (mediaChannel, bookMessageFormat))

        assert(config.manyToOneRelationsStartingFrom.getOrElse((mediaChannel, bookMessageFormat), Set.empty).size == 2)
        assert(
            config.manyToOneRelationsStartingFrom
                .getOrElse((mediaChannel, bookMessageFormat), Set.empty)
                .contains(books2authors)
        )
        assert(
            config.manyToOneRelationsStartingFrom
                .getOrElse((mediaChannel, bookMessageFormat), Set.empty)
                .contains(books2categories)
        )

        assert(
            config.manyToOneRelationsStartingFrom
                .getOrElse((categoriesChannel, categoryMessageFormat), Set.empty) == Set(
                categories2shelves
            )
        )

        assert(
            config.manyToOneRelationsStartingFrom.getOrElse((storageChannel, receiptMessageFormat), Set.empty) == Set(
                stocks2shelves
            )
        )

        assert(
            config.manyToOneRelationsStartingFrom
                .getOrElse((authorsChannel, authorMessageFormat), Set.empty) == Set.empty
        )

        assert(
            config.manyToOneRelationsStartingFrom
                .getOrElse((storageChannel, shelfMessageFormat), Set.empty) == Set.empty
        )

        assert(config.oneToManyRelationsLeadingTo.getOrElse((mediaChannel, bookMessageFormat), Set.empty) == Set.empty)
        assert(
            config.oneToManyRelationsLeadingTo
                .getOrElse((categoriesChannel, categoryMessageFormat), Set.empty) == Set.empty
        )

        assert(
            config.oneToManyRelationsLeadingTo.getOrElse((storageChannel, receiptMessageFormat), Set.empty).size == 1
        )
        assert(
            config.oneToManyRelationsLeadingTo
                .getOrElse((storageChannel, receiptMessageFormat), Set.empty)
                .contains(books2stocks)
        )

        assert(
            config.oneToManyRelationsLeadingTo.getOrElse((authorsChannel, authorMessageFormat), Set.empty) == Set.empty
        )

        assert(
            config.oneToManyRelationsLeadingTo.getOrElse((storageChannel, shelfMessageFormat), Set.empty) == Set.empty
        )
