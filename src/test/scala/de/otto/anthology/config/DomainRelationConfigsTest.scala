package de.otto.anthology.config

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.TestData.*
import de.otto.anthology.config.DomainRelationConfigs
import de.otto.anthology.config.ManyToOneConfig
import de.otto.anthology.config.OneToManyConfig
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DomainRelationConfigsTest extends AnyFlatSpec, Matchers, Diagrams:

    "DomainRelationConfigs" should "init with one relation" in:

        // given
        val books2authors = ManyToOneConfig(
            from = (mediaDomain, bookAggregate),
            to = (authorsDomain, authorAggregate),
            toAggregatePath = JsonPath.compile("$.authorId")
        )

        // when
        val config = DomainRelationConfigs(Seq(books2authors))

        // then
        assert(config.root == (mediaDomain, bookAggregate))
        assert(
            config.manyToOneRelationsStartingFrom.getOrElse((mediaDomain, bookAggregate), Set.empty) == Set(
                books2authors
            )
        )
        assert(
            config.manyToOneRelationsStartingFrom.getOrElse((authorsDomain, authorAggregate), Set.empty) == Set.empty
        )
        assert(config.oneToManyRelationsLeadingTo.getOrElse((mediaDomain, bookAggregate), Set.empty) == Set.empty)
        assert(config.oneToManyRelationsLeadingTo.getOrElse((authorsDomain, authorAggregate), Set.empty) == Set.empty)

    it should "init with a complex relation graph" in:

        // given
        val books2authors = ManyToOneConfig(
            from = (mediaDomain, bookAggregate),
            to = (authorsDomain, authorAggregate),
            toAggregatePath = JsonPath.compile("$.authorId")
        )

        val books2stocks = OneToManyConfig(
            from = (mediaDomain, bookAggregate),
            to = (storageDomain, receiptAggregate),
            fromAggregatePath = JsonPath.compile("$.bookId")
        )

        val stocks2shelves = ManyToOneConfig(
            from = (storageDomain, receiptAggregate),
            to = (storageDomain, shelfAggregate),
            toAggregatePath = JsonPath.compile("$.shelfId")
        )

        val books2categories = ManyToOneConfig(
            from = (mediaDomain, bookAggregate),
            to = (categoriesDomain, categoryAggregate),
            toAggregatePath = JsonPath.compile("$.categoryId")
        )

        val categories2shelves = ManyToOneConfig(
            from = (categoriesDomain, categoryAggregate),
            to = (storageDomain, shelfAggregate),
            toAggregatePath = JsonPath.compile("$.shelfId")
        )

        // when
        val config = DomainRelationConfigs(
            Seq(books2authors, books2stocks, stocks2shelves, books2categories, categories2shelves)
        )

        // then
        assert(config.root == (mediaDomain, bookAggregate))

        assert(config.manyToOneRelationsStartingFrom.getOrElse((mediaDomain, bookAggregate), Set.empty).size == 2)
        assert(
            config.manyToOneRelationsStartingFrom
                .getOrElse((mediaDomain, bookAggregate), Set.empty)
                .contains(books2authors)
        )
        assert(
            config.manyToOneRelationsStartingFrom
                .getOrElse((mediaDomain, bookAggregate), Set.empty)
                .contains(books2categories)
        )

        assert(
            config.manyToOneRelationsStartingFrom.getOrElse((categoriesDomain, categoryAggregate), Set.empty) == Set(
                categories2shelves
            )
        )

        assert(
            config.manyToOneRelationsStartingFrom.getOrElse((storageDomain, receiptAggregate), Set.empty) == Set(
                stocks2shelves
            )
        )

        assert(
            config.manyToOneRelationsStartingFrom.getOrElse((authorsDomain, authorAggregate), Set.empty) == Set.empty
        )

        assert(config.manyToOneRelationsStartingFrom.getOrElse((storageDomain, shelfAggregate), Set.empty) == Set.empty)

        assert(config.oneToManyRelationsLeadingTo.getOrElse((mediaDomain, bookAggregate), Set.empty) == Set.empty)
        assert(
            config.oneToManyRelationsLeadingTo.getOrElse((categoriesDomain, categoryAggregate), Set.empty) == Set.empty
        )

        assert(config.oneToManyRelationsLeadingTo.getOrElse((storageDomain, receiptAggregate), Set.empty).size == 1)
        assert(
            config.oneToManyRelationsLeadingTo
                .getOrElse((storageDomain, receiptAggregate), Set.empty)
                .contains(books2stocks)
        )

        assert(config.oneToManyRelationsLeadingTo.getOrElse((authorsDomain, authorAggregate), Set.empty) == Set.empty)

        assert(config.oneToManyRelationsLeadingTo.getOrElse((storageDomain, shelfAggregate), Set.empty) == Set.empty)
