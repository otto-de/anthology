package de.otto.anthology.config

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.config.jsonPathConfigReader
import pureconfig.ConfigReader
import pureconfig.generic.*

sealed trait DomainRelationConfig derives ConfigReader:
    def relFrom: (DomainName, AggregateName)
    def relTo: (DomainName, AggregateName)
    def refFromManyToOnePath: JsonPath

object DomainRelationConfig:
    given FieldCoproductHint[DomainRelationConfig] = FieldCoproductHint[DomainRelationConfig]("type")

given domainName2AggregateNameConfigReader: ConfigReader[(DomainName, AggregateName)] =
    ConfigReader[String].map: d2aStr =>
        val d2a = d2aStr.split("/")
        (DomainName(d2a(0)), AggregateName(d2a(1)))

case class OneToMany(
    relFrom: (DomainName, AggregateName),
    relTo: (DomainName, AggregateName),
    refFromManyToOnePath: JsonPath
) extends DomainRelationConfig

case class ManyToOne(
    relFrom: (DomainName, AggregateName),
    relTo: (DomainName, AggregateName),
    refFromManyToOnePath: JsonPath
) extends DomainRelationConfig
