package de.otto.anthology.config

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.config.jsonPathConfigReader
import pureconfig.ConfigReader
import pureconfig.generic.*

sealed trait DomainRelationConfig derives ConfigReader:
    def from: (DomainName, AggregateName)
    def to: (DomainName, AggregateName)

object DomainRelationConfig:
    given FieldCoproductHint[DomainRelationConfig] = FieldCoproductHint[DomainRelationConfig]("type")

case class OneToManyConfig(
    from: (DomainName, AggregateName),
    to: (DomainName, AggregateName),
    fromAggregatePath: JsonPath
) extends DomainRelationConfig

case class ManyToOneConfig(
    from: (DomainName, AggregateName),
    to: (DomainName, AggregateName),
    toAggregatePath: JsonPath
) extends DomainRelationConfig
