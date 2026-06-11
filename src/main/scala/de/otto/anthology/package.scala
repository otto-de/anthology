package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import pureconfig.ConfigReader

opaque type AggregateId = String
object AggregateId:
    def apply(aid: String): AggregateId = aid

opaque type Aggregate = JsonNode

object Aggregate:
    def apply(agg: JsonNode): Aggregate = agg
    extension (agg: Aggregate) def toJson: JsonNode = agg

/** Name of a source domain.
  */
opaque type DomainName = String
object DomainName:
    def apply(name: String): DomainName = name
    given domainNameConfigReader: ConfigReader[DomainName] =
        ConfigReader[String].map(nameStr => DomainName(nameStr))

/** Name of an agggregate. Unlike the [[anthology.AggregateId]], it does not identify an instance, but rather a class of
  * aggregates.
  */
opaque type AggregateName = String
object AggregateName:
    def apply(name: String): AggregateName = name
    given aggregateNameConfigReader: ConfigReader[AggregateName] =
        ConfigReader[String].map(nameStr => AggregateName(nameStr))

opaque type Parallelism = Int
object Parallelism:
    def apply(para: Int): Parallelism = para
    given parallelismConfigReader: ConfigReader[Parallelism] =
        ConfigReader[Int].map(paraInt => Parallelism(paraInt))
    extension (para: Parallelism) def toInt: Int = para
