package de.otto.anthology.config

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.AggregateName
import de.otto.anthology.filtering.DomainFilteringConfig
import de.otto.anthology.transformation.DomainAggregateIdTransformationConfig
import de.otto.anthology.transformation.DomainAggregateTransformationConfig
import pureconfig.ConfigReader

// If the recognitionPath matches the message (== returns something), the message is recognised as being of this Aggregate

case class AggregateConfig(
    name: AggregateName,
    recognitionPath: Option[JsonPath],
    filtering: Option[DomainFilteringConfig],
    idTransformation: Option[DomainAggregateIdTransformationConfig],
    transformation: Option[DomainAggregateTransformationConfig]
) derives ConfigReader
