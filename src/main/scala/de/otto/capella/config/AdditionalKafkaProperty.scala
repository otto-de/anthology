package de.otto.capella.config

import pureconfig.ConfigReader

case class AdditionalKafkaProperty(name: String, value: String) derives ConfigReader

extension (props: Seq[AdditionalKafkaProperty])
    def asMap: Map[String, String] =
        props.map(p => p.name -> p.value).toMap
