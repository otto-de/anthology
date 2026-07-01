package de.otto.anthology.config

import de.otto.anthology.config.AdditionalKafkaPropertiesLoader
import de.otto.anthology.kafka.ClusterName
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AdditionalKafkaPropertiesLoaderTest extends AnyFlatSpec, Matchers:

    "AdditionalKafkaPropertiesLoader.fromJson" should "parse properties for multiple clusters" in:
        val json =
            """{"cluster-a":{"username":"user1","password":"pass1"},"cluster-b":{"username":"user2","password":"pass2"}}"""
        val result = AdditionalKafkaPropertiesLoader.fromJson(json)
        result(ClusterName("cluster-a")) shouldEqual Map("username" -> "user1", "password" -> "pass1")
        result(ClusterName("cluster-b")) shouldEqual Map("username" -> "user2", "password" -> "pass2")

    it should "return an empty map for empty JSON object" in:
        AdditionalKafkaPropertiesLoader.fromJson("{}") shouldEqual Map.empty

    it should "throw on invalid JSON" in:
        an[Exception] should be thrownBy AdditionalKafkaPropertiesLoader.fromJson("not-json")
