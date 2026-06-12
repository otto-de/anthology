package de.otto.anthology.config

import de.otto.anthology.config.CredentialsLoader
import de.otto.anthology.kafka.ClusterName
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CredentialsLoaderTest extends AnyFlatSpec, Matchers:

    "CredentialsLoader.fromJson" should "parse credentials for multiple clusters" in:
        val json =
            """{"cluster-a":{"username":"user1","password":"pass1"},"cluster-b":{"username":"user2","password":"pass2"}}"""
        val result = CredentialsLoader.fromJson(json)
        result(ClusterName("cluster-a")) shouldEqual Map("username" -> "user1", "password" -> "pass1")
        result(ClusterName("cluster-b")) shouldEqual Map("username" -> "user2", "password" -> "pass2")

    it should "return an empty map for empty JSON object" in:
        CredentialsLoader.fromJson("{}") shouldEqual Map.empty

    it should "throw on invalid JSON" in:
        an[Exception] should be thrownBy CredentialsLoader.fromJson("not-json")

    it should "throw on missing ANTHOLOGY_CREDENTIALS env var" in:
        // Only run this assertion when the env var is genuinely absent
        assume(!sys.env.contains("ANTHOLOGY_CREDENTIALS"))
        an[IllegalStateException] should be thrownBy CredentialsLoader()
