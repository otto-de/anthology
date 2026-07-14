package de.otto.anthology.statestore

import com.fasterxml.jackson.databind.JsonNode
import de.otto.anthology.JsonSupport.mapper

import java.nio.charset.StandardCharsets

/** Interface to Anthology's state store, which is backed by an embedded key-value-store. One Anthology instance works
  * with one state store instance.
  */
trait StateStore:

    def get(key: String): Option[Array[Byte]]

    def put(key: String, value: Array[Byte]): Unit

    def delete(key: String): Unit

    def getJson(key: String): Option[JsonNode] = get(key).map(mapper.readTree)

    def putJson(key: String, value: JsonNode): Unit = put(key, mapper.writeValueAsBytes(value))

    def writeBatch(operations: Seq[StateStore.BatchOperation]): Unit =
        operations.foreach:
            case StateStore.BatchOperation.Put(key, value) => put(key, value)
            case StateStore.BatchOperation.Delete(key) => delete(key)

    def getStringSet(key: String): Set[String] =
        get(key) match
            case Some(value) =>
                new String(value, StandardCharsets.UTF_8)
                    .split(StateStore.ELEMENT_SEPARATOR)
                    .toSet
            case None =>
                Set.empty

    def putStringSet(key: String, value: Set[String]): Unit =
        if value.nonEmpty
        then put(key, value.mkString(StateStore.ELEMENT_SEPARATOR).getBytes(StandardCharsets.UTF_8))
        else delete(key)

    def addStringToSet(key: String, elem: String): Unit =
        val oldSet: Set[String] = getStringSet(key)
        val newSet: Set[String] = oldSet + elem
        putStringSet(key, newSet)

    def addStringsToSet(key: String, elems: Set[String]): Unit =
        val oldSet: Set[String] = getStringSet(key)
        val newSet = oldSet ++ elems
        putStringSet(key, newSet)

    def removeStringFromSet(key: String, elem: String): Unit =
        val oldSet: Set[String] = getStringSet(key)
        val newSet = oldSet - elem
        putStringSet(key, newSet)

    def removeStringsFromSet(key: String, elems: Set[String]): Unit =
        val oldSet: Set[String] = getStringSet(key)
        val newSet = oldSet -- elems
        putStringSet(key, newSet)

object StateStore:

    val ELEMENT_SEPARATOR: String = ";"

    val SEGMENT_SEPARATOR: String = "/"

    enum BatchOperation:
        case Put(key: String, value: Array[Byte])
        case Delete(key: String)
