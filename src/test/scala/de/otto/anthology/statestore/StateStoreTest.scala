package de.otto.anthology.statestore

import de.otto.anthology.TestUtils.InMemoryStateStore
import de.otto.anthology.statestore.StateStore
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StateStoreTest extends AnyFlatSpec, Matchers, Diagrams:

    "StateStore" should "write and read set of strings" in:

        // given
        val store: StateStore = InMemoryStateStore()
        val expectedSet: Set[String] = Set("abc-def", "ghi-123", "2398742983öß?&%$_-:/={}")

        // when
        store.putStringSet("1", expectedSet)
        val actualSet = store.getStringSet("1")

        // then
        assert(expectedSet.toList.sorted == actualSet.toList.sorted)

    it should "write and read in empty set of strings" in:

        // given
        val store: StateStore = InMemoryStateStore()
        val expectedSet: Set[String] = Set.empty

        // when
        store.putStringSet("1", expectedSet)
        val actualSet = store.getStringSet("1")

        // then
        assert(expectedSet == actualSet)

    it should "add a string to set" in:

        // given
        val store: StateStore = InMemoryStateStore()
        val set1: Set[String] = Set("abc-def", "ghi-123")
        val set2: Set[String] = Set("abc-def", "ghi-123", "xyz")

        // when
        store.putStringSet("1", set1)
        store.addStringToSet("1", "xyz")
        val actualSet = store.getStringSet("1")

        // then
        assert(set2.toList.sorted == actualSet.toList.sorted)

    it should "add multiple strings to set" in:

        // given
        val store: StateStore = InMemoryStateStore()
        val set1: Set[String] = Set("abc-def", "ghi-123")
        val set2: Set[String] = Set("abc-def", "ghi-123", "xyzq", "xyzw", "xyze")

        // when
        store.putStringSet("1", set1)
        store.addStringsToSet("1", Set("xyzq", "xyzw", "xyze"))
        val actualSet = store.getStringSet("1")

        // then
        assert(set2.toList.sorted == actualSet.toList.sorted)

    it should "remove a string from set" in:
        // given
        val store: StateStore = InMemoryStateStore()
        val set1: Set[String] = Set("abc-def", "ghi-123", "xyz")
        val set2: Set[String] = Set("abc-def", "ghi-123")

        // when
        store.putStringSet("1", set1)
        store.removeStringFromSet("1", "xyz")
        val actualSet = store.getStringSet("1")

        // then
        assert(set2.toList.sorted == actualSet.toList.sorted)

    it should "remove multiple strings from set" in:

        // given
        val store: StateStore = InMemoryStateStore()
        val set1: Set[String] = Set("abc-def", "ghi-123", "xyzq", "xyzw", "xyze")
        val set2: Set[String] = Set("abc-def", "ghi-123")

        // when
        store.putStringSet("1", set1)
        store.removeStringsFromSet("1", Set("xyzq", "xyzw", "xyze"))
        val actualSet = store.getStringSet("1")

        // then
        assert(set2.toList.sorted == actualSet.toList.sorted)

    it should "remove key when set is empty" in:

        // given
        val store: StateStore = InMemoryStateStore()
        val set1: Set[String] = Set("abc-def", "ghi-123", "xyzq", "xyzw", "xyze")
        val set2: Set[String] = Set("abc-def", "ghi-123")
        val set3: Set[String] = Set("xyzq", "xyzw", "xyze")

        // when (1)
        store.putStringSet("1", set1)
        store.removeStringsFromSet("1", set2)

        // then (1)
        assert(set3.toList.sorted == store.getStringSet("1").toList.sorted)

        // when (2)
        store.removeStringsFromSet("1", set3)

        // then (2)
        assert(store.getStringSet("1").isEmpty)
