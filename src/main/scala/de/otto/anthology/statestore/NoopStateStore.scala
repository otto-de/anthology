package de.otto.anthology.statestore

/** StateStore implementation meant for troubleshooting purposes that does essentially do nothing.
  */
class NoopStateStore() extends StateStore:
    override def get(key: String): Option[Array[Byte]] = None
    override def put(key: String, value: Array[Byte]): Unit = ()
    override def delete(key: String): Unit = ()
