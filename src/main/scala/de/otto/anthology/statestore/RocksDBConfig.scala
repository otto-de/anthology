package de.otto.anthology.statestore

import pureconfig.*
import pureconfig.generic.*
import pureconfig.generic.semiauto.deriveReader

// 'derives ConfigReader' doesn't work yet for default values
// see https://github.com/pureconfig/pureconfig/issues/1488
// see https://github.com/pureconfig/pureconfig/issues/1673

case class RocksDBConfig(
    cacheSizeMb: Long = 256,
    writeBufferSizeMb: Long = 64,
    bestEffortsRecovery: Boolean = true
):
    assert(writeBufferSizeMb < cacheSizeMb)

object RocksDBConfig:
    given ConfigReader[RocksDBConfig] = deriveReader[RocksDBConfig]
