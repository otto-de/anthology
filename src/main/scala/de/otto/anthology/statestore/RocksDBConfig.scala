package de.otto.anthology.statestore

import pureconfig.*
import pureconfig.generic.*
import pureconfig.generic.semiauto.deriveReader

// 'derives ConfigReader' doesn't work yet for default values
// see https://github.com/pureconfig/pureconfig/issues/1488
// see https://github.com/pureconfig/pureconfig/issues/1673

case class RocksDBConfig(
    cacheSizeMb: Long = 0L,
    writeBufferSizeMb: Long = 0L,
    bestEffortsRecovery: Boolean = true,
    deactivate: Boolean = false
):
    assert:
        (cacheSizeMb > 0L && writeBufferSizeMb > 0L && cacheSizeMb > writeBufferSizeMb) || (cacheSizeMb == 0L && writeBufferSizeMb == 0L)

object RocksDBConfig:
    given ConfigReader[RocksDBConfig] = deriveReader[RocksDBConfig]
