package de.otto.anthology.statestore

import pureconfig.*
import pureconfig.generic.*
import pureconfig.generic.semiauto.deriveReader

// 'derives ConfigReader' doesn't work yet for default values
// see https://github.com/pureconfig/pureconfig/issues/1488
// see https://github.com/pureconfig/pureconfig/issues/1673

case class RocksDBConfig(
    dbPath: String = sys.env.getOrElse("ANTHOLOGY_STATE_STORE_PATH", throw errorNoDbConfig),
    cacheSizeMb: Long = 256,
    writeBufferSizeMb: Long = 64
):
    assert(writeBufferSizeMb < cacheSizeMb)

object RocksDBConfig:
    given ConfigReader[RocksDBConfig] = deriveReader[RocksDBConfig]

private[statestore] def errorNoDbConfig: IllegalArgumentException =
    new IllegalArgumentException(
        "No DB config found: neither 'rocks-db.db-config' nor ANTHOLOGY_STATE_STORE_PATH environment variable is set"
    )
