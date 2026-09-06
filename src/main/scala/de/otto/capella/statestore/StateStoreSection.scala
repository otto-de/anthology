package de.otto.capella.statestore

enum StateStoreSection:
    /** Section where the domain messages are persisted.
      */
    case DOM

    /** Section where the codomain messages are persisted.
      */
    case COD

    /** Section where the links are persisted.
      */
    case LNK

    /** Section where the back links are persisted.
      */
    case BLK

    /** Section where the flat-structured codomain messages are persisted (staging area).
      */
    case STA
