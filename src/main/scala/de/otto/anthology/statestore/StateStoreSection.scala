package de.otto.anthology.statestore

enum StateStoreSection:
    /** Section where the domain aggregates are persisted.
      */
    case DOM

    /** Section where the codomain aggregates are persisted.
      */
    case COD

    /** Section where the links are persisted.
      */
    case LNK

    /** Section where the back links are persisted.
      */
    case BLK

    /** Section where the flat-structured codomain aggregates are persisted (staging area).
      */
    case STA
