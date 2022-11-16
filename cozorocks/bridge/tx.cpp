/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#include "tx.h"
#include "cozorocks/src/bridge/mod.rs.h"

void TxBridge::start()
{
    Transaction *txn = db->db->BeginTransaction(w_opts, tx_opts);
    tx.reset(txn);
    assert(tx);
}
