/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#pragma once

#include "common.h"

inline void set_w_opts_sync(WriteOptions &opts, bool val)
{
    opts.sync = val;
}

inline void set_w_opts_disable_wal(WriteOptions &opts, bool val)
{
    opts.disableWAL = val;
}

inline void set_w_opts_no_slowdown(WriteOptions &opts, bool val)
{
    opts.no_slowdown = val;
}
