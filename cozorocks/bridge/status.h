/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#pragma once

#include "common.h"

void write_status(const Status &rstatus, RocksDbStatus &status);

RocksDbStatus convert_status(const Status &status);
