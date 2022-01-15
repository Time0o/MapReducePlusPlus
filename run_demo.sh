#!/bin/bash

(
  cd demo/out

  ../../build/master ../in/pg-*.txt &

  sleep 1

  ../../build/worker &
  ../../build/worker &
  ../../build/worker &

  wait

  LC_COLLATE=C sort reduce_out_*.mr | grep . > reduce_out.mr
)
