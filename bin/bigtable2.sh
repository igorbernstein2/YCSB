#!/usr/bin/env bash

set -ex

P=igorbernstein-dev
I=benchmarks

COUNT=10000000


function run {
  TABLE=${1:-usertable2}

  createTable $TABLE

  for i in `seq 1 3`; do
    runBenchmark "new-run${i}" $TABLE
  done

  deleteTable $TABLE
}

function createTable {
  TABLE=$1

  seq -f "user%02G" 0 100 | xargs cbt -project $P -instance $I createtable $TABLE
  cbt -project $P -instance $I createfamily $TABLE cf
  cbt -project $P -instance $I setgcpolicy $TABLE cf maxversions=1
}

function deleteTable {
  TABLE=$1

  cbt -project $P -instance $I deletetable $TABLE
}

function runBenchmark {
  PREFIX=$1
  TABLE=$2

  OUTPUT="log/$PREFIX"
  mkdir -p $OUTPUT

  loadTable $OUTPUT $TABLE
  shortPerfRun $OUTPUT $TABLE
  clientLoadRun $OUTPUT $TABLE
}

function loadTable {
  OUTPUT=$1
  TABLE=$2

  bin/ycsb load googlebigtable2 \
    -jvm-args ' -Xmx4096m' \
    -threads 1 \
    -P workloads/workloada \
    -p zeropadding=7 \
    -p recordcount=$COUNT \
    -p operationcount=$COUNT \
    -p insertcount=$COUNT \
    -p insertstart=0 \
    -p measurementtype=hdrhistogram \
    -p measurement.interval=intended \
    -p hdrhistogram.fileoutput=true \
    -p hdrhistogram.output.path=$OUTPUT/load.hdr \
    -p bigtable.instance=projects/$P/instances/$I \
    -p table=$TABLE \
    -p bigtable.batching=true \
    -s | tee $OUTPUT/load.log
}

function shortPerfRun {
  OUTPUT=$1
  TABLE=$2

  ./bin/ycsb run googlebigtable2 \
    -jvm-args ' -Xmx4096m' \
    -threads 8 \
    -P workloads/workloada \
    -p zeropadding=7 \
    -p recordcount=$COUNT \
    -p operationcount=$COUNT \
    -p insertcount=$COUNT \
    -p maxexecutiontime=1200 \
    -p measurementtype=hdrhistogram \
    -p measurement.interval=intended \
    -p hdrhistogram.fileoutput=true \
    -p hdrhistogram.output.path=$OUTPUT/short-perf.hdr \
    -p bigtable.instance=projects/$P/instances/$I \
    -p table=$TABLE \
    -p bigtable.batching=false \
    -s | tee $OUTPUT/short-perf.log
}

function clientLoadRun {
  OUTPUT=$1
  TABLE=$2

  ./bin/ycsb run googlebigtable2 \
    -jvm-args ' -Xmx4096m' \
    -threads 80 \
    -P workloads/workloada \
    -p zeropadding=7 \
    -p recordcount=$COUNT \
    -p operationcount=$COUNT \
    -p maxexecutiontime=1200 \
    -p measurementtype=hdrhistogram \
    -p measurement.interval=intended \
    -p hdrhistogram.fileoutput=true \
    -p hdrhistogram.output.path=$OUTPUT/client-load.hdr \
    -p bigtable.instance=projects/$P/instances/$I \
    -p table=$TABLE \
    -p bigtable.batching=false \
    -s | tee $OUTPUT/client-load.log
}

run "$@"
