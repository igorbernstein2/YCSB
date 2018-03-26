#!/usr/bin/env bash

set -ex

P=igorbernstein-dev
I=benchmarks

COUNT=10000000


function run {
  TABLE=${1:-usertable}

  createTable $TABLE

  for i in `seq 1 3`; do
    runBenchmark "old-run${i}" $TABLE
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
  mkdir -p $LOGS

  loadTable $OUTPUT $TABLE
  shortPerfRun $OUTPUT $TABLE
  clientLoadRun $OUTPUT $TABLE
}

function loadTable {
  bin/ycsb load googlebigtable \
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
    -p hdrhistogram.output.path=logs/load.hdr \
    -p google.bigtable.project.id=$P \
    -p google.bigtable.instance.id=$I \
    -p table=$1 \
    -p columnfamily=cf \
    -p clientbuffering=true \
    -s | tee logs/load.log
}

function shortPerfRun {
  ./bin/ycsb run googlebigtable \
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
    -p hdrhistogram.output.path=logs/short-perf-run.hdr \
    -p google.bigtable.project.id=$P \
    -p google.bigtable.instance.id=$I \
    -p table=$1 \
    -p columnfamily=cf \
    -p clientbuffering=false \
    -s | tee logs/short-perf-run.log
}

function clientLoadRun {
  ./bin/ycsb run googlebigtable \
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
    -p hdrhistogram.output.path=logs/client-load-run.hdr \
    -p google.bigtable.project.id=$P \
    -p google.bigtable.instance.id=$I \
    -p table=$1 \
    -p columnfamily=cf \
    -p clientbuffering=false \
    -s | tee logs/clientload-run.log
}

run "$@"
