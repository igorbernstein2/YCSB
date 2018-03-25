#!/usr/bin/env bash


P=igorbernstein-dev
I=benchmarks

COUNT=10000000


function run {
  ACTION=$1
  TABLE=${2:-usertable}

  case $ACTION in
    create)
      createTable $TABLE
      loadTable $TABLE
      ;;
    cleanup)
      cbt -project $P -instance $I deletetable $TABLE
      ;;
    perf)
      shortPerfRun $TABLE
      ;;
    client-load)
      clientLoadRun $TABLE
      ;;
    *)
      echo "Unknown test: $1" 1>&2
      exit 1
  esac
}

function createTable {
  cbt -project $P -instance $I createtable $1
  cbt -project $P -instance $I createfamily $1 cf
  cbt -project $P -instance $I setgcpolicy $1 cf maxversions=1
}

function deleteTable {
  cbt -project $P -instance $I deletetable $1
}

function loadTable {
  bin/ycsb load googlebigtable2 \
    -jvm-args ' -Xmx4096m' \
    -threads 1 \
    -P workloads/workloada \
    -p recordcount=$COUNT \
    -p operationcount=$COUNT \
    -p insertcount=$COUNT \
    -p insertstart=0 \
    -p measurementtype=hdrhistogram \
    -p measurement.interval=intended \
    -p hdrhistogram.fileoutput=true \
    -p hdrhistogram.output.path=logs/load2.hdr \
    -p bigtable.instance=projects/$P/instances/$I \
    -p table=$1 \
    -p bigtable.batching=true \
    -s | tee logs/load2.log
}

function shortPerfRun {
  ./bin/ycsb run googlebigtable2 \
    -jvm-args ' -Xmx4096m' \
    -threads 8 \
    -P workloads/workloada \
    -p recordcount=$COUNT \
    -p operationcount=$COUNT \
    -p insertcount=$COUNT \
    -p maxexecutiontime=1200 \
    -p measurementtype=hdrhistogram \
    -p measurement.interval=intended \
    -p hdrhistogram.fileoutput=true \
    -p hdrhistogram.output.path=logs/short-perf-run2.hdr \
    -p bigtable.instance=projects/$P/instances/$I \
    -p table=$1 \
    -p bigtable.batching=false \
    -s | tee logs/short-perf-run2.log
}

function clientLoadRun {
  ./bin/ycsb run googlebigtable2 \
    -jvm-args ' -Xmx4096m' \
    -threads 80 \
    -P workloads/workloada \
    -p recordcount=$COUNT \
    -p operationcount=$COUNT \
    -p maxexecutiontime=1200 \
    -p measurementtype=hdrhistogram \
    -p measurement.interval=intended \
    -p hdrhistogram.fileoutput=true \
    -p hdrhistogram.output.path=logs/client-load-run2.hdr \
    -p bigtable.instance=projects/$P/instances/$I \
    -p table=$1 \
    -p bigtable.batching=false \
    -s | tee logs/clientload-run2.log
}

run "$@"
