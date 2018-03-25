#!/usr/bin/env bash

set -ex

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
      echo "Unknown action: $1" 1>&2
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
  bin/ycsb load googlebigtable \
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
    -p hdrhistogram.output.path=logs/load.hdr \
    -p google.bigtable.project.id=$P \
    -p google.bigtable.instance.id=$I \
    -p table=$1 \
    -p columnfamily=cf \
    -p clientbuffering=true \
    -s | tee logs/load.log
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
    -p hdrhistogram.output.path=logs/short-perf-run.hdr \
    -p google.bigtable.project.id=$P \
    -p google.bigtable.instance.id=$I \
    -p table=$1 \
    -p columnfamily=cf \
    -p clientbuffering=false \
    -s | tee logs/short-perf-run.log
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
    -p hdrhistogram.output.path=logs/client-load-run.hdr \
    -p google.bigtable.project.id=$P \
    -p google.bigtable.instance.id=$I \
    -p table=$1 \
    -p columnfamily=cf \
    -p clientbuffering=false \
    -s | tee logs/clientload-run.log
}

run "$@"
