/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher;
import com.google.cloud.bigtable.data.v2.models.Filters.ChainFilter;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.InputStreamByteIterator;
import com.yahoo.ycsb.Status;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.threeten.bp.Duration;

/**
 * Google Bigtable native client for YCSB framework.
 *
 * <p>Bigtable offers three APIs. An HBase compatible API, an unsupported Protobuf GRPC API and the
 * newer native alpha API in google-cloud-java. This client implements the native API. To use the
 * HBase API, see the hbase10 client binding.
 */
public class GoogleBigtableClient2 extends com.yahoo.ycsb.DB {
  private static final Logger LOGGER = Logger.getLogger(GoogleBigtableClient2.class.getName());

  // Property names
  private static final String INSTANCE_PROP = "bigtable.instance";
  private static final String COLUMN_FAMILY_PROP = "bigtable.family";
  private static final String ENABLE_BATCHING_PROP = "bigtable.batching";

  // Shared client
  private static final Object LOCK = new Object();
  private static BigtableDataClient sharedClient;
  private static String columnFamily;
  private static int sharedClientRefCount = 0;
  private static boolean enableBatching;

  // Per thread state
  private BulkMutationBatcher batcher;

  @Override
  public void init() throws DBException {
    synchronized (LOCK) {
      if (sharedClientRefCount == 0) {
        enableBatching =
            Boolean.parseBoolean(getProperties().getProperty(ENABLE_BATCHING_PROP, "false"));
        columnFamily = getProperties().getProperty(COLUMN_FAMILY_PROP, "cf");

        InstanceName instanceName;
        try {
          String instanceNameStr = getProperties().getProperty(INSTANCE_PROP);
          instanceName = InstanceName.parse(instanceNameStr);
        } catch (Exception e) {
          throw new DBException(
              "The property bigtable.instance must be of the form projects/<PROJECT>/instances/<INSTANCE>",
              e);
        }

        try {
          sharedClient = BigtableDataClient.create(instanceName);
        } catch (IOException e) {
          throw new DBException("Failed to create the shared client", e);
        }
      }

      sharedClientRefCount++;
    }

    batcher = sharedClient.newBulkMutationBatcher();
  }

  @Override
  public void cleanup() throws DBException {
    boolean hasError = false;

    try {
      batcher.close(Duration.ofMinutes(10));
    } catch (Exception e) {
      hasError = true;

      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      LOGGER.log(Level.WARNING, "Failed to close the writer", e);
    }

    synchronized (LOCK) {
      sharedClientRefCount--;
      if (sharedClientRefCount == 0) {
        try {
          sharedClient.close();
        } catch (Exception e) {
          hasError = true;

          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }

          LOGGER.log(Level.WARNING, "Failed to close the client", e);
        }
      }
    }

    if (hasError) {
      throw new DBException("Failed to cleanup");
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

    Query query = createBaseQuery(table, fields).rowKey(key);

    try {
      Row row = sharedClient.readRowsCallable().first().call(query);

      if (row == null) {
        return Status.NOT_FOUND;
      } else {
        rowToHash(row, result);
        return Status.OK;
      }
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Failed to read " + table + ", " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {

    Query query =
        createBaseQuery(table, fields)
            .range(ByteStringRange.unbounded().startClosed(startkey))
            .limit(recordcount);

    try {
      ServerStream<Row> rows = sharedClient.readRowsCallable().call(query);

      for (Row row : rows) {
        HashMap<String, ByteIterator> rowHash = Maps.newHashMap();
        rowToHash(row, rowHash);
        result.add(rowHash);
      }

      return Status.OK;

    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Failed to read " + table + ", startKey: " + startkey, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    RowMutation rowMutation = RowMutation.create(table, key);

    for (Entry<String, ByteIterator> entry : values.entrySet()) {
      ByteString value = UnsafeByteOperations.unsafeWrap(entry.getValue().toArray());
      rowMutation.setCell(columnFamily, ByteString.copyFromUtf8(entry.getKey()), value);
    }

    return mutate(rowMutation);
  }

  @Override
  public Status delete(String table, String key) {
    return mutate(RowMutation.create(table, key).deleteRow());
  }

  // Helpers
  private Query createBaseQuery(String table, Set<String> fields) {
    ChainFilter filter =
        FILTERS
            .chain()
            .filter(FILTERS.limit().cellsPerColumn(1))
            .filter(FILTERS.family().exactMatch(columnFamily));

    if (fields != null && !fields.isEmpty()) {
      filter.filter(FILTERS.qualifier().regex(Joiner.on("|").join(fields)));
    }

    return Query.create(table).filter(filter);
  }

  private void rowToHash(Row row, Map<String, ByteIterator> hash) {
    for (RowCell cell : row.getCells()) {
      hash.put(
          cell.getQualifier().toStringUtf8(),
          new InputStreamByteIterator(cell.getValue().newInput(), cell.getValue().size()));
    }
  }

  private Status mutate(RowMutation mutation) {
    try {
      if (enableBatching) {
        batcher.add(mutation);
        return Status.BATCHED_OK;
      } else {
        sharedClient.mutateRowCallable().call(mutation);
        return Status.OK;
      }
    } catch (Throwable e) {
      LOGGER.log(Level.WARNING, "Failed to mutate", e);
      return Status.ERROR;
    }
  }
}
