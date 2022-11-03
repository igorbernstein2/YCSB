/**
 * Copyright (c) 2022 YCSB contributors. All rights reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsRequest.Entry;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.RowRange;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;
import com.google.rpc.Code;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

@RunWith(JUnit4.class)
public class TestGoogleBigtableClient {

  private static final String FAMILY = "my-family";
  private static final String QUALIFIER = "my-column";
  private Server fakeServer;
  private Properties clientProps;
  private GoogleBigtableClient client;
  private FakeBigtableService fakeService;

  @Before
  public void setup() throws DBException {
    List<IOException> errors = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      try {
        fakeServer = createServer();
      } catch (IOException e) {
        errors.add(e);
      }
    }
    clientProps = new Properties();
    clientProps.setProperty(
        GoogleBigtableClient.EMULATOR_HOST_KEY, "localhost:" + fakeServer.getPort());
    clientProps.setProperty(GoogleBigtableClient.PROJECT_KEY, "fake-project");
    clientProps.setProperty(GoogleBigtableClient.INSTANCE_KEY, "fake-instance");
    clientProps.setProperty(GoogleBigtableClient.COLUMN_FAMILY_KEY, FAMILY);

    client = new GoogleBigtableClient();
    client.setProperties(clientProps);
    client.init();
  }

  @After
  public void teardown() throws DBException {
    client.cleanup();
  }

  private Server createServer() throws IOException {
    int port;
    try (ServerSocket ss = new ServerSocket(0)) {
      port = ss.getLocalPort();
    }
    fakeService = new FakeBigtableService();
    return ServerBuilder.forPort(port).addService(fakeService).build().start();
  }

  @Test
  public void testRead() {
    Map<String, ByteIterator> results = new HashMap<>();
    Status status = client.read("fake-table", "key1", ImmutableSet.of(QUALIFIER), results);
    Assert.assertEquals(Status.OK, status);
    Assert.assertEquals(1, results.size());
    ByteIterator value = results.get(QUALIFIER);
    Assert.assertEquals("value", value.toString());
  }

  @Test
  public void testScan() {
    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    Status status = client.scan("fake-table", "key", 10, ImmutableSet.of(QUALIFIER), results);
    Assert.assertEquals(Status.OK, status);
    Assert.assertEquals(10, results.size());

    for (HashMap<String, ByteIterator> result : results) {
      Assert.assertEquals(1, result.size());
      ByteIterator value = result.get(QUALIFIER);
      Assert.assertEquals("value", value.toString());
    }
  }

  @Test
  public void testInsert() throws InterruptedException, DBException {
    Status status =
        client.insert(
            "fake-table", "key1", ImmutableMap.of(QUALIFIER, new StringByteIterator("value")));

    Assert.assertEquals(Status.OK, status);
    MutateRowRequest req = fakeService.mutateRequests.poll(30, TimeUnit.SECONDS);

    Assert.assertEquals(
        "projects/fake-project/instances/fake-instance/tables/fake-table", req.getTableName());
    Assert.assertEquals(ByteString.copyFromUtf8("key1"), req.getRowKey());
    Assert.assertEquals(1, req.getMutationsCount());

    SetCell setCell = req.getMutations(0).getSetCell();
    Assert.assertEquals(ByteString.copyFromUtf8(QUALIFIER), setCell.getColumnQualifier());
    Assert.assertEquals(ByteString.copyFromUtf8("value"), setCell.getValue());
  }

  @Test
  public void testInsertBatch() throws InterruptedException, DBException {
    Properties properties = new Properties(clientProps);
    properties.setProperty(GoogleBigtableClient.CLIENT_SIDE_BUFFERING, "true");

    GoogleBigtableClient bufferedClient = new GoogleBigtableClient();
    bufferedClient.setProperties(properties);
    bufferedClient.init();

    Status status;
    try {
      status =
          bufferedClient.insert(
              "fake-table", "key1", ImmutableMap.of(QUALIFIER, new StringByteIterator("value")));
    } finally {
      bufferedClient.cleanup();
    }
    Assert.assertEquals(Status.BATCHED_OK, status);
    MutateRowsRequest req = fakeService.mutateRowsRequests.poll(30, TimeUnit.SECONDS);

    Assert.assertEquals(
        "projects/fake-project/instances/fake-instance/tables/fake-table", req.getTableName());
    Assert.assertEquals(1, req.getEntriesCount());
    Entry entry = req.getEntries(0);
    Assert.assertEquals(ByteString.copyFromUtf8("key1"), entry.getRowKey());
    Assert.assertEquals(1, entry.getMutationsCount());
    SetCell setCell = entry.getMutations(0).getSetCell();
    Assert.assertEquals(ByteString.copyFromUtf8(QUALIFIER), setCell.getColumnQualifier());
    Assert.assertEquals(ByteString.copyFromUtf8("value"), setCell.getValue());
  }

  private static class FakeBigtableService extends BigtableGrpc.BigtableImplBase {

    private final BlockingQueue<MutateRowRequest> mutateRequests = new LinkedBlockingDeque<>();
    private final BlockingQueue<MutateRowsRequest> mutateRowsRequests = new LinkedBlockingDeque<>();

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {

      SortedSet<ByteString> sortedKeys =
          new TreeSet<>(Comparator.comparing(ByteString::toStringUtf8));
      sortedKeys.addAll(request.getRows().getRowKeysList());
      for (ByteString key : sortedKeys) {
        responseObserver.onNext(
            ReadRowsResponse.newBuilder()
                .addChunks(
                    CellChunk.newBuilder()
                        .setRowKey(key)
                        .setFamilyName(StringValue.newBuilder().setValue("f"))
                        .setQualifier(
                            BytesValue.newBuilder().setValue(ByteString.copyFromUtf8(QUALIFIER)))
                        .setTimestampMicros(10_000)
                        .setValue(ByteString.copyFromUtf8("value"))
                        .setCommitRow(true))
                .build());
      }

      SortedSet<RowRange> ranges =
          new TreeSet<>(Comparator.comparing(o -> o.getStartKeyClosed().toStringUtf8()));
      ranges.addAll(request.getRows().getRowRangesList());

      for (RowRange range : ranges) {
        for (int i = 0; i < request.getRowsLimit(); i++) {
          ByteString key =
              range.getStartKeyClosed().concat(ByteString.copyFromUtf8(String.format("-%02d", i)));

          responseObserver.onNext(
              ReadRowsResponse.newBuilder()
                  .addChunks(
                      CellChunk.newBuilder()
                          .setRowKey(key)
                          .setFamilyName(StringValue.newBuilder().setValue("family"))
                          .setQualifier(
                              BytesValue.newBuilder().setValue(ByteString.copyFromUtf8(QUALIFIER)))
                          .setTimestampMicros(10_000)
                          .setValue(ByteString.copyFromUtf8("value"))
                          .setCommitRow(true))
                  .build());
        }
      }
      responseObserver.onCompleted();
    }

    @Override
    public void mutateRow(
        MutateRowRequest request, StreamObserver<MutateRowResponse> responseObserver) {
      mutateRequests.add(request);
      responseObserver.onNext(MutateRowResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void mutateRows(
        MutateRowsRequest request, StreamObserver<MutateRowsResponse> responseObserver) {
      mutateRowsRequests.add(request);

      MutateRowsResponse.Builder response = MutateRowsResponse.newBuilder();
      for (int i = 0; i < request.getEntriesCount(); i++) {
        response.addEntries(
            MutateRowsResponse.Entry.newBuilder()
                .setIndex(i++)
                .setStatus(com.google.rpc.Status.newBuilder().setCode(Code.OK.getNumber()))
                .build());
      }
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }
  }
}
