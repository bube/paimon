/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code sink.operator-uid.suffix} exists so that operator ids stay stable when the topology around
 * a Paimon pipeline changes. It does not reach every operator Paimon adds, and each uncovered one
 * takes its id from the shape of the stream graph, so a change elsewhere in the job orphans its
 * entry in an existing checkpoint.
 *
 * <p>An append table shows the mechanics: {@link FlinkSinkBuilder} leaves its row conversion
 * operators without a uid, and {@link AppendTableSink} leaves the compaction coordinator and worker
 * without one. Other shapes add more, among them {@code local merge}, the changelog compaction
 * operators in {@link FlinkSink}, and the index bootstrap and assigner in {@code
 * GlobalDynamicBucketSink}.
 */
class OperatorUidSuffixTest {

    private static final String UID_SUFFIX = "test-uid";
    private static final String TABLE_NAME = "tbl";

    private static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("k", new IntType()),
                            new RowType.RowField("v", new VarCharType(10))));

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.INT()),
                    DataTypes.FIELD("v", DataTypes.STRING()));

    @TempDir private Path warehouse;

    private int tableSeq;

    @Test
    void testSinkOperatorsCarryAUid() throws Exception {
        assertThat(operatorsWithoutUid(buildSinkJob(appendTable(), 0))).isEmpty();
    }

    /** The consequence: an operator without a uid loses its id when anything upstream changes. */
    @Test
    void testOperatorIdsSurviveAnUpstreamTopologyChange() throws Exception {
        FileStoreTable table = appendTable();

        Set<String> orphaned = new LinkedHashSet<>(operatorIds(buildSinkJob(table, 0)));
        orphaned.removeAll(operatorIds(buildSinkJob(table, 1)));

        assertThat(orphaned)
                .as("operator ids an existing checkpoint would no longer find")
                .isEmpty();
    }

    /** The suffix does reach the writer, the committer and the dynamic bucket assigner. */
    @Test
    void testSuffixReachesWriterCommitterAndAssigner() throws Exception {
        Map<String, String> uids = uidsByOperatorName(buildSinkJob(primaryKeyTable(), 0));

        assertThat(uids)
                .containsEntry("Writer : " + TABLE_NAME, uid("Writer"))
                .containsEntry("Global Committer : " + TABLE_NAME, uid("Global Committer"))
                .containsEntry("dynamic-bucket-assigner", uid("dynamic-bucket-assigner"));
    }

    private static String uid(String prefix) {
        return prefix + "_" + TABLE_NAME + "_" + UID_SUFFIX;
    }

    /**
     * Uid-less operator names, numbered to tell apart the several called {@code Map}. Every
     * operator these tests build has a uid, so each name here comes from Paimon.
     */
    private static List<String> operatorsWithoutUid(StreamExecutionEnvironment env) {
        return env.getStreamGraph(false).getStreamNodes().stream()
                .filter(node -> node.getTransformationUID() == null)
                .map(node -> node.getOperatorName() + "#" + node.getId())
                .collect(Collectors.toList());
    }

    private static Map<String, String> uidsByOperatorName(StreamExecutionEnvironment env) {
        Map<String, String> uids = new LinkedHashMap<>();
        for (StreamNode node : env.getStreamGraph(false).getStreamNodes()) {
            uids.put(node.getOperatorName(), node.getTransformationUID());
        }
        return uids;
    }

    private static List<String> operatorIds(StreamExecutionEnvironment env) {
        List<String> ids = new ArrayList<>();
        for (JobVertex vertex : env.getStreamGraph(false).getJobGraph().getVertices()) {
            vertex.getOperatorIDs().stream()
                    .map(OperatorIDPair::getGeneratedOperatorID)
                    .map(Object::toString)
                    .forEach(ids::add);
        }
        return ids;
    }

    private StreamExecutionEnvironment buildSinkJob(FileStoreTable table, int extraOperators) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> source =
                env.fromSequence(0, 9)
                        .uid("sequence")
                        .map(x -> Row.of(x.intValue(), "v"))
                        .returns(Types.ROW(Types.INT, Types.STRING))
                        .uid("to-row");

        for (int i = 0; i < extraOperators; i++) {
            source =
                    source.map(row -> row)
                            .returns(Types.ROW(Types.INT, Types.STRING))
                            .uid("extra-" + i);
        }

        new FlinkSinkBuilder(table).forRow(source, INPUT_TYPE).build().uid("sink");
        return env;
    }

    /**
     * No primary key and bucket = -1 is BUCKET_UNAWARE, the mode {@link AppendTableSink} serves.
     */
    private FileStoreTable appendTable() throws Exception {
        return table(Collections.emptyList());
    }

    private FileStoreTable primaryKeyTable() throws Exception {
        return table(Collections.singletonList("k"));
    }

    private FileStoreTable table(List<String> primaryKeys) throws Exception {
        Options options = new Options();
        // one parent per table, so the name Paimon derives from the path is the same for both
        options.set(PATH, warehouse.resolve("db" + tableSeq++).resolve(TABLE_NAME).toString());
        options.set(SINK_OPERATOR_UID_SUFFIX, UID_SUFFIX);

        Schema schema =
                new Schema(
                        toDataType(TABLE_TYPE).getFields(),
                        Collections.emptyList(),
                        primaryKeys,
                        options.toMap(),
                        "");
        new SchemaManager(LocalFileIO.create(), new CoreOptions(options.toMap()).path())
                .createTable(schema);
        return FileStoreTableFactory.create(LocalFileIO.create(), options);
    }
}
