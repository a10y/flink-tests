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

package io.github.a10y;

import java.util.Map;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.StringUtils;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        // Set the conf.
        Configuration conf = new Configuration();
        conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/testing/4373c3ae3bb84befad5e969174b95247");
        conf.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Register UDF
        tEnv.registerFunction("split", new SplitUdf());
        tEnv.registerFunction("index", new IndexUdf());

        // Register table source from network.
        // tEnv.registerTableSource("network", new NetworkTableSource(2387));
        tEnv.registerTableSource("network", new NetworkTableSourceCloned(2387));

        // Do some cool logic.
        String query = "SELECT index(split(line, ','), 0) AS `left`, CAST(index(split(line, ','), 1) AS INT) AS `right`" +
                "FROM network";
        Table split = tEnv.sqlQuery(query);
        Table grouped = split.distinct().groupBy("left").select("left, right.max AS big_r");

        tEnv.registerTableSink(
                "split_lines",
                grouped.getSchema().getFieldNames(),
                grouped.getSchema().getFieldTypes(),
                new ConsoleTableSink());

        tEnv.sqlUpdate("INSERT INTO split_lines SELECT * FROM " + grouped);

        // Try changing this shit, see the operator state graph, etc.
        System.err.println(env.getExecutionPlan());

        StreamGraph graph = env.getStreamGraph();

        StreamGraphHasherV2 hasher = new StreamGraphHasherV2();

        hasher.traverseStreamGraphAndGenerateHashes(graph).entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(ent -> System.err.println(ent.getKey() + " => " + StringUtils.byteToHexString(ent.getValue())));

        // for (Tuple2<Integer, StreamOperator<?>> op : graph.getOperators()) {
        //     int nodeId = op.f0;
        //     StreamNode node = graph.getStreamNode(nodeId);
        //     System.err.println(nodeId + " => " + node.getTransformationUID());
        // }

        // Something needs to be assigning all of these UIDs...

        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, Time.seconds(1)));
        // env.enableCheckpointing(100L);
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // env.execute("Network exec.");
    }
}
