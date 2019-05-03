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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Register UDF
        tEnv.registerFunction("split", new SplitUdf());
        tEnv.registerFunction("index", new IndexUdf());

        // Register table source from network.
        tEnv.registerTableSource("network", new NetworkTableSource(2387));

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

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, Time.seconds(1)));
        env.enableCheckpointing(100L);
        env.setStateBackend((StateBackend) new FsStateBackend("file:///tmp/testing"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.execute("Network exec.");
    }
}
