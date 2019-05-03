package io.github.a10y;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

final class NetworkTableSource implements StreamTableSource<Row> {
    private static final TypeInformation<?>[] TYPES = new TypeInformation[]{Types.STRING};
    private static final String[] FIELDS = new String[] {"line"};
    private static final RowTypeInfo ROW_TYPE = new RowTypeInfo(TYPES, FIELDS);

    private int port;

    NetworkTableSource(int port) {
        this.port = port;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        return env.addSource(
                new ReconnectingSocketSource(port),
                new RowTypeInfo(ReconnectingSocketSource.TYPES, ReconnectingSocketSource.FIELDS));
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return ROW_TYPE;
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema(FIELDS, ROW_TYPE.getFieldTypes());
    }
}
