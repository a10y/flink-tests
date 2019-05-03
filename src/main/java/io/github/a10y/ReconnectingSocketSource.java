package io.github.a10y;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import java.util.Scanner;

final class ReconnectingSocketSource implements SourceFunction<Row> {
    private int port;
    private volatile boolean stopped;

    static final String[] FIELDS = new String[]{"line"};
    static final TypeInformation<?>[] TYPES = new TypeInformation<?>[]{Types.STRING};

    ReconnectingSocketSource(int port) {
        this.port = port;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        ServerSocket server = createServer();

        while (!stopped) {
            Socket socket = server.accept();
            emitRecords(socket.getInputStream(), ctx);
        }
    }

    @Override
    public void cancel() {
        stopped = true;
    }

    private ServerSocket createServer() throws IOException {
        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress(port));
        return server;
    }

    private void emitRecords(InputStream inputStream, SourceContext<Row> ctx) {
        Scanner scanner = new Scanner(inputStream);

        while (!stopped && scanner.hasNext() && Objects.isNull(scanner.ioException())) {
            String line = scanner.next();
            ctx.collect(Row.of(line));
        }
    }
}
