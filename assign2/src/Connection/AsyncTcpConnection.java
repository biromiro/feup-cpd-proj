package Connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class AsyncTcpConnection implements AutoCloseable {
    private static final int RECEIVE_BUFFER_SIZE = 2048;
    private final AsynchronousSocketChannel socket;

    public AsyncTcpConnection(AsynchronousSocketChannel socket) {
        this.socket = socket;
    }

    public interface ConnectionHandler {
        void completed(AsyncTcpConnection connection);
        void failed(Throwable exc);
    }

    public static void connect(ScheduledThreadPoolExecutor executor, String host, int port, ConnectionHandler handler) {
        AsynchronousSocketChannel socket;
        try {
             socket = AsynchronousSocketChannel.open(AsynchronousChannelGroup.withThreadPool(executor));
        } catch (IOException e) {
            handler.failed(e);
            return;
        }
        socket.connect(new InetSocketAddress(host, port), null, new CompletionHandler<Void, Void>() {
            @Override
            public void completed(Void result, Void attachment) {
                handler.completed(new AsyncTcpConnection(socket));
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                handler.failed(exc);
            }
        });
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    public interface ReadHandler {
        void completed(Integer result, String message);
        void failed(Throwable exc);
    }

    public interface WriteHandler {
        void completed(Integer result);
        void failed(Throwable exc);
    }

    public void read(ReadHandler handler) {
        ByteBuffer buffer = ByteBuffer.allocate(RECEIVE_BUFFER_SIZE);
        socket.read(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                String receivedMessage = new String(buffer.array(), buffer.arrayOffset(), result);
                handler.completed(result, receivedMessage);
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                handler.failed(exc);
            }
        });
    }

    public void write(String message, WriteHandler handler) {
        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes());
        socket.write(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                handler.completed(result);
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                handler.failed(exc);
            }
        });
    }
}
