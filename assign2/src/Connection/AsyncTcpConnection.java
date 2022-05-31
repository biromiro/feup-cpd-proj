package Connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class AsyncTcpConnection implements AutoCloseable {
    private static final int RECEIVE_BUFFER_SIZE = 2048;
    private final AsynchronousSocketChannel socket;

    public AsyncTcpConnection(AsynchronousSocketChannel socket) {
        this.socket = socket;
    }

    public AsyncTcpConnection(ThreadPoolExecutor executor, int port) {
        try {
            this.socket = AsynchronousSocketChannel.open(AsynchronousChannelGroup.withThreadPool(executor));
            this.socket.connect(new InetSocketAddress(port));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        void completed(Integer result, String message);
        void failed(Throwable exc);
    }

    public void read(ReadHandler handler) {
        ByteBuffer buffer = ByteBuffer.allocate(RECEIVE_BUFFER_SIZE);
        socket.read(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                String receivedMessage = new String(buffer.array(), buffer.arrayOffset(), buffer.array().length);
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

                handler.completed(result, message);
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                handler.failed(exc);
            }
        });
    }
}
