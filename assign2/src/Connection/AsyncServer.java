package Connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class AsyncServer implements AutoCloseable {
    private final InetSocketAddress address;
    private final AsynchronousServerSocketChannel socket;

    public AsyncServer(InetSocketAddress address, ScheduledThreadPoolExecutor executor) throws IOException {
        this.socket = AsynchronousServerSocketChannel
                .open(AsynchronousChannelGroup.withThreadPool(executor))
                .bind(address);
        this.address = (InetSocketAddress) this.socket.getLocalAddress();
    }

    public AsyncServer(int port, ScheduledThreadPoolExecutor executor) throws IOException {
        this(new InetSocketAddress(port), executor);
    }

    public AsyncServer(ScheduledThreadPoolExecutor executor) throws IOException {
        this(new InetSocketAddress(0), executor);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    public int getPort() {
        return address.getPort();
    }

    public Future<AsynchronousSocketChannel> accept() {
        return socket.accept();
    }

    public interface ConnectionHandler {
        void completed(AsyncTcpConnection channel);
        void failed(Throwable exc);
    }

    public void loop(ConnectionHandler handler) {
        socket.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
            public void completed(AsynchronousSocketChannel channel, Void att) {
                socket.accept(null, this);

                handler.completed(new AsyncTcpConnection(channel));
            }
            public void failed(Throwable exc, Void att) {
                handler.failed(exc);
            }
        });
    }
}
