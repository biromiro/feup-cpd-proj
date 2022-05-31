package Connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class AsyncServer implements AutoCloseable {
    private final InetSocketAddress address;
    private final AsynchronousServerSocketChannel socket;

    public AsyncServer(InetSocketAddress address) throws IOException {
        this.address = address;
        this.socket = AsynchronousServerSocketChannel.open().bind(address);
    }

    public AsyncServer(int port) throws IOException {
        this(new InetSocketAddress(port));
    }

    public AsyncServer() throws IOException {
        this(new InetSocketAddress(0));
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
