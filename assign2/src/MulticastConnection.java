import java.io.IOException;
import java.net.*;

public class MulticastConnection implements AutoCloseable {
    private final int port;
    private final InetAddress group;
    private final MulticastSocket socket;

    public MulticastConnection(String host, int port) throws IOException {
        this.port = port;
        group = InetAddress.getByName(host);
        socket = new MulticastSocket();
        socket.joinGroup(new InetSocketAddress(host, port), NetworkInterface.getByName("lo"));
    }

    public void send(String message) throws IOException {
        socket.send(new DatagramPacket(message.getBytes(), message.length(), group, port));
    }

    public void close() throws IOException {
        socket.close();
    }
}
