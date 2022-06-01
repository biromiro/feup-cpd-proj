package Connection;

import java.io.IOException;
import java.net.*;

public class MulticastConnection implements AutoCloseable {
    private static final int RECEIVE_BUFFER_SIZE = 2048;
    private static final int TIMEOUT = 3000;

    private final int port;
    private final InetAddress group;
    private final MulticastSocket socket;
    private final String host;
    private boolean joined = false;

    public MulticastConnection(String host, int port) throws IOException {
        // TODO Chnage host to null
        this.port = port;
        this.host = host;
        group = InetAddress.getByName(host);
        socket = new MulticastSocket(port);
        socket.setSoTimeout(TIMEOUT);
    }

    public void send(String message) throws IOException {
        socket.send(new DatagramPacket(message.getBytes(), message.length(), group, port));
    }

    public String receive() throws IOException {
        if (!joined) {
            socket.joinGroup(new InetSocketAddress(host, port), NetworkInterface.getByName("lo"));
            this.joined = true;
        }

        byte[] byteArray = new byte[RECEIVE_BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(byteArray, byteArray.length);
        socket.receive(packet);

        return new String(packet.getData(), packet.getOffset(), packet.getLength());
    }

    public boolean isClosed() {
        return socket.isClosed();
    }

    @Override
    public void close() throws IOException {
        this.leave();
        socket.close();
    }

    public void leave() throws IOException {
        if (joined) {
            System.out.println("Leaving group");
            socket.leaveGroup(new InetSocketAddress(host, port), NetworkInterface.getByName("lo"));
            this.joined = false;
        }
    }
}
