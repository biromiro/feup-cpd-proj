package Connection;

import java.io.IOException;
import java.net.*;

public class MulticastConnection implements AutoCloseable {
    private static final int RECEIVE_BUFFER_SIZE = 1024;
    private final int port;
    private final InetAddress group;
    private final MulticastSocket socket;
    private boolean joined = false;

    public MulticastConnection(String host, int port) throws IOException {
        this.port = port;
        group = InetAddress.getByName(host);
        socket = new MulticastSocket(port);
    }

    public void send(String message) throws IOException {
        System.out.println("sending " +  group + " " +  port);
        socket.send(new DatagramPacket(message.getBytes(), message.length(), group, port));
    }

    public String receive() throws IOException {
        if (!joined) {
            socket.joinGroup(new InetSocketAddress(group, port), NetworkInterface.getByName("lo"));
            this.joined = true;
        }

        byte[] byteArray = new byte[RECEIVE_BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(byteArray, byteArray.length);
        System.out.println("waiting " +  group + " " +  port);
        socket.receive(packet);
        System.out.println("received");

        return new String(packet.getData(), packet.getOffset(), packet.getLength());
    }

    public boolean isClosed() {
        return socket.isClosed();
    }

    public void close() throws IOException {
        if (joined) {
            socket.leaveGroup(new InetSocketAddress(group, port), NetworkInterface.getByName("lo"));
        }

        socket.close();
    }
}
