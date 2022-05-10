import java.io.IOException;
import java.net.*;

public class Application {
    public static void main(String[] args) throws IOException {
        String host = "239.239.239.99";
        int port = 9998;
        String message = "test-multicastSocket";
        try {
            InetAddress group = InetAddress.getByName(host);
            MulticastSocket s = new MulticastSocket();

            NetworkInterface local = NetworkInterface.getByName("lo");

            s.joinGroup(new InetSocketAddress(host, port), local);
            DatagramPacket dp = new DatagramPacket(message.getBytes(), message.length(), group, port);
            s.send(dp);
            s.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        }
}
