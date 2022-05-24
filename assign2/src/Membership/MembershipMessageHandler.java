package Membership;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class MembershipMessageHandler implements Runnable {

    private final Socket socket;

    MembershipMessageHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        InputStream input = null;
        try {
            input = socket.getInputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));

        String time = null;
        try {
            time = reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("New MEMBERSHIP message: " + time);
        // TODO parse membership message
    }
}
