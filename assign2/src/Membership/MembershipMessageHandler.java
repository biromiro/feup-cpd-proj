package Membership;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Arrays;

public class MembershipMessageHandler implements Runnable {

    private final AsynchronousSocketChannel worker;

    MembershipMessageHandler(AsynchronousSocketChannel worker) {
        this.worker = worker;
    }

    @Override
    public void run() {
        // read until token is received
        ByteBuffer buffer = ByteBuffer.allocate(32);
        worker.read(buffer);

        String time = Arrays.toString(buffer.array());
        System.out.println("New MEMBERSHIP message: " + time);
        // TODO parse membership message
    }
}
