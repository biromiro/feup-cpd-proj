package Membership;

import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Arrays;

public class MembershipMessageHandler implements Runnable {
    private static final int RECEIVE_BUFFER_SIZE = 2048;
    private final AsynchronousSocketChannel worker;
    private final MembershipView membershipView;

    MembershipMessageHandler(AsynchronousSocketChannel worker, MembershipView membershipView) {
        this.worker = worker;
        this.membershipView = membershipView;
    }

    @Override
    public void run() {
        // read until token is received
        ByteBuffer buffer = ByteBuffer.allocate(RECEIVE_BUFFER_SIZE);
        worker.read(buffer);

        String receivedMessage = new String(buffer.array(), buffer.arrayOffset(), buffer.array().length);

        System.out.println("Gonna parse it");
        MembershipMessageProtocol parsedMessage;
        try {
            parsedMessage = MembershipMessageProtocol.parse(receivedMessage);
        } catch (MessageProtocolException e) {
            throw new RuntimeException("Invalid message", e);
        }

        if (parsedMessage instanceof MembershipMessageProtocol.MembershipMessage membershipMessage) {
            System.out.println("received membership message");
            handleMembership(membershipMessage);
        } else {
            throw new RuntimeException("Unexpected message " + parsedMessage);
        }
    }

    private void handleMembership(MembershipMessageProtocol.MembershipMessage membershipMessage) {
        membershipView.merge(membershipMessage.getMembers(), membershipMessage.getLog());
        // TODO
    }
}
