package KVStore;

import Membership.MembershipView;
import Message.ClientServerMessageProtocol;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

public class KVStoreMessageHandler implements Runnable {
    private static final int RECEIVE_BUFFER_SIZE = 2048;
    private final String localNodeId;
    private final AsynchronousSocketChannel worker;
    private final MembershipView membershipView;

    public KVStoreMessageHandler(String localNodeId, AsynchronousSocketChannel worker, MembershipView membershipView) {
        this.localNodeId = localNodeId;
        this.worker = worker;
        this.membershipView = membershipView;
    }

    @Override
    public void run() {
        ByteBuffer buffer = ByteBuffer.allocate(RECEIVE_BUFFER_SIZE);
        worker.read(buffer);

        String receivedMessage = new String(buffer.array(), buffer.arrayOffset(), buffer.array().length);

        ClientServerMessageProtocol parsedMessage;
        try {
            parsedMessage = ClientServerMessageProtocol.parse(receivedMessage);
        } catch (MessageProtocolException e) {
            throw new RuntimeException("Invalid message", e);
        }

        if (parsedMessage instanceof ClientServerMessageProtocol.Get getMessage) {
            handleGet(getMessage.getKey());
        } else if (parsedMessage instanceof ClientServerMessageProtocol.Put putMessage) {
            handlePut(putMessage.getKey(), putMessage.getValue());
        } else if (parsedMessage instanceof ClientServerMessageProtocol.Delete deleteMessage) {
            handleDelete(deleteMessage.getKey());
        } else {
            throw new RuntimeException("Unexpected message " + parsedMessage);
        }
    }

    private void handleGet(String key) {
        Cluster cluster = membershipView.getCluster();
        String successor = cluster.successor(key);
        if (successor.equals(this.localNodeId)) {
            // Answer
        } else {
            // Ask successor
        }

        //TCP connection to get entry from there
        // TODO
    }

    private void handlePut(String key, String value) {
        // TODO
    }

    private void handleDelete(String key) {
        // TODO
    }

}
