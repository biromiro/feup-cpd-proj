package KVStore;

import Connection.AsyncTcpConnection;
import Membership.MembershipView;
import Message.ClientServerMessageProtocol;
import Message.MessageProtocolException;

public class KVStoreMessageHandler implements Runnable {
    private final String localNodeId;
    private final AsyncTcpConnection worker;
    private final MembershipView membershipView;

    public KVStoreMessageHandler(String localNodeId, AsyncTcpConnection worker, MembershipView membershipView) {
        this.localNodeId = localNodeId;
        this.worker = worker;
        this.membershipView = membershipView;
    }

    @Override
    public void run() {
        worker.read(new AsyncTcpConnection.ReadHandler() {
            @Override
            public void completed(Integer result, String message) {
                handleMessage(message);
            }

            @Override
            public void failed(Throwable exc) {
                System.out.println("Failed to read from socket");
                exc.printStackTrace();
            }
        });
    }

    private void handleMessage(String message) {
        ClientServerMessageProtocol parsedMessage;
        try {
            parsedMessage = ClientServerMessageProtocol.parse(message);
        } catch (MessageProtocolException e) {
            System.out.println("Invalid message");
            e.printStackTrace();
            return;
        }

        if (parsedMessage instanceof ClientServerMessageProtocol.Get getMessage) {
            handleGet(getMessage.getKey());
        } else if (parsedMessage instanceof ClientServerMessageProtocol.Put putMessage) {
            handlePut(putMessage.getKey(), putMessage.getValue());
        } else if (parsedMessage instanceof ClientServerMessageProtocol.Delete deleteMessage) {
            handleDelete(deleteMessage.getKey());
        } else {
            System.out.println("Unexpected message " + parsedMessage);
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
