package KVStore;

import Connection.AsyncTcpConnection;
import Membership.MembershipView;
import Message.ClientServerMessageProtocol;
import Message.MessageProtocolException;
import Storage.Bucket;
import Storage.PersistentStorage;

public class KVStoreMessageHandler {
    private final String localNodeId;
    private final AsyncTcpConnection worker;
    private final Bucket bucket;
    private final MembershipView membershipView;

    public KVStoreMessageHandler(String localNodeId, AsyncTcpConnection worker, Bucket bucket, MembershipView membershipView) {
        this.localNodeId = localNodeId;
        this.worker = worker;
        this.bucket = bucket;
        this.membershipView = membershipView;
    }

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

    private class LocalGetHandler implements PersistentStorage.ReadHandler {

        @Override
        public void completed(Integer len, String message) {
            worker.write(message, new ReturnGetHandler());
        }

        @Override
        public void failed(Throwable exc) {
            throw new RuntimeException("Failed to get from my bucket", exc);
        }
    }

    private class ReturnGetHandler implements AsyncTcpConnection.WriteHandler {
        @Override
        public void completed(Integer result) {

        }

        @Override
        public void failed(Throwable exc) {
            throw new RuntimeException("Failed to give the value to requester.", exc);
        }
    }

    private void handleGet(String key) {
        Cluster cluster = membershipView.getCluster();
        String successor = cluster.successor(key);
        // If node has key, get it and send it back
        if (successor.equals(localNodeId)) {
            bucket.get(key, new LocalGetHandler());
        } else {
            //TODO connect w/ TCP to destination node, ask for get, wait for response, then send it back.
        }
    }

    private class LocalPutHandler implements PersistentStorage.WriteHandler {

        @Override
        public void completed(Integer result) {

        }

        @Override
        public void failed(Throwable exc) {
            throw new RuntimeException("Failed to put on my bucket", exc);
        }
    }

    private void handlePut(String key, String value) {
        Cluster cluster = membershipView.getCluster();
        String successor = cluster.successor(key);
        if (successor.equals(localNodeId)) {
            bucket.put(key, value, new LocalPutHandler());
        } else {
            //TODO connect w/ TCP to destination node and ask for put.
        }
    }

    private void handleDelete(String key) {
        Cluster cluster = membershipView.getCluster();
        String successor = cluster.successor(key);
        if (successor.equals(localNodeId)) {
            bucket.delete(key);
        } else {
            //TODO connect w/ TCP to destination node and ask for deletion.
        }
    }

}
