package KVStore;

import Connection.AsyncTcpConnection;
import Membership.MembershipView;
import Message.ClientServerMessageProtocol;
import Message.MessageProtocolException;
import Storage.Bucket;
import Storage.PersistentStorage;

import java.io.IOException;
import java.util.List;

public class KVStoreMessageHandler {
    private final boolean acceptingMessages;
    private final String localNodeId;
    private final AsyncTcpConnection worker;
    private final Bucket bucket;
    private final MembershipView membershipView;

    public KVStoreMessageHandler(boolean acceptingMessages, String localNodeId, AsyncTcpConnection worker,
                                 Bucket bucket, MembershipView membershipView) {
        this.acceptingMessages = acceptingMessages;
        this.localNodeId = localNodeId;
        this.worker = worker;
        this.bucket = bucket;
        this.membershipView = membershipView;
    }

    public void run() {
        worker.read(new AsyncTcpConnection.ReadHandler() {
            @Override
            public void completed(Integer result, String message) {
                if (acceptingMessages) {
                    handleMessage(message);
                } else {
                    sendError("This node doesn't belong to a cluster.");
                }
            }

            @Override
            public void failed(Throwable exc) {
                System.out.println("Failed to read from socket");
                exc.printStackTrace();
            }
        });
    }

    private void sendError(String message) {
        worker.write(ClientServerMessageProtocol.error(message), new AsyncTcpConnection.WriteHandler() {
            @Override
            public void completed(Integer result) {
                try {
                    worker.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void failed(Throwable exc) {
                throw new RuntimeException(exc);
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
        } else if (parsedMessage instanceof ClientServerMessageProtocol.Transfer transferMessage){
            handleTransfer(transferMessage.getKey(), transferMessage.getValue());
        }
        else {
            System.out.println("Unexpected message " + parsedMessage);
        }
    }

    private void redirect(List<String> targets) {
        System.out.println("ALMOST...");
        worker.write(ClientServerMessageProtocol.redirect(targets), new AsyncTcpConnection.WriteHandler() {
            @Override
            public void completed(Integer result) {
                try {
                    worker.close();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to close connection", e);
                }
            }

            @Override
            public void failed(Throwable exc) {
                throw new RuntimeException("Failed to write redirect message", exc);
            }
        });
    }

    private class LocalGetHandler implements PersistentStorage.ReadHandler {

        @Override
        public void completed(Integer len, String message) {
            worker.write(ClientServerMessageProtocol.done(message), new ReturnGetHandler());
        }

        @Override
        public void failed(Throwable exc) {
            sendError("Key not found.");
        }
    }

    private class ReturnGetHandler implements AsyncTcpConnection.WriteHandler {
        @Override
        public void completed(Integer result) {
            try {
                worker.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close connection");
            }
        }

        @Override
        public void failed(Throwable exc) {
            throw new RuntimeException("Failed to give the value to requester.", exc);
        }
    }

    private void handleGet(String key) {
        Cluster cluster = membershipView.getCluster();
        List<String> successors = cluster.nNextSuccessors(key);
        // If node has key, get it and send it back
        System.out.println("HANDLING GET");
        if (successors.contains(localNodeId)) {
            bucket.get(key, new LocalGetHandler());
        } else {
            System.out.println("REDIRECTING");
            redirect(cluster.nNextSuccessors(key));
        }
    }

    private class LocalPutHandler implements PersistentStorage.WriteHandler {

        @Override
        public void completed(Integer result) {
            try {
                worker.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close connection", e);
            }
        }

        @Override
        public void failed(Throwable exc) {
            throw new RuntimeException("Failed to put on my bucket", exc);
        }
    }

    private void redirectToReplicators(Cluster cluster, String key) {
        List<String> successors = cluster.nNextSuccessors(key);
        successors.remove(localNodeId);
        redirect(successors);
    }

    private void handlePut(String key, String value) {
        Cluster cluster = membershipView.getCluster();
        List<String> successors = cluster.nNextSuccessors(key);
        if (successors.contains(localNodeId)) {
            bucket.put(key, value, new LocalPutHandler());
            redirectToReplicators(cluster, key);
        } else {
            redirect(cluster.nNextSuccessors(key));
        }
    }

    private void handleDelete(String key) {
        Cluster cluster = membershipView.getCluster();
        List<String> successors = cluster.nNextSuccessors(key);
        if (successors.contains(localNodeId)) {
            bucket.delete(key);
            redirectToReplicators(cluster, key);
        } else {
            redirect(cluster.nNextSuccessors(key));
        }
    }

    private class TransferHandler implements PersistentStorage.WriteHandler {
        @Override
        public void completed(Integer result) {
            try {
                worker.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close connection", e);
            }
        }

        @Override
        public void failed(Throwable exc) {
            throw new RuntimeException(exc);
        }
    }

    private void handleTransfer(String key, String value) {
        String[] parts = key.split("\\.");
        if (key.equals(parts[0])) {
            bucket.put(key, value, new TransferHandler());
        }
        else {
            bucket.delete(parts[0]);
            try {
                worker.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close connection", e);
            }
        }
    }
}
