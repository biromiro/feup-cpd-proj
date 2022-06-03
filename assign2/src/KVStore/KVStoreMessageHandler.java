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
    private static final int REPLICATION_FACTOR = 3;
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

    private class CloseHandler implements AsyncTcpConnection.WriteHandler {
        private final String errorMessage;

        public CloseHandler(String errorMessage) {
            this.errorMessage = errorMessage;
        }

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
            throw new RuntimeException(errorMessage, exc);
        }
    }

    private void sendError(String message) {
        worker.write(ClientServerMessageProtocol.error(message),
                new CloseHandler("Error during error response."));
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

    private void redirectSkipSelf(Cluster cluster, String key) {
        List<String> successors = cluster.nNextSuccessors(key, REPLICATION_FACTOR);
        successors.remove(localNodeId);
        redirect(successors);
    }

    private void redirect(List<String> targets) {
        worker.write(ClientServerMessageProtocol.redirect(targets),
                new CloseHandler("Failed to write redirect message"));
    }

    private void handleGet(String key) {
        Cluster cluster = membershipView.getCluster();
        List<String> successors = cluster.nNextSuccessors(key, REPLICATION_FACTOR);

        if (successors.contains(localNodeId)) {
            bucket.get(key, new PersistentStorage.ReadHandler() {
                @Override
                public void completed(Integer len, String message) {
                    worker.write(ClientServerMessageProtocol.done(message),
                            new CloseHandler("Failed to give the value to requester."));
                }

                @Override
                public void failed(Throwable exc) {
                    sendError("Key not found.");
                }
            });
        } else {
            redirect(cluster.nNextSuccessors(key, REPLICATION_FACTOR));
        }
    }

    private void handlePut(String key, String value) {
        Cluster cluster = membershipView.getCluster();
        List<String> successors = cluster.nNextSuccessors(key, REPLICATION_FACTOR);
        if (successors.contains(localNodeId)) {
            bucket.put(key, value, new PersistentStorage.WriteHandler() {
                @Override
                public void completed(Integer result) {
                    redirectSkipSelf(cluster, key);
                }

                @Override
                public void failed(Throwable exc) {
                    throw new RuntimeException("Failed to write to bucket", exc);
                }
            });
        } else {
            redirect(cluster.nNextSuccessors(key, REPLICATION_FACTOR));
        }
    }

    private void handleDelete(String key) {
        Cluster cluster = membershipView.getCluster();
        List<String> successors = cluster.nNextSuccessors(key, REPLICATION_FACTOR);
        if (successors.contains(localNodeId)) {
            bucket.delete(key);
            redirectSkipSelf(cluster, key);
        } else {
            redirect(cluster.nNextSuccessors(key, REPLICATION_FACTOR));
        }
    }
}
