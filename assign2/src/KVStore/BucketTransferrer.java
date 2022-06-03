package KVStore;

import Connection.AsyncTcpConnection;
import Message.ClientServerMessageProtocol;
import Storage.Bucket;
import Storage.PersistentStorage;

import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BucketTransferrer {
    private final String nodeId;
    private final Bucket bucket;
    private final Cluster cluster;
    private final ScheduledThreadPoolExecutor executor;
    private final int storePort;

    public BucketTransferrer(String nodeId, Bucket bucket, Cluster cluster, ScheduledThreadPoolExecutor executor, int storePort) {
        this.nodeId = nodeId;
        this.bucket = bucket;
        this.cluster = cluster;
        this.executor = executor;
        this.storePort = storePort;
    }

    private class ConnectionHandler implements AsyncTcpConnection.ConnectionHandler {
        private final ListIterator<String> iterator;
        private final String destination;
        private final Function<Void, Void> whenTransferred;

        public ConnectionHandler(ListIterator<String> iterator, String destination, Function<Void, Void> whenTransferred) {
            this.iterator = iterator;
            this.destination = destination;
            this.whenTransferred = whenTransferred;
        }

        @Override
        public void completed(AsyncTcpConnection connection) {
            if (!iterator.hasNext()) {
                if (cluster.size() > Cluster.REPLICATION_FACTOR) {
                    List<String> predecessors = cluster.nPreviousPredecessors(nodeId);
                    String lastPredecessor = predecessors.get(predecessors.size() - 1);

                    List<String> keys = bucket.getMarkedKeys();
                    List<String> toDelete = keys.stream()
                            .filter(key -> cluster
                                    .nNextSuccessors(key.split("\\.")[0], 1)
                                    .contains(lastPredecessor))
                            .toList();

                    for (String key: toDelete) {
                        bucket.destroy(key);
                    }
                }

                whenTransferred.apply(null);

                return;
            }

            String key = iterator.next();
            bucket.get(key, new ReadHandler(key, connection, this));
        }

        @Override
        public void failed(Throwable exc) {
            exc.printStackTrace();
            whenTransferred.apply(null);
        }

        public Function<Void, Void> getWhenTransferred() {
            return whenTransferred;
        }

        public String getDestination() {
            return destination;
        }
    }

    private class ReadHandler implements PersistentStorage.ReadHandler {
        private final AsyncTcpConnection connection;
        private final ConnectionHandler handler;
        private final String key;

        public ReadHandler(String key, AsyncTcpConnection connection, ConnectionHandler handler) {
            this.connection = connection;
            this.handler = handler;
            this.key = key;
        }

        @Override
        public void completed(Integer len, String message) {
            connection.write(ClientServerMessageProtocol.transfer(key, message),
                    new SentHandler(handler));
        }

        @Override
        public void failed(Throwable exc) {
            exc.printStackTrace();
            handler.getWhenTransferred().apply(null);
        }
    }

    private class SentHandler implements AsyncTcpConnection.WriteHandler {
        private final ConnectionHandler handler;

        public SentHandler(ConnectionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void completed(Integer result) {
            AsyncTcpConnection.connect(executor, handler.getDestination(), storePort, handler);
        }

        @Override
        public void failed(Throwable exc) {
            exc.printStackTrace();
            this.handler.getWhenTransferred().apply(null);
        }
    }

    private void transferKeys(String destination, List<String> keys, Function<Void, Void> whenTransferred) {
        if (keys.isEmpty()) {
            whenTransferred.apply(null);
            return;
        }
        ListIterator<String> iterator = keys.listIterator();
        AsyncTcpConnection.connect(executor, destination, storePort,
                new ConnectionHandler(iterator, destination, whenTransferred));
    }

    private void transferKeys(String destination, List<String> keys) {
        transferKeys(destination, keys, (_null) -> null);
    }

    public void transfer(String newcomer) {
        if (!cluster.nPreviousPredecessors(nodeId).contains(newcomer))
            return;

        List<String> keys = bucket.getMarkedKeys();

        List<String> toTransfer;
        if (cluster.size() <= Cluster.REPLICATION_FACTOR) {
            toTransfer = keys;
        } else {
            toTransfer = keys.stream()
                    .filter(key -> !cluster
                            .nNextSuccessors(key.split("\\.")[0], 1)
                            .contains(this.nodeId))
                    .toList();
        }

        transferKeys(newcomer, toTransfer);
    }

    public void transfer(Function<Void, Void> whenTransferred) {
        List<String> destinations = cluster.nNextSuccessors(nodeId, Cluster.REPLICATION_FACTOR + 1);
        destinations.remove(nodeId);
        List<String> sources = cluster.nPreviousPredecessors(nodeId, Cluster.REPLICATION_FACTOR - 1);
        List<String> keys = bucket.getMarkedKeys();

        int[] completed = new int[1];
        for (int i = 0; i < destinations.size(); i++) {
            int index = i;
            transferKeys(destinations.get(i), keys, (_null) -> {
                synchronized (completed) {
                    completed[0]++;
                    if (completed[0] == destinations.size()) {
                        for (String key: bucket.getMarkedKeys()) {
                            bucket.destroy(key);
                        }
                        whenTransferred.apply(null);
                    }
                }
                return null;
            });
            if (i == destinations.size() - 1) {
                break;
            }

            keys = keys.stream()
                    .filter(key -> !cluster
                            .nNextSuccessors(key.split("\\.")[0], 1)
                            .contains(sources.get(sources.size() - 1 - index)))
                    .toList();
        }
    }

    public void reinitialize(String nodeId) {
        List<String> successors = cluster.nNextSuccessors(nodeId);
        List<String> predecessors = cluster.nPreviousPredecessors(nodeId, Cluster.REPLICATION_FACTOR - 1);

        if (!successors.contains(this.nodeId) && !predecessors.contains(this.nodeId)) {
            return;
        }

        List<String> keys = bucket.getMarkedKeys()
                .stream()
                .filter(key -> cluster.nNextSuccessors(key).contains(nodeId))
                .collect(Collectors.toList());

        transferKeys(nodeId, keys);
    }
}
