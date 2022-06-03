package KVStore;

import Connection.AsyncTcpConnection;
import Membership.MembershipHandler;
import Message.ClientServerMessageProtocol;
import Storage.Bucket;
import Storage.PersistentStorage;

import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;

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

    private class FetchHandler implements AsyncTcpConnection.ConnectionHandler, AsyncTcpConnection.WriteHandler {
        private final ListIterator<String> iterator;
        private AsyncTcpConnection connection;
        private final Function<Void, Void> whenTransferred;

        public FetchHandler(ListIterator<String> iterator, Function<Void, Void> whenTransferred) {
            this.iterator = iterator;
            this.whenTransferred = whenTransferred;
        }

        public AsyncTcpConnection getConnection() {
            return connection;
        }

        @Override
        public void completed(Integer result) {
            completed(connection);
        }
        @Override
        public void completed(AsyncTcpConnection connection) {
            this.connection = connection;

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
            bucket.get(key, new TransferHandler(key, this));
        }

        @Override
        public void failed(Throwable exc) {
            exc.printStackTrace();
            whenTransferred.apply(null);
        }

        public Function<Void, Void> getWhenTransferred() {
            return whenTransferred;
        }
    }

    private class TransferHandler implements PersistentStorage.ReadHandler {
        private final FetchHandler handler;
        private final String key;

        public TransferHandler(String key, FetchHandler handler) {
            this.handler = handler;
            this.key = key;
        }

        @Override
        public void completed(Integer len, String message) {
            handler.getConnection().write(ClientServerMessageProtocol.transfer(key, message), handler);
        }

        @Override
        public void failed(Throwable exc) {
            exc.printStackTrace();
            handler.getWhenTransferred().apply(null);
        }
    }

    private void transferKeys(String destination, List<String> keys, Function<Void, Void> whenTransferred) {
        if (keys.isEmpty()) {
            whenTransferred.apply(null);
            return;
        }
        ListIterator<String> iterator = keys.listIterator();
        AsyncTcpConnection.connect(executor, destination, storePort, new FetchHandler(iterator, whenTransferred));
    }

    private void transferKeys(String destination, List<String> keys) {
        transferKeys(destination, keys, (_null) -> null);
    }

    public void transfer(String newcomer) {
        if (!cluster.nPreviousPredecessors(nodeId).contains(newcomer))
            return;

        List<String> keys = bucket.getMarkedKeys(); // TODO devia ser async

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
        System.out.println(destinations);
        System.out.println(destinations.size());
        for (int i = 0; i < destinations.size(); i++) {
            int index = i;
            System.out.println("THE FUCKING INDEX IS " + i);
            transferKeys(destinations.get(i), keys, (_null) -> {
                System.out.println("THE TRANSFER SHOULD HAVE BEEN COMPLETEEEED. " + completed[0]);
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
}
