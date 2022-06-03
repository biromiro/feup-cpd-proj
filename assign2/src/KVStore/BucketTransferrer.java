package KVStore;

import Connection.AsyncTcpConnection;
import Message.ClientServerMessageProtocol;
import Storage.Bucket;
import Storage.PersistentStorage;

import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ScheduledThreadPoolExecutor;

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

        public FetchHandler(ListIterator<String> iterator) {
            this.iterator = iterator;
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
                    System.out.println("THE LAST PREDECESSOR IS " + lastPredecessor);

                    List<String> keys = bucket.getMarkedKeys();
                    List<String> toDelete = keys.stream()
                            .filter(key -> cluster
                                    .nNextSuccessors(key.split("\\.")[0], 1)
                                    .contains(lastPredecessor))
                            .toList();

                    System.out.println(cluster
                            .nNextSuccessors(keys.get(0).split("\\.")[0], 1));
                    System.out.println(toDelete);
                    System.out.println("ABOUT TO DELETE!!!");

                    for (String key: toDelete) {
                        System.out.println("Dafq, should have deleted...");
                        bucket.delete(key);
                    }
                }

                return;
            }

            String key = iterator.next();
            bucket.get(key, new TransferHandler(key, this));
        }

        @Override
        public void failed(Throwable exc) {
            throw new RuntimeException(exc);
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
            throw new RuntimeException(exc);
        }
    }

    private void transferKeys(String destination, List<String> keys) {
        if (keys.isEmpty())
            return;
        ListIterator<String> iterator = keys.listIterator();
        AsyncTcpConnection.connect(executor, destination, storePort, new FetchHandler(iterator));
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

    public void transfer() {
        List<String> destinations = cluster.nNextSuccessors(nodeId, Cluster.REPLICATION_FACTOR + 1);
        destinations.remove(nodeId);
        List<String> sources = cluster.nPreviousPredecessors(nodeId, Cluster.REPLICATION_FACTOR - 1);
        List<String> keys = bucket.getMarkedKeys();
        for (int i = 0; i < destinations.size(); i++) {
            int index = i;
            transferKeys(destinations.get(i), keys);
            if (i == destinations.size() - 1) {
                break;
            }
            keys = keys.stream()
                    .filter(key -> key.compareTo(sources.get(sources.size() - 1 - index)) > 0)
                    .toList();
        }
    }
}
