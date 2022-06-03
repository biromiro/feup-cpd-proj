package KVStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Cluster {
    public static final int REPLICATION_FACTOR = 3;
    private final List<KVEntry> nodes;

    public Cluster() {
        nodes = new ArrayList<>();
    }

    public Cluster(List<String> members) {
        nodes = new ArrayList<>();
        for (String node: members) {
            add(node);
        }
    }

    private int insertionPoint(int failedIndex) {
        return -failedIndex - 1;
    }

    public boolean add(String node) {
        KVEntry entry = new KVEntry(node);
        int index = Collections.binarySearch(nodes, entry);
        if (index >= 0) {
            return false;
        }
        nodes.add(insertionPoint(index), entry);
        return true;
    }

    public void remove(String node) {
        int index = Collections.binarySearch(nodes, new KVEntry(node));
        if (index < 0) {
            return;
        }
        nodes.remove(index);
    }

    public List<String> nPreviousPredecessors(String key) {
        return nPreviousPredecessors(key, REPLICATION_FACTOR);
    }
    public List<String> nPreviousPredecessors(String key, int n) {
        if (n <= 0) {
            return new ArrayList<>();
        }
        if (n > nodes.size()) {
            return getMembers();
        }
        List<String> predecessors = new ArrayList<>();
        int index = predecessorIndex(key);
        for (int i = 0; i < n; i++) {
            String node = nodes.get(index).getValue();
            predecessors.add(node);
            index -= 1;
            if (index < 0) {
                index = nodes.size() - 1;
            }
        }
        return predecessors;
    }

    private int predecessorIndex(String key) {
        int index = Collections.binarySearch(nodes, new KVEntry(key));
        if (index < 0) {
            index = insertionPoint(index);
        }
        index -= 1;
        if (index < 0) {
            index = nodes.size() - 1;
        }
        return index;
    }

    private int successorIndex(String key) {
        int index = Collections.binarySearch(nodes, new KVEntry(key));
        if (index < 0) {
            index = insertionPoint(index) % nodes.size();
        }
        return index;
    }

    public List<String> nNextSuccessors(String key) {
        return nNextSuccessors(key, REPLICATION_FACTOR);
    }
    public List<String> nNextSuccessors(String key, int n) {
        if (n <= 0) {
            return new ArrayList<>();
        }
        if (n > nodes.size()) {
            return getMembers();
        }
        List<String> successors = new ArrayList<>();
        int index = successorIndex(key);
        for (int i = 0; i < n; i++) {
            String node = nodes.get(index).getValue();
            successors.add(node);
            index = (index + 1) % nodes.size();
        }
        return successors;
    }

    public List<String> getMembers() {
        return nodes.stream().map(KVEntry::getValue).collect(Collectors.toList());
    }

    public int size() {
        return nodes.size();
    }
}
