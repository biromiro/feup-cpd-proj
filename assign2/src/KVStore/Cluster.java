package KVStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Cluster {
    private final List<KVEntry> nodes;

    public Cluster() {
        nodes = new ArrayList<>();
    }

    public Cluster(List<String> members) {
        nodes = new ArrayList<>();
        for (String node : members) {
            add(node);
        }
    }

    private int insertionPoint(int failedIndex) {
        return -failedIndex - 1;
    }

    public void add(String node) {
        KVEntry entry = new KVEntry(node);
        int index = Collections.binarySearch(nodes, entry);
        if (index >= 0) {
            return;
        }
        nodes.add(insertionPoint(index), entry);
    }

    public void remove(String node) {
        int index = Collections.binarySearch(nodes, new KVEntry(node));
        if (index < 0) {
            return;
        }
        nodes.remove(index);
    }

    public String predecessor(String key) {
        int index = Collections.binarySearch(nodes, new KVEntry(key));
        if (index < 0) {
            index = insertionPoint(index);
        }
        index = (index - 1) % nodes.size();
        return nodes.get(index).getValue();
    }

    private int successorIndex(String key) {
        int index = Collections.binarySearch(nodes, new KVEntry(key));
        if (index < 0) {
            index = insertionPoint(index) % nodes.size();
        }
        return index;
    }

    public String successor(String key) {
        int index = successorIndex(key);
        return nodes.get(index).getValue();
    }

    public List<String> nNextSuccessors(String key, int n) {
        if (n <= 0) {
            return new ArrayList<>();
        }
        if (n > nodes.size()) {
            n = nodes.size();
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
