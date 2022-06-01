package KVStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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

    public String successor(String key) {
        int index = Collections.binarySearch(nodes, new KVEntry(key));
        if (index < 0) {
            index = insertionPoint(index) % nodes.size();
        }
        return nodes.get(index).getValue();
    }

    public boolean contains(String node) {
        for (KVEntry entry : nodes) {
            if (Objects.equals(entry.getValue(), node)) {
                return true;
            }
        }
        return false;
    }

    public List<String> getMembers() {
        return nodes.stream().map(KVEntry::getValue).collect(Collectors.toList());
    }
}
