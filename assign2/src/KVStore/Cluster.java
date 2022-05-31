package KVStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Cluster {
    private final List<NodeEntry> nodes;

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

    public boolean add(String node) {
        NodeEntry nodeEntry = new NodeEntry(node);
        int index = Collections.binarySearch(nodes, nodeEntry);
        if (index >= 0) {
            return false;
        }
        nodes.add(insertionPoint(index), nodeEntry);
        return true;
    }

    public boolean remove(String node) {
        int index = Collections.binarySearch(nodes, new NodeEntry(node));
        if (index < 0) {
            return false;
        }
        nodes.remove(index);
        return true;
    }

    public String predecessor(String key) {
        int index = Collections.binarySearch(nodes, new NodeEntry(key));
        if (index < 0) {
            index = insertionPoint(index);
        }
        index = (index - 1) % nodes.size();
        return nodes.get(index).getValue();
    }

    public String successor(String key) {
        int index = Collections.binarySearch(nodes, new NodeEntry(key));
        if (index < 0) {
            index = insertionPoint(index) % nodes.size();
        }
        return nodes.get(index).getValue();
    }

    public List<String> getMembers() {
        return nodes.stream().map(NodeEntry::getValue).collect(Collectors.toList());
    }
}
