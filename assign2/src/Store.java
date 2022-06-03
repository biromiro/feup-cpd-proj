public class Store {
    private static void printUsage() {
        System.out.println("java Store <IP_mcast_addr> <IP_mcast_port> <node_id>  <Store_port>");
    }

    private static void startStore(String mcastAddr, int mcastPort, String nodeId, int storePort) {
        Node node = new Node(mcastAddr, mcastPort, nodeId, storePort);
        node.bindRMI(nodeId);
        node.start();
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            printUsage();
            return;
        }

        String mcast_addr, node_id;
        int mcast_port, store_port;

        mcast_addr = args[0];
        try {
            mcast_port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid mcast port " + args[1] + ".");
            printUsage();
            return;
        }
        node_id = args[2];
        try {
            store_port = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid store port " + args[1] + ".");
            printUsage();
            return;
        }

        startStore(mcast_addr, mcast_port, node_id, store_port);
    }
}
