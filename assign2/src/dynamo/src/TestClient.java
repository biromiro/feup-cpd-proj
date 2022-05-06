public class TestClient {
    private static void printUsage() {
        System.out.println("java TestClient <node_ap> <operation> [<opnd>]");
    }

    public static void main(String[] args) {
        for (String arg : args)
            System.out.println(arg);

        if (args.length < 2) {
            printUsage();
            return;
        }

        String remoteObj = args[0];
        String operation = args[1];

        if((operation.equals("join") || operation.equals("leave")) && args.length == 2) {
            //...
        }

        else if((operation.equals("put") || operation.equals("get") || operation.equals("delete")) && args.length == 3) {
            //...
        }

        else {
            printUsage();
        }
    }
}
