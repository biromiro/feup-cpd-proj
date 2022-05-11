import java.io.IOException;

public class Node implements MembershipService {
    private final PersistentStorage storage;
    public Node(PersistentStorage storage) {
        this.storage = storage;
    }

    @Override
    public void join() {
        System.out.println("Node joined");
        MembershipCounter membershipCounter = new MembershipCounter(storage);

        try {
            membershipCounter.increment();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void leave() {
        System.out.println("Node left");
    }
}
