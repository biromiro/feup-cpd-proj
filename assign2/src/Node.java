public class Node implements MembershipService {
    @Override
    public void join() {
        System.out.println("Node joined");
    }

    @Override
    public void leave() {
        System.out.println("Node left");
    }
}
