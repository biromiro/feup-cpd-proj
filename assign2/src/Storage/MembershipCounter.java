package Storage;

import java.io.FileNotFoundException;
import java.io.IOException;

public class MembershipCounter {
    private static final String MEMBERSHIP_COUNTER_FILE = "membership_counter";
    private final PersistentStorage storage;
    private Integer count;

    public MembershipCounter(PersistentStorage storage) {
        this.storage = storage;
        try {
            // The membership counter is needed right when the program starts so the file is read
            // at the beginning (and only at the beginning). As such, the read may be synchronous.
            count = Integer.parseInt(storage.readSync(MEMBERSHIP_COUNTER_FILE));
        } catch (FileNotFoundException e) {
            count = -1;
        }
    }

    public int get() {
        return count;
    }

    public void increment() throws IOException {
        get();
        count++;
        // The membership counter is only updated at the start of an RMI function (join/leave)
        // As such, the read may be synchronous.
        storage.writeSync(MEMBERSHIP_COUNTER_FILE, String.valueOf(count));
    }

    public boolean isJoin() {
        return isJoin(this.get());
    }
    public boolean isLeave() {
        return isLeave(this.get());
    }

    public static boolean isJoin(int count) { return count % 2 == 0; }
    public static boolean isLeave(int count) { return count % 2 == 1; }
}
