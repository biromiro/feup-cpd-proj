package Membership;

import Storage.PersistentStorage;

import java.io.FileNotFoundException;
import java.io.IOException;

public class MembershipCounter {
    private static final String MEMBERSHIP_COUNTER_FILE = "membership_counter";
    private final PersistentStorage storage;
    private Integer count = null;

    public MembershipCounter(PersistentStorage storage) {
        this.storage = storage;
    }

    public int get() {
        if (count == null) {
            try {
                count = Integer.parseInt(storage.read(MEMBERSHIP_COUNTER_FILE));
            } catch (FileNotFoundException e) {
                count = -1;
            }
        }
        return count;
    }

    public int increment() throws IOException {
        get();
        count++;
        storage.write(MEMBERSHIP_COUNTER_FILE, String.valueOf(count));
        return count;
    }

    public boolean isJoinCount() {
        return this.get() % 2 == 0;
    }

    public boolean isLeaveCount() {
        return this.get() % 2 == 1;
    }
}
