package Message;

import Storage.PersistentStorage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class MembershipLog {
    private static final String MEMBERSHIP_LOG_FILE = "membership_log";
    private final PersistentStorage storage;
    private List<MembershipLogEntry> log = null;

    public MembershipLog(PersistentStorage storage) {
        this.storage = storage;
    }

    public List<MembershipLogEntry> get() {
        if (log == null) {
            log = new ArrayList<>();
            try {
                Scanner scanner = new Scanner(storage.getFile(MEMBERSHIP_LOG_FILE));
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    int delimiter = line.lastIndexOf(' ');
                    String nodeId = line.substring(0, delimiter);
                    int membershipCount = Integer.parseInt(line.substring(delimiter + 1));
                    log.add(new MembershipLogEntry(nodeId, membershipCount));
                }
                scanner.close();
            } catch (FileNotFoundException e) {
                // File does not exist, so there are no entries
            }
        }
        return log;
    }

    public void log(MembershipLogEntry entry) throws IOException {
        if (log != null) {
            log.add(entry);
        }
        storage.append(MEMBERSHIP_LOG_FILE, entry.toString());
    }
}
