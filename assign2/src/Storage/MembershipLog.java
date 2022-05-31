package Storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;

public class MembershipLog {
    private static final String MEMBERSHIP_LOG_FILE = "membership_log";
    private final PersistentStorage storage;
    private List<MembershipLogEntry> log;

    public MembershipLog(PersistentStorage storage) {
        this.storage = storage;
        log = new ArrayList<>();
        try {
            // The membership log is created right when the program starts so the file is read
            // at the beginning (and only at the beginning). As such, the read may be synchronous.
            Scanner scanner = new Scanner(storage.getFileSync(MEMBERSHIP_LOG_FILE));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.isEmpty()) {
                    continue;
                }
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

    public List<MembershipLogEntry> get() {
        return log;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (MembershipLogEntry entry: this.get()) {
            sb.append(entry.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    private void addEntry(MembershipLogEntry entry) {
        Optional<MembershipLogEntry> oldEntry = this.get()
                .stream()
                .filter(e -> e.nodeId().equals(entry.nodeId()))
                .findAny();
        if (oldEntry.isPresent()) {
            if (oldEntry.get().membershipCounter() >= entry.membershipCounter()) {
                return;
            }
            this.get().remove(oldEntry.get());
        }
        this.get().add(entry);
    }

    public void log(MembershipLogEntry entry) {
        this.addEntry(entry);
        this.save();
    }

    public void log(List<MembershipLogEntry> entries) {
        for (MembershipLogEntry entry: entries) {
            System.out.println("Logging " + entry);
            this.addEntry(entry);
        }
        this.save();
    }

    private void save() {
        storage.write(MEMBERSHIP_LOG_FILE, this.toString(), new PersistentStorage.WriteHandler() {
            @Override
            public void completed(Integer result) {}

            @Override
            public void failed(Throwable exc) {
                System.out.println("Failed to write membership log: " + exc.getMessage());
            }
        });
    }
}
