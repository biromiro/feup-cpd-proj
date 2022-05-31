package Storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Collectors;

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

    private int addEntry(MembershipLogEntry entry) {
        Optional<MembershipLogEntry> oldEntry = this.get()
                .stream()
                .filter(e -> e.nodeId().equals(entry.nodeId()))
                .findAny();
        if (oldEntry.isPresent()) {
            if (oldEntry.get().membershipCounter() >= entry.membershipCounter()) {
                return oldEntry.get().membershipCounter();
            }
            this.get().remove(oldEntry.get());
        }
        this.get().add(entry);
        return entry.membershipCounter();
    }

    public int log(MembershipLogEntry entry) {
        int counter = this.addEntry(entry);
        this.save();
        return counter;
    }

    public List<MembershipLogEntry> log(List<MembershipLogEntry> entries) {
        List<MembershipLogEntry> counters = entries
                .stream()
                .map(entry -> new MembershipLogEntry(entry.nodeId(), this.addEntry(entry)))
                .collect(Collectors.toList());

        this.save();
        return counters;
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
