package Storage;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class Bucket {
    private static final String baseFolder = "bucket/";
    private static final String TOMBSTONE_EXT = ".dead";
    private final PersistentStorage storage;

    public Bucket(PersistentStorage storage) {
        this.storage = storage;
        File file = this.storage.getFileSync(baseFolder);
        if (!file.exists()) {
            if (!file.mkdirs()) {
                throw new RuntimeException("Could not create directory " + baseFolder + ".");
            }
        } else if (!file.isDirectory()) {
            throw new RuntimeException(file + " is not a directory.");
        }
    }

    public void get(String key, PersistentStorage.ReadHandler handler) {
        storage.read(keyFile(key), handler);
    }

    public void put(String key, String value, PersistentStorage.WriteHandler handler) {
        try {
            storage.deleteIfExists(tombstoneFile(key));
        } catch (IOException ex) {
            throw new RuntimeException("Failed to delete tombstone for key: " + key);
        }
        storage.write(keyFile(key), value, handler);
    }

    public void delete(String key) {
        storage.write(tombstoneFile(key), "", new PersistentStorage.WriteHandler() {
            @Override
            public void completed(Integer result) {
                try {
                    storage.deleteIfExists(keyFile(key));
                } catch (IOException ex) {
                    throw new RuntimeException("Failed to delete pair with key: " + key, ex);
                }
            }
            @Override
            public void failed(Throwable exc) {
                throw new RuntimeException("Failed to write tombstone for key: " + key, exc);
            }
        });
    }

    public List<String> getMarkedKeys() {
        return storage.listFiles(baseFolder);
    }

    private String tombstoneFile(String key) {
        return keyFile(key) + TOMBSTONE_EXT;
    }

    private String keyFile(String key) {
        return baseFolder + key;
    }
}
