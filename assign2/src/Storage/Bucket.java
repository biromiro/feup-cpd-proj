package Storage;

import java.io.IOException;

public class Bucket {
    private static final String TOMBSTONE_EXT = ".DEAD";
    private final PersistentStorage storage;

    public Bucket(PersistentStorage storage) {
        this.storage = storage;
    }

    public void get(String key, PersistentStorage.ReadHandler handler) {
        storage.read(key, handler);
    }

    public void put(String key, String value, PersistentStorage.WriteHandler handler) {
        try {
            storage.deleteIfExists(tombstone(key));
        } catch (IOException ex) {
            throw new RuntimeException("Failed to delete tombstone for key: " + key);
        }
        storage.write(key, value, handler);
    }

    public void delete(String key){
        storage.write(tombstone(key), "", new PersistentStorage.WriteHandler() {
            @Override
            public void completed(Integer result) {
                try {
                    storage.deleteIfExists(key);
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

    private String tombstone(String key) {
        return key + TOMBSTONE_EXT;
    }
}
