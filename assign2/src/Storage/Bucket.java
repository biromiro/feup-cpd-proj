package Storage;

import java.io.IOException;

public class Bucket {
    private static final String BUCKET_FOLDER = "bucket/";
    private static final String TOMBSTONE_EXT = ".DEAD";
    private final PersistentStorage storage;

    public Bucket(PersistentStorage storage) {
        this.storage = storage;
    }

    public void get(String key, PersistentStorage.ReadHandler handler) {
        storage.read(keyFile(key), handler);
    }

    public void put(String key, String value, PersistentStorage.WriteHandler handler) {
        try {
            storage.delete(tombstone(key));
        } catch (IOException ex) {
            throw new RuntimeException("Failed to delete tombstone for key: " + key);
        }

        storage.write(keyFile(key), value, handler);
    }

    public void delete(String key){
        storage.write(tombstone(key), "", new PersistentStorage.WriteHandler() {
            @Override
            public void completed(Integer result) {
                try {
                    storage.delete(keyFile(key));
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

    private String keyFile(String key) {
        return BUCKET_FOLDER + key;
    }

    private String tombstone(String key) {
        return keyFile(key + TOMBSTONE_EXT);
    }
}
