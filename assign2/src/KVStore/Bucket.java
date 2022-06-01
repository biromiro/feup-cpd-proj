package KVStore;

import Storage.PersistentStorage;

public class Bucket {
    private static final String BUCKET_FOLDER = "bucket/";
    private static final String TOMBSTONE_EXT = ".DEAD";
    private final PersistentStorage storage;

    public Bucket(PersistentStorage storage) {
        this.storage = storage;
    }

    public void get(String key, PersistentStorage.ReadHandler handler) {
        storage.read(key, handler);
    }

    public void put(String key, String value, PersistentStorage.WriteHandler handler) {
        storage.delete(tombstone(key), new PersistentStorage.WriteHandler() {
            @Override
            public void completed(Integer result) {
                storage.write(key, value, handler);
            }

            @Override
            public void failed(Throwable exc) {
                throw new RuntimeException("Failed to delete tombstone for key: " + key, exc);
            }
        });
    }

    public void delete(String key){
        storage.write(tombstone(key), "", new PersistentStorage.WriteHandler() {
            @Override
            public void completed(Integer result) {
                storage.delete(key, new PersistentStorage.WriteHandler() {
                    @Override
                    public void completed(Integer result) {

                    }

                    @Override
                    public void failed(Throwable exc) {
                        throw new RuntimeException("Failed to delete pair with key: " + key, exc);
                    }
                });
            }
            @Override
            public void failed(Throwable exc) {
                throw new RuntimeException("Failed to write tombstone for key: " + key, exc);
            }
        });
    }

    private String tombstone(String key) {
        return BUCKET_FOLDER + key + TOMBSTONE_EXT;
    }
}
