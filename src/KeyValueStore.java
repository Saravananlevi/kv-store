import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KeyValueStore {
    private final Map<String, KeyValueEntry> store;
    private final File file;
    private static final long MAX_FILE_SIZE = 1024 * 1024 * 1024; // 1 GB
    private final Lock lock = new ReentrantLock();

    // Constructor to initialize the data store
    public KeyValueStore(String filePath) throws IOException {
        this.store = new ConcurrentHashMap<>();
        this.file = filePath == null ? new File("dataStore.json") : new File(filePath);

        if (this.file.exists()) {
            loadFromFile();
        }

        // Start background thread to check and clean up expired keys
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this::removeExpiredKeys, 0, 1, TimeUnit.MINUTES);
    }

    // Inner class to store the key, value, and TTL
    private static class KeyValueEntry {
        String value;
        Instant expiration;

        KeyValueEntry(String value, Instant expiration) {
            this.value = value;
            this.expiration = expiration;
        }

        boolean isExpired() {
            return expiration != null && Instant.now().isAfter(expiration);
        }
    }

    // Create operation
    public void create(String key, String value, Integer ttlInSeconds) throws Exception {
        if (key.length() > 32 || value.length() > 16 * 1024) {
            throw new Exception("Key or value exceeds allowed size.");
        }

        lock.lock();
        try {
            if (store.containsKey(key)) {
                throw new Exception("Key already exists.");
            }

            Instant expiration = ttlInSeconds == null ? null : Instant.now().plusSeconds(ttlInSeconds);
            store.put(key, new KeyValueEntry(value, expiration));

            // Save to file
            saveToFile();
        } finally {
            lock.unlock();
        }
    }

    // Read operation
    public String read(String key) throws Exception {
        lock.lock();
        try {
            KeyValueEntry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                throw new Exception("Key does not exist or has expired.");
            }
            return entry.value;
        } finally {
            lock.unlock();
        }
    }

    // Delete operation
    public void delete(String key) throws Exception {
        lock.lock();
        try {
            if (!store.containsKey(key) || store.get(key).isExpired()) {
                throw new Exception("Key does not exist or has expired.");
            }
            store.remove(key);
            saveToFile();
        } finally {
            lock.unlock();
        }
    }

    // Batch Create operation
    public void batchCreate(Map<String, String> entries, Integer ttlInSeconds) throws Exception {
        if (entries.size() > 100) { // Limiting batch size to 100
            throw new Exception("Batch size exceeds limit.");
        }
        lock.lock();
        try {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                create(entry.getKey(), entry.getValue(), ttlInSeconds);
            }
        } finally {
            lock.unlock();
        }
    }

    // Method to periodically remove expired keys
    private void removeExpiredKeys() {
        lock.lock();
        try {
            store.entrySet().removeIf(entry -> entry.getValue().isExpired());
            saveToFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    // Load data from file
    private void loadFromFile() throws IOException {
        // Read from file (skipping implementation for brevity)
    }

    // Save data to file
    private void saveToFile() throws IOException {
        // Write to file (skipping implementation for brevity)
    }

    public static void main(String[] args) throws Exception {
        KeyValueStore kvStore = new KeyValueStore(null);

        kvStore.create("key1", "{\"name\":\"John\"}", 60); // TTL of 60 seconds
        System.out.println(kvStore.read("key1"));

        kvStore.delete("key1");
        try {
            System.out.println(kvStore.read("key1"));
        } catch (Exception e) {
            System.out.println(e.getMessage()); // Expected: Key does not exist or has expired.
        }
    }
}
