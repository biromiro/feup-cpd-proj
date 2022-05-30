package KVStore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class KVEntry implements Comparable<KVEntry> {
    private final String key;
    private final String value;

    KVEntry(String key, String value) {
        this.key = hash(key);
        this.value = value;
    }

    public static String hash(String item) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] hashBytes = digest.digest(
                item.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(hashBytes);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    @Override
    public String toString() {
        return "( " +  key + " : \"" + value + "\" )";
    }

    public int compareTo(KVEntry entry) {
        return this.key.compareTo(entry.getKey());
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
