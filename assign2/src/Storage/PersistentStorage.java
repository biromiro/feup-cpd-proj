package Storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;

public class PersistentStorage {
    private static final String BASE_FOLDER = "storage";
    private final String id;

    public PersistentStorage(String id) {
        this.id = id;
        File file = new File(BASE_FOLDER, this.id);
        if (!file.exists()) {
            if (!file.mkdirs()) {
                throw new RuntimeException("Could not create directory " + id + ".");
            }
        } else if (!file.isDirectory()) {
            throw new RuntimeException(file + " is not a directory.");
        }
    }

    public File getFile(String fileName) {
        return getPath(fileName).toFile();
    }

    public Path getPath(String fileName) {
        return Paths.get(BASE_FOLDER, this.id, fileName);
    }

    public void write(String fileName, String content) throws IOException {
        PrintWriter writer = new PrintWriter(getFile(fileName), StandardCharsets.UTF_8);
        writer.println(content);
        writer.close();
    }

    public String read(String fileName) throws FileNotFoundException {
        StringBuilder data = new StringBuilder();
        Scanner scanner = new Scanner(getFile(fileName));
        while (scanner.hasNextLine()) {
            data.append(scanner.nextLine());
        }
        scanner.close();

        return data.toString();
    }

    public void append(String filename, String content) throws IOException {
        Files.writeString(
                getPath(filename),
                content + System.lineSeparator(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND
        );
    }
}
