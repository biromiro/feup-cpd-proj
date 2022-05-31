package Storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Scanner;

import static java.nio.file.StandardOpenOption.*;

public class PersistentStorage {
    private static final String BASE_FOLDER = "storage";
    private static final int READ_BUFFER_SIZE = 1024;
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

    public AsynchronousFileChannel getFile(String fileName, OpenOption... options) throws IOException {
        return AsynchronousFileChannel.open(getPath(fileName), options);
    }

    public File getFileSync(String fileName) {
        return getPath(fileName).toFile();
    }

    public Path getPath(String fileName) {
        return Paths.get(BASE_FOLDER, this.id, fileName);
    }

    public interface WriteHandler {
        void completed(Integer result);
        void failed(Throwable exc);
    }

    public void write(String fileName, String content, WriteHandler handler) {
        ByteBuffer buffer = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
        try (AsynchronousFileChannel file = getFile(fileName, WRITE, CREATE)) {
            file.write(buffer, 0, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer result, Void attachment) {
                    handler.completed(result);
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    handler.failed(exc);
                }
            });
        } catch (IOException e) {
            handler.failed(e);
        }
    }

    public interface ReadHandler {
        void completed(Integer len, String message);
        void failed(Throwable exc);
    }

    public void read(String fileName, ReadHandler handler) {
        ByteBuffer buffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
        try (AsynchronousFileChannel file = getFile(fileName, READ)) {
            file.read(buffer, 0, null, new CompletionHandler<Integer, Void>() {
                private int pos = 0;
                private final StringBuilder sb = new StringBuilder();

                @Override
                public void completed(Integer result, Void attachment) {
                    if (result == -1) {
                        handler.completed(result, sb.toString());
                    } else {
                        pos += result;
                        sb.append(new String(buffer.array(), buffer.arrayOffset(), buffer.array().length));

                        buffer.clear();
                        file.read(buffer, pos , null, this);
                    }
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    handler.failed(exc);
                }
            });
        } catch (IOException e) {
            handler.failed(e);
        }
    }

    public void writeSync(String fileName, String content) throws IOException {
        PrintWriter writer = new PrintWriter(getFileSync(fileName), StandardCharsets.UTF_8);
        writer.print(content);
        writer.close();
    }

    public String readSync(String fileName) throws FileNotFoundException {
        StringBuilder data = new StringBuilder();
        Scanner scanner = new Scanner(getFileSync(fileName));
        while (scanner.hasNextLine()) {
            data.append(scanner.nextLine());
        }
        scanner.close();

        return data.toString();
    }
}
