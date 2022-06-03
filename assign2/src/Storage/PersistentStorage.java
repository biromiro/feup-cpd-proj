package Storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.*;

public class PersistentStorage {
    private final String baseFolder;
    private static final int READ_BUFFER_SIZE = 1024;
    private final ScheduledThreadPoolExecutor executor;
    private final String fileName;

    public PersistentStorage(String fileName, String baseFolder, ScheduledThreadPoolExecutor executor) {
        this.fileName = fileName;
        this.baseFolder = baseFolder;
        this.executor = executor;
        File file = new File(baseFolder, this.fileName);
        if (!file.exists()) {
            if (!file.mkdirs()) {
                throw new RuntimeException("Could not create directory " + fileName + ".");
            }
        } else if (!file.isDirectory()) {
            throw new RuntimeException(file + " is not a directory.");
        }
    }

    public AsynchronousFileChannel getFile(String fileName, OpenOption... options) throws IOException {
        return AsynchronousFileChannel.open(
                getPath(fileName),
                Arrays.stream(options).collect(Collectors.toSet()),
                this.executor
        );
    }

    public File getFileSync(String fileName) {
        return getPath(fileName).toFile();
    }

    public Path getPath(String fileName) {
        return Paths.get(baseFolder, this.fileName, fileName);
    }

    public List<String> list() {
        File folder = new File(baseFolder);
        String[] fileList = folder.list();
        return fileList == null ? List.of() : Arrays.stream(fileList).toList();
    }

    public interface WriteHandler {
        void completed(Integer result);
        void failed(Throwable exc);
    }

    public void write(String fileName, String content, WriteHandler handler) {
        ByteBuffer buffer = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
        AsynchronousFileChannel file;
        try {
            file = getFile(fileName, WRITE, CREATE, TRUNCATE_EXISTING);
        } catch (IOException ex) {
            handler.failed(ex);
            return;
        }
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
    }

    public interface ReadHandler {
        void completed(Integer len, String message);
        void failed(Throwable exc);
    }

    public void read(String fileName, ReadHandler handler) {
        ByteBuffer buffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
        AsynchronousFileChannel file;
        try {
            file = getFile(fileName, READ);
        } catch (IOException e) {
            handler.failed(e);
            return;
        }
        file.read(buffer, 0, null, new CompletionHandler<Integer, Void>() {
            private int pos = 0;
            private final StringBuilder sb = new StringBuilder();

            @Override
            public void completed(Integer result, Void attachment) {
                if (result == -1) {
                    handler.completed(result, sb.toString());
                } else {
                    pos += result;
                    sb.append(new String(buffer.array(), buffer.arrayOffset(), result));

                    buffer.clear();
                    file.read(buffer, pos , null, this);
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                handler.failed(exc);
            }
        });
    }

    public void deleteIfExists(String fileName) throws IOException {
        Files.deleteIfExists(getPath(fileName));
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
