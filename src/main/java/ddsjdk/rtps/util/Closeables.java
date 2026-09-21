package ddsjdk.rtps.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class Closeables {
    private Closeables() { }

    @FunctionalInterface
    public interface Opener {
        Closeable open() throws IOException;
    }

    public static List<Closeable> openAll(Opener... openers) throws IOException {
        List<Closeable> opened = new ArrayList<>();
        try {
            for (Opener opener : openers) opened.add(opener.open());
            return List.copyOf(opened);
        } catch (IOException | RuntimeException failure) {
            rollback(failure, opened);
            throw failure;
        }
    }

    public static void rollback(Throwable failure, List<? extends Closeable> opened) {
        closeAndCapture(failure, opened.reversed());
    }

    public static void closeAll(Iterable<? extends Closeable> resources) throws IOException {
        Throwable failure = closeAndCapture(null, resources);
        if (failure instanceof IOException e) throw e;
        if (failure instanceof RuntimeException e) throw e;
    }

    private static Throwable closeAndCapture(Throwable failure, Iterable<? extends Closeable> resources) {
        for (Closeable resource : resources) {
            try {
                resource.close();
            } catch (IOException | RuntimeException e) {
                if (failure == null) failure = e;
                else if (failure != e) failure.addSuppressed(e);
            }
        }
        return failure;
    }
}
