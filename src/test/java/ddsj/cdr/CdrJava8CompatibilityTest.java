package ddsj.cdr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class CdrJava8CompatibilityTest {
    @TempDir Path output;

    @Test void sharedCodecCompilesWithoutRecordOrModernJdkApis() throws Exception {
        var compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "Run tests with a JDK");
        List<String> arguments = new ArrayList<>(List.of("--release", "8", "-d", output.toString()));
        try (var paths = Files.list(Path.of("src/main/java/ddsj/cdr"))) {
            paths.filter(path -> path.toString().endsWith(".java")).sorted()
                    .forEach(path -> arguments.add(path.toString()));
        }
        arguments.add("src/main/java/ddsj/rtps/runtime/PayloadSerializer.java");
        var diagnostics = new ByteArrayOutputStream();
        int result = compiler.run(null, diagnostics, diagnostics, arguments.toArray(String[]::new));
        assertEquals(0, result, diagnostics.toString());
    }
}
