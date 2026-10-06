package dev.hotdb.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** 포트 경계 — Debezium 은 어댑터 안에만, 어댑터는 조립(자동 설정) 말고는 아무도 직접 부르지 않는다. */
class PortBoundaryTest {

    static final Path MAIN = Path.of("src/main/java/dev/hotdb/cdc");

    @Test
    void Debezium_은_어댑터_밖에서_보이지_않는다() throws IOException {
        assertThat(filesImporting("import io.debezium.", Path.of("adapter", "debezium"))).isEmpty();
    }

    @Test
    void 어댑터는_자동_설정만_안다() throws IOException {
        assertThat(filesImporting("import dev.hotdb.cdc.adapter.", Path.of("adapter")))
                .containsExactly("HotdbCdcAutoConfiguration.java");
    }

    private static List<String> filesImporting(String prefix, Path allowedDir) throws IOException {
        try (Stream<Path> files = Files.walk(MAIN)) {
            return files.filter(f -> f.toString().endsWith(".java"))
                    .filter(f -> !MAIN.relativize(f).startsWith(allowedDir))
                    .filter(f -> read(f).contains(prefix))
                    .map(f -> MAIN.relativize(f).toString())
                    .toList();
        }
    }

    private static String read(Path f) {
        try {
            return Files.readString(f);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
