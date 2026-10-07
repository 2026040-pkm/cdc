package dev.hotdb.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * 포트 경계 — cdc-core 는 엔진을 모른다. Debezium 은 cdc-debezium 모듈에만 있고, 그 모듈이 자동 설정으로 끼어든다.
 * Gradle 의존성으로도 막혀 있지만(컴파일이 안 된다) 어댑터 패키지를 core 에 되돌려 놓는 실수를 여기서 잡는다.
 */
class PortBoundaryTest {

    static final Path MAIN = Path.of("src/main/java/dev/hotdb/cdc");

    @Test
    void core_는_엔진을_import_하지_않는다() throws IOException {
        assertThat(filesContaining("import io.debezium.")).isEmpty();
        assertThat(filesContaining("import dev.hotdb.cdc.adapter.")).isEmpty();
    }

    @Test
    void core_에_어댑터_패키지가_없다() {
        assertThat(MAIN.resolve("adapter")).doesNotExist();
    }

    private static List<String> filesContaining(String text) throws IOException {
        try (Stream<Path> files = Files.walk(MAIN)) {
            return files.filter(f -> f.toString().endsWith(".java"))
                    .filter(f -> read(f).contains(text))
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
