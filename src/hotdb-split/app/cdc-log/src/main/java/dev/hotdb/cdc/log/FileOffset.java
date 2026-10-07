package dev.hotdb.cdc.log;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.OptionalLong;

/**
 * 전달부 오프셋 — 반영을 끝낸 마지막 순번 하나. 임시 파일에 쓰고 fsync 한 뒤 바꿔 끼운다(반쯤 쓴 파일이 남지 않게).
 *
 * <p>반영 커밋 뒤 · 여기 쓰기 전에 죽으면 같은 배치를 다시 받는다 — 싱크가 멱등이라 결과가 같다(at-least-once).
 * 반영과 같은 DB 트랜잭션에 오프셋을 넣으면 정확히 한 번이 되지만, 그러려면 Hot DB 에 표가 하나 더 든다(분리판 비교 범위 밖).
 */
final class FileOffset {

    private final Path file;

    FileOffset(Path file) {
        this.file = file;
    }

    OptionalLong read() {
        try {
            return Files.exists(file) ? OptionalLong.of(Long.parseLong(Files.readString(file).trim())) : OptionalLong.empty();
        } catch (IOException e) {
            throw new IllegalStateException("오프셋 파일을 읽지 못했다: " + file, e);
        }
    }

    void write(long seq) {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                ch.write(StandardCharsets.US_ASCII.encode(Long.toString(seq)));
                ch.force(true);
            }
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("오프셋 파일을 쓰지 못했다: " + file, e);
        }
    }
}
