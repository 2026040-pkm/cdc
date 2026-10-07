package dev.hotdb.dbagent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * DB Agent (O-6) — Oracle 레거시를 주기적으로 읽어 레거시 DB 의 mes · lgs · geo 에 쓴다.
 *
 * <p>코드는 poll-core 가 들고 있고, 이 서비스는 작업 목록(application.yml 의 hotdb.poll.jobs)만 가진다.
 */
@SpringBootApplication
public class DbAgentApplication {
    public static void main(String[] args) {
        SpringApplication.run(DbAgentApplication.class, args);
    }
}
