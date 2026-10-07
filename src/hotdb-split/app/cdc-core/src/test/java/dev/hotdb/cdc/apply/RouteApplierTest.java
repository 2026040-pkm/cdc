package dev.hotdb.cdc.apply;

import static org.assertj.core.api.Assertions.assertThat;

import dev.hotdb.cdc.HotdbCdcProperties;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.BadSqlGrammarException;

/** 배치 나누기 · 실패 판정 · 정지 판단. DB 없이 돈다. */
class RouteApplierTest {

    record Row(String device, int seq) {}

    @Test
    void 같은_키는_한_묶음에_한_번만_온_순서대로() {
        List<Row> rows = List.of(new Row("A", 1), new Row("B", 1), new Row("A", 2), new Row("A", 3), new Row("C", 1),
                new Row("B", 2));

        List<List<Row>> waves = RouteApplier.splitByKey(rows, Row::device);

        assertThat(waves).containsExactly(
                List.of(new Row("A", 1), new Row("B", 1), new Row("C", 1)),
                List.of(new Row("A", 2), new Row("B", 2)),
                List.of(new Row("A", 3)));
    }

    @Test
    void 키가_안_겹치거나_upsert_가_아니면_통째로() {
        List<Row> distinct = List.of(new Row("A", 1), new Row("B", 1));
        assertThat(RouteApplier.splitByKey(distinct, Row::device)).containsExactly(distinct);

        List<Row> dup = List.of(new Row("A", 1), new Row("A", 2));
        assertThat(RouteApplier.splitByKey(dup, r -> null)).containsExactly(dup);
        assertThat(RouteApplier.splitByKey(List.<Row>of(), Row::device)).containsExactly(List.of());
    }

    @Test
    void 실패는_SQLSTATE_로_셋으로_가른다() {
        SQLException cardinality = new BatchUpdateException("cannot affect row a second time", "21000", 0, new int[0]);
        assertThat(FailureVerdict.of(new DataIntegrityViolationException("x", cardinality))).isEqualTo(FailureVerdict.DEAD_LETTER);
        assertThat(FailureVerdict.of(new DataIntegrityViolationException("x", new SQLException("fk", "23503"))))
                .isEqualTo(FailureVerdict.DEAD_LETTER);

        assertThat(FailureVerdict.of(new TransientDataAccessResourceException("x", new SQLException("conn", "08006"))))
                .isEqualTo(FailureVerdict.RETRY);
        assertThat(FailureVerdict.of(new TransientDataAccessResourceException("x", new SQLException("deadlock", "40P01"))))
                .isEqualTo(FailureVerdict.RETRY);
        assertThat(FailureVerdict.of(new RuntimeException("no sql state"))).isEqualTo(FailureVerdict.RETRY);

        // 기동 때 대조한 표 · 컬럼이 돌던 중에 사라졌다 — 다른 건도 다 같은 이유로 실패한다
        assertThat(FailureVerdict.of(new BadSqlGrammarException("x", "sql", new SQLException("no table", "42P01"))))
                .isEqualTo(FailureVerdict.HALT);
        assertThat(FailureVerdict.of(new SQLException("permission", "42501"))).isEqualTo(FailureVerdict.HALT);
    }

    @Test
    void 정지_판정은_건수와_비율을_같이_본다() {
        HotdbCdcProperties.Apply policy = new HotdbCdcProperties.Apply(3, 200, 0.5, 10);

        assertThat(RouteApplier.shouldHalt(1, 1, policy)).as("한 건짜리 배치의 독이 든 행").isFalse();
        assertThat(RouteApplier.shouldHalt(9, 10, policy)).as("최소 건수 미만").isFalse();
        assertThat(RouteApplier.shouldHalt(10, 100, policy)).as("비율 미만").isFalse();
        assertThat(RouteApplier.shouldHalt(51, 100, policy)).as("대상 표가 사라진 것처럼 대부분 실패").isTrue();
    }

    @Test
    void 정지는_엔진이_감싸도_알아본다() {
        PipelineHaltedException halted = new PipelineHaltedException("DLQ_RATIO", "멈춤", null);
        assertThat(PipelineHaltedException.find(new RuntimeException("engine", new IllegalStateException(halted))))
                .isSameAs(halted);
        assertThat(PipelineHaltedException.find(new RuntimeException("other"))).isNull();
    }
}
