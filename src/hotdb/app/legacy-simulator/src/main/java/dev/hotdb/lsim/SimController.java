package dev.hotdb.lsim;

import java.util.Map;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 부하 조절 — 필드 발행기와 같은 모양.
 * <pre>
 *   GET  /sim                  현재 상태 · 표 수 · 누적 행
 *   POST /sim/speed?value=5    배속
 *   POST /sim/pause · /sim/resume
 *   POST /sim/tick             지금 바로 한 주기 (폴링 확인용)
 * </pre>
 * 현장 발행기의 부하 제어판(:59480/)이 다른 출처에서 부르므로 CORS 를 연다.
 */
@CrossOrigin
@RestController
@RequestMapping("/sim")
public class SimController {

    private final LegacySimulator sim;

    public SimController(LegacySimulator sim) {
        this.sim = sim;
    }

    @GetMapping
    public Map<String, Object> status() {
        return sim.status();
    }

    @PostMapping("/speed")
    public Map<String, Object> speed(@RequestParam double value) {
        sim.speed(value);
        return sim.status();
    }

    @PostMapping("/pause")
    public Map<String, Object> pause() {
        sim.pause();
        return sim.status();
    }

    @PostMapping("/resume")
    public Map<String, Object> resume() {
        sim.resume();
        return sim.status();
    }

    @PostMapping("/tick")
    public Map<String, Object> tick() {
        sim.tick();
        return sim.status();
    }
}
