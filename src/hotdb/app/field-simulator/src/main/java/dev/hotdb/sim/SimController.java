package dev.hotdb.sim;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 부하 조절.
 * <pre>
 *   GET  /sim                  현재 상태 · 예상 행/초
 *   POST /sim/speed?value=5    배속 (1 = 카탈로그 기준 ≈ 426 행/초)
 *   POST /sim/pause · /sim/resume
 * </pre>
 */
@RestController
@RequestMapping("/sim")
public class SimController {

    private final FieldSimulator sim;

    public SimController(FieldSimulator sim) {
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
}
