package com.flowfleet.generator.web;

import com.flowfleet.generator.GeneratorRunner;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Load-test control plane: read / set the generator's target throughput at runtime, so a
 * ramp ({@code scripts/loadtest.sh}) can step 1k → 50k ev/s without restarting the pod
 * (which would reset the fleet and the Kafka producer).
 *
 * <pre>
 *   curl localhost:18090/api/load/rate
 *   curl -XPOST 'localhost:18090/api/load/rate?value=25000'
 * </pre>
 */
@RestController
@RequestMapping("/api/load")
public class LoadControlController {

    private final GeneratorRunner runner;

    public LoadControlController(GeneratorRunner runner) {
        this.runner = runner;
    }

    @GetMapping("/rate")
    public Map<String, Object> rate() {
        return Map.of(
                "targetRatePerSec", runner.targetRate(),
                "sentTotal", runner.sentTotal());
    }

    @PostMapping("/rate")
    public Map<String, Object> setRate(@RequestParam double value) {
        runner.setTargetRate(value);
        return rate();
    }
}
