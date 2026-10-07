package com.github.lechandonga.eventtimewindowaggregatorjava.web;

import com.github.lechandonga.eventtimewindowaggregatorjava.model.DeadLetter;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;
import com.github.lechandonga.eventtimewindowaggregatorjava.proc.ProcessingService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

// HTTP surface:
//   POST /events        ingest one event (202 accepted / 503 backpressure)
//   GET  /results       closed-window results only (optional windowStart/key filters)
//   GET  /dead-letters  late/malformed/sink-failed records with reasons
//   GET  /metrics       counters incl. queue depth and queue-full rejections
//   GET  /health        readiness of the processing pipeline
@RestController
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class EventController {

    private final ProcessingService processing;

    public EventController(ProcessingService processing) {
        this.processing = processing;
    }

    @PostMapping(path = "/events", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody String rawBody) {
        ProcessingService.IngestResult result = processing.ingest(rawBody);
        return ResponseEntity.status(result.httpStatus()).body(Map.of(
                "accepted", result.accepted(),
                "code", result.code(),
                "offset", result.offset(),
                "message", result.message()));
    }

    @GetMapping("/results")
    public Map<String, Object> results(@RequestParam(required = false) Long windowStart,
                                       @RequestParam(required = false) String key) {
        List<WindowResult> results = processing.aggregator().visibleResults(windowStart, key);
        return Map.of("count", results.size(),
                "watermark", processing.aggregator().watermark(),
                "results", results);
    }

    @GetMapping("/dead-letters")
    public Map<String, Object> deadLetters(@RequestParam(required = false) String reason) {
        List<DeadLetter> all = processing.deadLetters().all();
        List<DeadLetter> filtered = reason == null
                ? all
                : all.stream().filter(d -> reason.equals(d.reason())).toList();
        return Map.of("count", filtered.size(), "deadLetters", filtered);
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        var m = processing.metrics();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ingested", m.ingested.get());
        body.put("consumed", m.consumed.get());
        body.put("duplicate", m.duplicate.get());
        body.put("late", m.late.get());
        body.put("malformed", m.malformed.get());
        body.put("sinkRetried", m.sinkRetried.get());
        body.put("sinkDeadLettered", m.sinkDeadLettered.get());
        body.put("windowsClosed", m.windowsClosed.get());
        body.put("rejectedQueueFull", m.rejectedQueueFull.get());
        body.put("checkpoints", m.checkpoints.get());
        body.put("queueSize", processing.queueSize());
        body.put("queueCapacity", processing.queueCapacity());
        body.put("watermark", processing.aggregator().watermark());
        body.put("lastConsumedOffset", processing.lastConsumedOffset());
        return body;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", processing.isRunning() ? "UP" : "STARTING",
                "lastConsumedOffset", processing.lastConsumedOffset(),
                "watermark", processing.aggregator() == null
                        ? Long.MIN_VALUE : processing.aggregator().watermark());
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, Object>> onError(RuntimeException error) {
        return ResponseEntity.internalServerError().body(Map.of(
                "accepted", false,
                "code", "INGEST_FAILED",
                "message", String.valueOf(error.getMessage())));
    }
}
