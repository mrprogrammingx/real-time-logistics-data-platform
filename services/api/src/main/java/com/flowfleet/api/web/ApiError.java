package com.flowfleet.api.web;

import java.time.Instant;
import java.util.List;

/** Uniform error body for every 4xx/5xx response. */
public record ApiError(
        Instant timestamp,
        int status,
        String error,
        String message,
        List<String> details) {

    public static ApiError of(int status, String error, String message, List<String> details) {
        return new ApiError(Instant.now(), status, error, message, details == null ? List.of() : details);
    }
}
