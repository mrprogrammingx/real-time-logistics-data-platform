package com.flowfleet.api.web;

import java.util.List;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
class GlobalExceptionHandler {

    @ExceptionHandler(NotFoundException.class)
    ResponseEntity<ApiError> handleNotFound(NotFoundException ex) {
        return status(HttpStatus.NOT_FOUND, ex.getMessage(), List.of());
    }

    @ExceptionHandler(IllegalStateTransitionException.class)
    ResponseEntity<ApiError> handleTransition(IllegalStateTransitionException ex) {
        return status(HttpStatus.CONFLICT, ex.getMessage(), List.of());
    }

    @ExceptionHandler(OptimisticLockingFailureException.class)
    ResponseEntity<ApiError> handleOptimisticLock(OptimisticLockingFailureException ex) {
        return status(HttpStatus.CONFLICT, "resource was modified concurrently, retry", List.of());
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    ResponseEntity<ApiError> handleIntegrity(DataIntegrityViolationException ex) {
        // e.g. referencing a customer_id / restaurant_id / vehicle_id that does not exist
        return status(HttpStatus.BAD_REQUEST, "request violates a database constraint", List.of(rootMessage(ex)));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ApiError> handleValidation(MethodArgumentNotValidException ex) {
        List<String> details = ex.getBindingResult().getFieldErrors().stream()
                .map(f -> "%s %s".formatted(f.getField(), f.getDefaultMessage()))
                .toList();
        return status(HttpStatus.BAD_REQUEST, "validation failed", details);
    }

    @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    ResponseEntity<ApiError> handleDomain(RuntimeException ex) {
        return status(HttpStatus.BAD_REQUEST, ex.getMessage(), List.of());
    }

    private static ResponseEntity<ApiError> status(HttpStatus s, String message, List<String> details) {
        return ResponseEntity.status(s).body(ApiError.of(s.value(), s.getReasonPhrase(), message, details));
    }

    private static String rootMessage(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}
