package com.flowfleet.api.web;

/** Thrown when a lookup by id finds nothing. Mapped to HTTP 404. */
public class NotFoundException extends RuntimeException {

    public NotFoundException(String entity, Object id) {
        super("%s %s not found".formatted(entity, id));
    }
}
