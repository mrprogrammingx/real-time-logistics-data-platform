package com.flowfleet.api.web;

/** Thrown when a caller asks for a status change the domain state machine forbids. Mapped to HTTP 409. */
public class IllegalStateTransitionException extends RuntimeException {

    public IllegalStateTransitionException(Enum<?> from, Enum<?> to) {
        super("illegal transition %s -> %s".formatted(from, to));
    }
}
