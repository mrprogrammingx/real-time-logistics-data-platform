package com.flowfleet.common.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OrderStatusTest {

    @Test
    void happyPathTransitionsAreAllowed() {
        assertThat(OrderStatus.CREATED.canTransitionTo(OrderStatus.CONFIRMED)).isTrue();
        assertThat(OrderStatus.CONFIRMED.canTransitionTo(OrderStatus.PREPARING)).isTrue();
        assertThat(OrderStatus.OUT_FOR_DELIVERY.canTransitionTo(OrderStatus.DELIVERED)).isTrue();
    }

    @Test
    void cancellationIsAllowedFromAnyNonTerminalState() {
        for (OrderStatus s : OrderStatus.values()) {
            if (!s.isTerminal()) {
                assertThat(s.canTransitionTo(OrderStatus.CANCELLED))
                        .as("cancel from %s", s)
                        .isTrue();
            }
        }
    }

    @Test
    void terminalStatesHaveNoTransitions() {
        assertThat(OrderStatus.DELIVERED.canTransitionTo(OrderStatus.CANCELLED)).isFalse();
        assertThat(OrderStatus.CANCELLED.canTransitionTo(OrderStatus.CREATED)).isFalse();
        assertThat(OrderStatus.DELIVERED.isTerminal()).isTrue();
    }

    @Test
    void skippingStatesIsRejected() {
        assertThat(OrderStatus.CREATED.canTransitionTo(OrderStatus.DELIVERED)).isFalse();
        assertThat(OrderStatus.CONFIRMED.canTransitionTo(OrderStatus.CREATED)).isFalse();
    }
}
