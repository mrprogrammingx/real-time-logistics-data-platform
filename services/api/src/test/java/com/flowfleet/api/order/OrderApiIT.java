package com.flowfleet.api.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.api.AbstractPostgisIT;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

class OrderApiIT extends AbstractPostgisIT {

    @Autowired
    TestRestTemplate rest;

    @Test
    void createsOrderComputesTotalAndReadsItBack() {
        Map<String, Object> body = Map.of(
                "customerId", 1,
                "restaurantId", 1,
                "items", List.of(
                        Map.of("name", "Lahmajoun", "quantity", 3, "unitPrice", 1.50),
                        Map.of("name", "Ayran", "quantity", 2, "unitPrice", 0.80)));

        ResponseEntity<Map> created = rest.postForEntity(url("/api/v1/orders"), body, Map.class);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getBody()).isNotNull();
        Number id = (Number) created.getBody().get("id");
        assertThat(id).isNotNull();
        assertThat(new BigDecimal(created.getBody().get("totalAmount").toString()))
                .isEqualByComparingTo("6.10");
        assertThat(created.getBody().get("status")).isEqualTo("CREATED");

        ResponseEntity<Map> fetched = rest.getForEntity(url("/api/v1/orders/" + id), Map.class);
        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat((List<?>) fetched.getBody().get("items")).hasSize(2);
    }

    @Test
    void rejectsIllegalStatusTransitionWith409() {
        Number id = createOrder(2, 2);

        ResponseEntity<Map> resp = patchStatus(id, "DELIVERED");

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void walksAnOrderThroughItsHappyPath() {
        Number id = createOrder(3, 3);

        for (String next : List.of("CONFIRMED", "PREPARING", "READY_FOR_PICKUP", "PICKED_UP",
                "OUT_FOR_DELIVERY", "DELIVERED")) {
            ResponseEntity<Map> resp = patchStatus(id, next);
            assertThat(resp.getStatusCode()).as("transition to %s", next).isEqualTo(HttpStatus.OK);
            assertThat(resp.getBody().get("status")).isEqualTo(next);
        }
    }

    @Test
    void validationErrorsReturn400WithDetails() {
        ResponseEntity<Map> resp = rest.postForEntity(url("/api/v1/orders"),
                Map.of("customerId", 1, "restaurantId", 1, "items", List.of()), Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat((List<?>) resp.getBody().get("details")).isNotEmpty();
    }

    private Number createOrder(int customerId, int restaurantId) {
        Map<String, Object> body = Map.of(
                "customerId", customerId,
                "restaurantId", restaurantId,
                "items", List.of(Map.of("name", "Item", "quantity", 1, "unitPrice", 5.00)));
        ResponseEntity<Map> resp = rest.postForEntity(url("/api/v1/orders"), body, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return (Number) resp.getBody().get("id");
    }

    private ResponseEntity<Map> patchStatus(Number orderId, String status) {
        return rest.exchange(
                RequestEntity.patch(url("/api/v1/orders/" + orderId + "/status"))
                        .body(Map.of("status", status)),
                Map.class);
    }
}
