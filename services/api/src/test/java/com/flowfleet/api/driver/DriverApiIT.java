package com.flowfleet.api.driver;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.api.AbstractPostgisIT;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

class DriverApiIT extends AbstractPostgisIT {

    @Autowired
    TestRestTemplate rest;

    @Test
    void createsDriverOfflineThenGoesAvailable() {
        ResponseEntity<Map> created = rest.postForEntity(url("/api/v1/drivers"),
                Map.of("name", "Test Driver", "vehicleId", 1), Map.class);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        Number id = (Number) created.getBody().get("id");
        assertThat(created.getBody().get("status")).isEqualTo("OFFLINE");
        assertThat(created.getBody().get("version")).isNotNull();

        ResponseEntity<Map> updated = rest.exchange(
                RequestEntity.patch(url("/api/v1/drivers/" + id + "/status"))
                        .body(Map.of("status", "AVAILABLE")),
                Map.class);
        assertThat(updated.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(updated.getBody().get("status")).isEqualTo("AVAILABLE");
        assertThat(((Number) updated.getBody().get("version")).longValue())
                .isGreaterThan(((Number) created.getBody().get("version")).longValue());
    }

    @Test
    void unknownDriverReturns404() {
        ResponseEntity<Map> resp = rest.getForEntity(url("/api/v1/drivers/999999"), Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void listFiltersByStatus() {
        rest.postForEntity(url("/api/v1/drivers"), Map.of("name", "Filter A"), Map.class);
        Number bId = (Number) rest.postForEntity(url("/api/v1/drivers"),
                Map.of("name", "Filter B"), Map.class).getBody().get("id");
        rest.exchange(RequestEntity.patch(url("/api/v1/drivers/" + bId + "/status"))
                .body(Map.of("status", "ON_BREAK")), Map.class);

        ResponseEntity<List> onBreak = rest.getForEntity(url("/api/v1/drivers?status=ON_BREAK"), List.class);
        assertThat(onBreak.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(onBreak.getBody()).allSatisfy(d ->
                assertThat(((Map<?, ?>) d).get("status")).isEqualTo("ON_BREAK"));
    }
}
