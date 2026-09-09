package com.flowfleet.api.driver;

import com.flowfleet.api.driver.dto.CreateDriverRequest;
import com.flowfleet.api.driver.dto.DriverResponse;
import com.flowfleet.api.driver.dto.UpdateDriverStatusRequest;
import com.flowfleet.common.domain.DriverStatus;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/drivers")
class DriverController {

    private final DriverService drivers;

    DriverController(DriverService drivers) {
        this.drivers = drivers;
    }

    @PostMapping
    ResponseEntity<DriverResponse> create(@Valid @RequestBody CreateDriverRequest request) {
        DriverResponse created = drivers.create(request);
        return ResponseEntity.created(URI.create("/api/v1/drivers/" + created.id())).body(created);
    }

    @GetMapping("/{id}")
    DriverResponse get(@PathVariable long id) {
        return drivers.get(id);
    }

    @GetMapping
    List<DriverResponse> list(@RequestParam(required = false) DriverStatus status) {
        return drivers.list(status);
    }

    @PatchMapping("/{id}/status")
    DriverResponse changeStatus(@PathVariable long id, @Valid @RequestBody UpdateDriverStatusRequest request) {
        return drivers.changeStatus(id, request.status());
    }
}
