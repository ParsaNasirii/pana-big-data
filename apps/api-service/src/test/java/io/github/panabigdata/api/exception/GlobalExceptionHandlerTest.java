package io.github.panabigdata.api.exception;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.panabigdata.api.dto.TimeRangeFilterDto;
import io.github.panabigdata.api.dto.TrafficFilterDto;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootTest(
    classes = GlobalExceptionHandlerTest.TestApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GlobalExceptionHandlerTest {

  @Autowired private TestRestTemplate restTemplate;

  @Autowired private ObjectMapper objectMapper;

  @Test
  void serializesApiDateTimesInTehranTime() throws Exception {
    String json = objectMapper.writeValueAsString(OffsetDateTime.parse("2023-10-01T08:30:00Z"));

    assertThat(json).isEqualTo("\"2023-10-01T12:00:00+03:30\"");
  }

  @Test
  void unexpectedExceptionsReturnSafeInternalServerError() throws Exception {
    ResponseEntity<String> response =
        restTemplate.getForEntity("/test/errors/unexpected", String.class);
    JsonNode body = body(response);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(body.path("status").asInt()).isEqualTo(500);
    assertThat(body.path("error").asText()).isEqualTo("Internal Server Error");
    assertThat(body.path("message").asText()).isEqualTo("An unexpected error occurred");
    assertThat(body.path("path").asText()).isEqualTo("/test/errors/unexpected");
    assertThat(body.has("timestamp")).isTrue();
    assertThat(body.has("trace")).isFalse();
    assertThat(body.has("exception")).isFalse();
    assertThat(response.getBody()).doesNotContain("super-secret");
  }

  @Test
  void dataAccessExceptionsReturnServiceUnavailableWithoutLeakingCause() throws Exception {
    ResponseEntity<String> response =
        restTemplate.getForEntity("/test/errors/database", String.class);
    JsonNode body = body(response);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    assertThat(body.path("status").asInt()).isEqualTo(503);
    assertThat(body.path("message").asText()).isEqualTo("The service is temporarily unavailable");
    assertThat(body.has("trace")).isFalse();
    assertThat(response.getBody()).doesNotContain("internal-host");
  }

  @Test
  void invalidParameterTypeReturnsBadRequest() throws Exception {
    ResponseEntity<String> response =
        restTemplate.getForEntity("/test/errors/limit?limit=not-a-number", String.class);
    JsonNode body = body(response);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(body.path("status").asInt()).isEqualTo(400);
    assertThat(body.path("message").asText()).isEqualTo("Invalid value for parameter 'limit'");
    assertThat(body.path("path").asText()).isEqualTo("/test/errors/limit");
    assertThat(body.has("trace")).isFalse();
  }

  @Test
  void constraintViolationReturnsValidationDetails() throws Exception {
    ResponseEntity<String> response =
        restTemplate.getForEntity("/test/errors/limit?limit=0", String.class);
    JsonNode body = body(response);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(body.path("message").asText()).isEqualTo("Request validation failed");
    assertThat(body.path("fieldErrors").path("limit").asText())
        .withFailMessage("response: %s", response.getBody())
        .isEqualTo("must be greater than or equal to 1");
    assertThat(body.has("trace")).isFalse();
  }

  @Test
  void malformedDateReturnsBadRequest() throws Exception {
    ResponseEntity<String> response =
        restTemplate.getForEntity("/test/errors/filter?start=not-a-date", String.class);
    JsonNode body = body(response);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(body.path("status").asInt()).isEqualTo(400);
    assertThat(body.path("message").asText()).isEqualTo("Request validation failed");
    assertThat(body.path("fieldErrors").has("start")).isTrue();
    assertThat(body.has("trace")).isFalse();
  }

  @Test
  void missingTimeRangeValueReturnsBadRequest() throws Exception {
    ResponseEntity<String> response = restTemplate.getForEntity("/test/errors/range", String.class);
    JsonNode body = body(response);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(body.path("message").asText()).isEqualTo("Request validation failed");
    assertThat(body.path("fieldErrors").path("start").asText()).isEqualTo("start is required");
    assertThat(body.path("fieldErrors").path("end").asText()).isEqualTo("end is required");
    assertThat(body.has("trace")).isFalse();
  }

  @Test
  void reversedTimeRangeReturnsBadRequest() throws Exception {
    ResponseEntity<String> response =
        restTemplate.getForEntity(
            "/test/errors/range?start=2026-09-23T10:00:00Z&end=2026-09-23T09:00:00Z", String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(body(response).path("fieldErrors").path("timeRangeValid").asText())
        .isEqualTo("end must be greater than or equal to start");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "999.1.1.1"})
  void invalidIpv4AddressReturnsBadRequest(String ip) throws Exception {
    ResponseEntity<String> response =
        restTemplate.getForEntity(
            "/test/errors/filter?ip={ip}&start=2026-09-23T08:30:00Z&end=2026-09-23T09:30:00Z",
            String.class,
            ip);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(body(response).path("fieldErrors").has("ip")).isTrue();
  }

  @Test
  void missingIpReturnsBadRequest() throws Exception {
    ResponseEntity<String> response =
        restTemplate.getForEntity(
            "/test/errors/filter?start=2026-09-23T08:30:00Z&end=2026-09-23T09:30:00Z",
            String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(body(response).path("fieldErrors").path("ip").asText()).isEqualTo("ip is required");
  }

  @Test
  void validFiltersWithEquivalentTimesAreAccepted() {
    ResponseEntity<String> response =
        restTemplate.getForEntity(
            "/test/errors/filter?ip=99.254.52.29&start={start}&end={end}",
            String.class,
            "2026-09-23T12:00:00+03:30",
            "2026-09-23T08:30:00Z");

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  void unsupportedMethodReturnsMethodNotAllowed() throws Exception {
    ResponseEntity<String> response =
        restTemplate.postForEntity("/test/errors/limit", null, String.class);
    JsonNode body = body(response);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
    assertThat(body.path("status").asInt()).isEqualTo(405);
    assertThat(body.path("message").asText()).isEqualTo("HTTP method is not supported");
    assertThat(body.has("trace")).isFalse();
  }

  private JsonNode body(ResponseEntity<String> response) throws Exception {
    return objectMapper.readTree(response.getBody());
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
  @Import({GlobalExceptionHandler.class, ErrorController.class})
  static class TestApplication {}

  @RestController
  @RequestMapping("/test/errors")
  @Validated
  static class ErrorController {

    @GetMapping("/limit")
    void limit(@RequestParam("limit") @Min(1) int limit) {}

    @GetMapping("/filter")
    void filter(@Valid TrafficFilterDto filter) {}

    @GetMapping("/range")
    void range(@Valid TimeRangeFilterDto filter) {}

    @GetMapping("/unexpected")
    void unexpected() {
      throw new IllegalStateException("database password=super-secret");
    }

    @GetMapping("/database")
    void database() {
      throw new DataAccessResourceFailureException("jdbc://internal-host");
    }
  }
}
