package io.github.panabigdata.api.exception;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(BindException.class)
  public ResponseEntity<ApiErrorResponse> handleValidation(
      BindException exception, HttpServletRequest request) {
    Map<String, String> errors = new LinkedHashMap<>();
    for (FieldError fieldError : exception.getFieldErrors()) {
      errors.putIfAbsent(fieldError.getField(), fieldError.getDefaultMessage());
    }
    return error(HttpStatus.BAD_REQUEST, "Request validation failed", request, errors);
  }

  @ExceptionHandler(ConstraintViolationException.class)
  public ResponseEntity<ApiErrorResponse> handleConstraintViolation(
      ConstraintViolationException exception, HttpServletRequest request) {
    Map<String, String> errors = new LinkedHashMap<>();
    exception
        .getConstraintViolations()
        .forEach(
            violation -> {
              String path = violation.getPropertyPath().toString();
              String field = path.substring(path.lastIndexOf('.') + 1);
              errors.put(field, violation.getMessage());
            });
    return error(HttpStatus.BAD_REQUEST, "Request validation failed", request, errors);
  }

  @ExceptionHandler(MethodArgumentTypeMismatchException.class)
  public ResponseEntity<ApiErrorResponse> handleTypeMismatch(
      MethodArgumentTypeMismatchException exception, HttpServletRequest request) {
    return error(
        HttpStatus.BAD_REQUEST,
        "Invalid value for parameter '" + exception.getName() + "'",
        request);
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<ApiErrorResponse> handleInvalidArgument(HttpServletRequest request) {
    return error(HttpStatus.BAD_REQUEST, "Invalid request parameter", request);
  }

  @ExceptionHandler(MissingServletRequestParameterException.class)
  public ResponseEntity<ApiErrorResponse> handleMissingParameter(
      MissingServletRequestParameterException exception, HttpServletRequest request) {
    return error(
        HttpStatus.BAD_REQUEST,
        "Missing required parameter '" + exception.getParameterName() + "'",
        request);
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<ApiErrorResponse> handleMalformedBody(HttpServletRequest request) {
    return error(HttpStatus.BAD_REQUEST, "Request body is malformed", request);
  }

  @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
  public ResponseEntity<ApiErrorResponse> handleUnsupportedMethod(HttpServletRequest request) {
    return error(HttpStatus.METHOD_NOT_ALLOWED, "HTTP method is not supported", request);
  }

  @ExceptionHandler(NoResourceFoundException.class)
  public ResponseEntity<ApiErrorResponse> handleNotFound(HttpServletRequest request) {
    return error(HttpStatus.NOT_FOUND, "Resource not found", request);
  }

  @ExceptionHandler(DataAccessException.class)
  public ResponseEntity<ApiErrorResponse> handleDataAccess(
      DataAccessException exception, HttpServletRequest request) {
    log.error("Database error for {} {}", request.getMethod(), request.getRequestURI(), exception);
    return error(HttpStatus.SERVICE_UNAVAILABLE, "The service is temporarily unavailable", request);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ApiErrorResponse> handleUnexpected(
      Exception exception, HttpServletRequest request) {
    log.error(
        "Unexpected error for {} {}", request.getMethod(), request.getRequestURI(), exception);
    return error(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred", request);
  }

  private ResponseEntity<ApiErrorResponse> error(
      HttpStatus status, String message, HttpServletRequest request) {
    return error(status, message, request, Map.of());
  }

  private ResponseEntity<ApiErrorResponse> error(
      HttpStatus status,
      String message,
      HttpServletRequest request,
      Map<String, String> fieldErrors) {
    ApiErrorResponse body =
        new ApiErrorResponse(
            Instant.now(),
            status.value(),
            status.getReasonPhrase(),
            message,
            request.getRequestURI(),
            fieldErrors);
    return ResponseEntity.status(status).body(body);
  }
}
