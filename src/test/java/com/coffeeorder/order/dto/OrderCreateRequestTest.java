package com.coffeeorder.order.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;

class OrderCreateRequestTest {

	private static ValidatorFactory validatorFactory;
	private static Validator validator;

	@BeforeAll
	static void setUpValidator() {
		validatorFactory = Validation.buildDefaultValidatorFactory();
		validator = validatorFactory.getValidator();
	}

	@AfterAll
	static void closeValidatorFactory() {
		validatorFactory.close();
	}

	@Test
	void 정상_값이면_violation이_없다() {
		OrderCreateRequest request = new OrderCreateRequest(1L, 2L, 1);

		Set<ConstraintViolation<OrderCreateRequest>> violations = validator.validate(request);

		assertThat(violations).isEmpty();
	}

	@Test
	void userId가_null이면_violation이_발생한다() {
		OrderCreateRequest request = new OrderCreateRequest(null, 2L, 1);

		Set<ConstraintViolation<OrderCreateRequest>> violations = validator.validate(request);

		assertThat(violations)
			.extracting(ConstraintViolation::getPropertyPath)
			.extracting(Object::toString)
			.containsExactly("userId");
	}

	@Test
	void menuId가_null이면_violation이_발생한다() {
		OrderCreateRequest request = new OrderCreateRequest(1L, null, 1);

		Set<ConstraintViolation<OrderCreateRequest>> violations = validator.validate(request);

		assertThat(violations)
			.extracting(ConstraintViolation::getPropertyPath)
			.extracting(Object::toString)
			.containsExactly("menuId");
	}

	@Test
	void quantity가_0이면_violation이_발생한다() {
		OrderCreateRequest request = new OrderCreateRequest(1L, 2L, 0);

		Set<ConstraintViolation<OrderCreateRequest>> violations = validator.validate(request);

		assertThat(violations)
			.extracting(ConstraintViolation::getPropertyPath)
			.extracting(Object::toString)
			.containsExactly("quantity");
	}

	@Test
	void quantity가_음수이면_violation이_발생한다() {
		OrderCreateRequest request = new OrderCreateRequest(1L, 2L, -1);

		Set<ConstraintViolation<OrderCreateRequest>> violations = validator.validate(request);

		assertThat(violations)
			.extracting(ConstraintViolation::getPropertyPath)
			.extracting(Object::toString)
			.containsExactly("quantity");
	}
}
