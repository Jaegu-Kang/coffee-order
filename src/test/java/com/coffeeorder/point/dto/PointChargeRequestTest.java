package com.coffeeorder.point.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.jupiter.api.Test;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;

class PointChargeRequestTest {

	private final Validator validator;

	PointChargeRequestTest() {
		ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		this.validator = factory.getValidator();
	}

	@Test
	void 정상_값이면_검증_오류가_없다() {
		PointChargeRequest request = new PointChargeRequest(1L, 10000L);

		Set<ConstraintViolation<PointChargeRequest>> violations = validator.validate(request);

		assertThat(violations).isEmpty();
	}

	@Test
	void userId가_null이면_검증_오류가_발생한다() {
		PointChargeRequest request = new PointChargeRequest(null, 10000L);

		Set<ConstraintViolation<PointChargeRequest>> violations = validator.validate(request);

		assertThat(violations)
			.extracting(violation -> violation.getPropertyPath().toString())
			.contains("userId");
	}

	@Test
	void amount가_null이면_검증_오류가_발생한다() {
		PointChargeRequest request = new PointChargeRequest(1L, null);

		Set<ConstraintViolation<PointChargeRequest>> violations = validator.validate(request);

		assertThat(violations)
			.extracting(violation -> violation.getPropertyPath().toString())
			.contains("amount");
	}

	@Test
	void amount가_0이면_검증_오류가_발생한다() {
		PointChargeRequest request = new PointChargeRequest(1L, 0L);

		Set<ConstraintViolation<PointChargeRequest>> violations = validator.validate(request);

		assertThat(violations)
			.extracting(violation -> violation.getPropertyPath().toString())
			.contains("amount");
	}

	@Test
	void amount가_음수이면_검증_오류가_발생한다() {
		PointChargeRequest request = new PointChargeRequest(1L, -1L);

		Set<ConstraintViolation<PointChargeRequest>> violations = validator.validate(request);

		assertThat(violations)
			.extracting(violation -> violation.getPropertyPath().toString())
			.contains("amount");
	}
}
