package com.coffeeorder.point.controller;

import jakarta.validation.Valid;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.coffeeorder.point.dto.PointChargeRequest;
import com.coffeeorder.point.dto.PointChargeResponse;
import com.coffeeorder.point.service.PointService;

/**
 * 포인트 충전 API. {@code docs/api/point.md}의 {@code POST /api/points/charge}를 제공한다.
 */
@RestController
@RequestMapping("/api/points")
public class PointController {

	private final PointService pointService;

	public PointController(PointService pointService) {
		this.pointService = pointService;
	}

	@PostMapping("/charge")
	public PointChargeResponse charge(@Valid @RequestBody PointChargeRequest request) {
		Long balance = pointService.charge(request.getUserId(), request.getAmount());
		return PointChargeResponse.from(request.getUserId(), balance);
	}
}
