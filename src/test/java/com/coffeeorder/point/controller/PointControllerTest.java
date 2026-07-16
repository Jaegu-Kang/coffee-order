package com.coffeeorder.point.controller;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.point.service.PointService;

@WebMvcTest(PointController.class)
class PointControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private PointService pointService;

	@Test
	void charge_정상_요청이면_200과_잔액을_응답한다() throws Exception {
		when(pointService.charge(1L, 10000L)).thenReturn(25000L);

		mockMvc.perform(post("/api/points/charge")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{ \"userId\": 1, \"amount\": 10000 }"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.userId").value(1))
				.andExpect(jsonPath("$.balance").value(25000));
	}

	@Test
	void charge_userId가_없으면_400_VALIDATION_ERROR를_응답한다() throws Exception {
		mockMvc.perform(post("/api/points/charge")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{ \"amount\": 10000 }"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
	}

	@Test
	void charge_amount가_없으면_400_VALIDATION_ERROR를_응답한다() throws Exception {
		mockMvc.perform(post("/api/points/charge")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{ \"userId\": 1 }"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
	}

	@Test
	void charge_amount가_0이하이면_400_VALIDATION_ERROR를_응답한다() throws Exception {
		mockMvc.perform(post("/api/points/charge")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{ \"userId\": 1, \"amount\": 0 }"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));

		mockMvc.perform(post("/api/points/charge")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{ \"userId\": 1, \"amount\": -1000 }"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
	}

	@Test
	void charge_존재하지_않는_사용자면_404_USER_NOT_FOUND를_응답한다() throws Exception {
		when(pointService.charge(anyLong(), anyLong()))
				.thenThrow(new BusinessException(ErrorCode.USER_NOT_FOUND));

		mockMvc.perform(post("/api/points/charge")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{ \"userId\": 999, \"amount\": 10000 }"))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.code").value("USER_NOT_FOUND"));
	}

	@Test
	void charge_서비스가_잘못된_금액으로_판단하면_400_INVALID_AMOUNT를_응답한다() throws Exception {
		when(pointService.charge(anyLong(), anyLong()))
				.thenThrow(new BusinessException(ErrorCode.INVALID_AMOUNT));

		mockMvc.perform(post("/api/points/charge")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{ \"userId\": 1, \"amount\": 1 }"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_AMOUNT"));
	}
}
