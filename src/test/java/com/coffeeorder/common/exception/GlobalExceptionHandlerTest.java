package com.coffeeorder.common.exception;

import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

/**
 * {@link GlobalExceptionHandler}의 BusinessException/검증 실패 매핑을 검증하는 웹 슬라이스 테스트.
 */
@WebMvcTest(controllers = GlobalExceptionHandlerTestController.class)
class GlobalExceptionHandlerTest {

	@Autowired
	private MockMvc mockMvc;

	@Test
	void BusinessException은_ErrorCode의_상태코드와_코드로_매핑된다() throws Exception {
		mockMvc.perform(MockMvcRequestBuilders.post("/test/exceptions/user-not-found"))
				.andExpect(MockMvcResultMatchers.status().isNotFound())
				.andExpect(MockMvcResultMatchers.jsonPath("$.code", is(ErrorCode.USER_NOT_FOUND.getCode())))
				.andExpect(MockMvcResultMatchers.jsonPath("$.message", is(ErrorCode.USER_NOT_FOUND.getDefaultMessage())));
	}

	@Test
	void BusinessException의_커스텀_메시지는_그대로_노출된다() throws Exception {
		mockMvc.perform(MockMvcRequestBuilders.post("/test/exceptions/insufficient-point"))
				.andExpect(MockMvcResultMatchers.status().isConflict())
				.andExpect(MockMvcResultMatchers.jsonPath("$.code", is(ErrorCode.INSUFFICIENT_POINT.getCode())))
				.andExpect(MockMvcResultMatchers.jsonPath("$.message", is("커스텀 메시지")));
	}

	@Test
	void 필드_검증_실패는_400과_VALIDATION_ERROR로_매핑된다() throws Exception {
		mockMvc.perform(MockMvcRequestBuilders.post("/test/exceptions/validate")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{}"))
				.andExpect(MockMvcResultMatchers.status().isBadRequest())
				.andExpect(MockMvcResultMatchers.jsonPath("$.code", is(ErrorCode.VALIDATION_ERROR.getCode())));
	}
}
