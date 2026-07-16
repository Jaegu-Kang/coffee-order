package com.coffeeorder.menu.controller;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.coffeeorder.menu.dto.MenuResponse;
import com.coffeeorder.menu.service.MenuService;

@WebMvcTest(MenuController.class)
class MenuControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private MenuService menuService;

	@Test
	void getMenus_메뉴가_있으면_menus_배열로_응답한다() throws Exception {
		when(menuService.getMenus()).thenReturn(List.of(
				new MenuResponse(1L, "아메리카노", 3000L),
				new MenuResponse(2L, "카페라떼", 3500L)));

		mockMvc.perform(get("/api/menus"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.menus.length()").value(2))
				.andExpect(jsonPath("$.menus[0].id").value(1))
				.andExpect(jsonPath("$.menus[0].name").value("아메리카노"))
				.andExpect(jsonPath("$.menus[0].price").value(3000))
				.andExpect(jsonPath("$.menus[1].id").value(2))
				.andExpect(jsonPath("$.menus[1].name").value("카페라떼"))
				.andExpect(jsonPath("$.menus[1].price").value(3500));
	}

	@Test
	void getMenus_메뉴가_없으면_빈_배열로_응답한다() throws Exception {
		when(menuService.getMenus()).thenReturn(Collections.emptyList());

		mockMvc.perform(get("/api/menus"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.menus").isArray())
				.andExpect(jsonPath("$.menus.length()").value(0));
	}
}
