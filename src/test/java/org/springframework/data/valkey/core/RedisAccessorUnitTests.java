/*
 * Copyright 2017-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.springframework.data.valkey.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.jupiter.api.Test;

import org.springframework.data.valkey.connection.ValkeyConnectionFactory;

/**
 * Unit tests for {@link ValkeyAccessor}.
 *
 * @author John Blum
 */
class ValkeyAccessorUnitTests {

	@Test
	void setAndGetConnectionFactory() {

		ValkeyConnectionFactory mockConnectionFactory = mock(ValkeyConnectionFactory.class);

		ValkeyAccessor redisAccessor = new TestValkeyAccessor();

		assertThat(redisAccessor.getConnectionFactory()).isNull();

		redisAccessor.setConnectionFactory(mockConnectionFactory);

		assertThat(redisAccessor.getConnectionFactory()).isSameAs(mockConnectionFactory);
		assertThat(redisAccessor.getRequiredConnectionFactory()).isSameAs(mockConnectionFactory);

		redisAccessor.setConnectionFactory(null);

		assertThat(redisAccessor.getConnectionFactory()).isNull();

		verifyNoInteractions(mockConnectionFactory);
	}

	@Test
	void getRequiredConnectionFactoryWhenNull() {

		assertThatIllegalStateException()
			.isThrownBy(() -> new TestValkeyAccessor().getRequiredConnectionFactory())
			.withMessage("ValkeyConnectionFactory is required")
			.withNoCause();
	}

	@Test
	void afterPropertiesSetCallsGetRequiredConnectionFactory() {

		ValkeyConnectionFactory mockConnectionFactory = mock(ValkeyConnectionFactory.class);

		ValkeyAccessor redisAccessor = spy(new TestValkeyAccessor());

		doReturn(mockConnectionFactory).when(redisAccessor).getRequiredConnectionFactory();

		redisAccessor.afterPropertiesSet();

		verify(redisAccessor, times(1)).afterPropertiesSet();
		verify(redisAccessor, times(1)).getRequiredConnectionFactory();
		verifyNoMoreInteractions(redisAccessor);
	}

	static class TestValkeyAccessor extends ValkeyAccessor { }

}
