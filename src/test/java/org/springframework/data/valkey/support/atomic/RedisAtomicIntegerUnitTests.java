/*
 * Copyright 2018-2024 the original author or authors.
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
 * limitations under the License.
 */
package org.springframework.data.valkey.support.atomic;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.valkey.core.ValkeyOperations;
import org.springframework.data.valkey.core.ValueOperations;
import org.springframework.data.valkey.serializer.ValkeySerializer;

/**
 * Unit tests for {@link ValkeyAtomicInteger}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class ValkeyAtomicIntegerUnitTests {

	@Mock ValkeyOperations<String, Integer> operationsMock;
	@Mock ValueOperations<String, Integer> valueOperationsMock;

	@Test // DATAREDIS-872
	@SuppressWarnings("unchecked")
	void shouldUseSetIfAbsentForInitialValue() {

		when(operationsMock.opsForValue()).thenReturn(valueOperationsMock);
		when(operationsMock.getKeySerializer()).thenReturn(mock(ValkeySerializer.class));
		when(operationsMock.getValueSerializer()).thenReturn(mock(ValkeySerializer.class));

		new ValkeyAtomicInteger("id", operationsMock);

		verify(valueOperationsMock).setIfAbsent("id", 0);
	}
}
