/*
 * Copyright 2016-2024 the original author or authors.
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
package org.springframework.data.valkey.repository.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.valkey.core.mapping.ValkeyPersistentEntity;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class MappingValkeyEntityInformationUnitTests<T, ID> {

	@Mock ValkeyPersistentEntity<T> entity;

	@Test // DATAREDIS-425
	void throwsMappingExceptionWhenNoIdPropertyPresent() {

		when(entity.hasIdProperty()).thenReturn(false);

		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> new MappingValkeyEntityInformation<T, ID>(entity));
	}
}
