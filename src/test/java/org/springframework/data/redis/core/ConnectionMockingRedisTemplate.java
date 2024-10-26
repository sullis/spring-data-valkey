/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.redis.core;

import org.mockito.Mockito;
import org.springframework.data.redis.connection.ValkeyConnection;
import org.springframework.data.redis.connection.ValkeyConnectionFactory;
import org.springframework.data.redis.serializer.ValkeySerializer;

/**
 * Test extension to {@link ValkeyTemplate} to use a Mockito mocked {@link ValkeyConnection}.
 *
 * @author Christoph Strobl
 */
public class ConnectionMockingValkeyTemplate<K, V> extends ValkeyTemplate<K, V> {

	private final ValkeyConnection connectionMock;

	private ConnectionMockingValkeyTemplate() {

		connectionMock = Mockito.mock(ValkeyConnection.class);

		ValkeyConnectionFactory connectionFactory = Mockito.mock(ValkeyConnectionFactory.class);
		Mockito.when(connectionFactory.getConnection()).thenReturn(connectionMock);

		setConnectionFactory(connectionFactory);
	}

	static <K, V> ConnectionMockingValkeyTemplate<K, V> template() {
		return builder().build();
	}

	static MockTemplateBuilder builder() {
		return new MockTemplateBuilder();
	}

	public ValkeyConnection verify() {
		return Mockito.verify(connectionMock);
	}

	public byte[] serializeKey(K key) {
		return ((ValkeySerializer<K>) getKeySerializer()).serialize(key);
	}

	public ValkeyConnection never() {
		return Mockito.verify(connectionMock, Mockito.never());
	}

	public ValkeyConnection doReturn(Object o) {
		return Mockito.doReturn(o).when(connectionMock);
	}

	public static class MockTemplateBuilder {

		private ConnectionMockingValkeyTemplate template = new ConnectionMockingValkeyTemplate();

		public <K, V> ConnectionMockingValkeyTemplate<K, V> build() {

			template.afterPropertiesSet();
			return template;
		}

	}

}
