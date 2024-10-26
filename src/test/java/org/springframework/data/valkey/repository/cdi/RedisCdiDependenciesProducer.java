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

package org.springframework.data.valkey.repository.cdi;

import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.valkey.connection.ValkeyConnectionFactory;
import org.springframework.data.valkey.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.valkey.core.ValkeyKeyValueAdapter;
import org.springframework.data.valkey.core.ValkeyKeyValueTemplate;
import org.springframework.data.valkey.core.ValkeyOperations;
import org.springframework.data.valkey.core.ValkeyTemplate;
import org.springframework.data.valkey.core.mapping.ValkeyMappingContext;
import org.springframework.data.valkey.test.extension.ValkeyStanalone;

/**
 * @author Mark Paluch
 */
public class ValkeyCdiDependenciesProducer {

	/**
	 * Provides a producer method for {@link ValkeyConnectionFactory}.
	 */
	@Produces
	public ValkeyConnectionFactory redisConnectionFactory() {
		return JedisConnectionFactoryExtension.getConnectionFactory(ValkeyStanalone.class);
	}

	/**
	 * Provides a producer method for {@link ValkeyOperations}.
	 */
	@Produces
	public ValkeyOperations<byte[], byte[]> redisOperationsProducer(ValkeyConnectionFactory redisConnectionFactory) {

		ValkeyTemplate<byte[], byte[]> template = new ValkeyTemplate<>();
		template.setConnectionFactory(redisConnectionFactory);
		template.afterPropertiesSet();
		return template;
	}

	// shortcut for managed KeyValueAdapter/Template.
	@Produces
	@PersonDB
	public ValkeyOperations<byte[], byte[]> redisOperationsProducerQualified(ValkeyOperations<byte[], byte[]> instance) {
		return instance;
	}

	public void closeValkeyOperations(@Disposes ValkeyOperations<byte[], byte[]> redisOperations) throws Exception {

		if (redisOperations instanceof DisposableBean disposableBean) {
			disposableBean.destroy();
		}
	}

	/**
	 * Provides a producer method for {@link ValkeyKeyValueTemplate}.
	 */
	@Produces
	public ValkeyKeyValueTemplate redisKeyValueAdapterDefault(ValkeyOperations<?, ?> redisOperations) {

		ValkeyKeyValueAdapter redisKeyValueAdapter = new ValkeyKeyValueAdapter(redisOperations);
		ValkeyKeyValueTemplate keyValueTemplate = new ValkeyKeyValueTemplate(redisKeyValueAdapter, new ValkeyMappingContext());
		return keyValueTemplate;
	}

}
