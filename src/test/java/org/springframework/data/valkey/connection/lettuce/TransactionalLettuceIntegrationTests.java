/*
 * Copyright 2014-2024 the original author or authors.
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
package org.springframework.data.valkey.connection.lettuce;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.valkey.SettingsUtils;
import org.springframework.data.valkey.connection.AbstractTransactionalTestBase;
import org.springframework.data.valkey.connection.lettuce.TransactionalLettuceIntegrationTests.LettuceContextConfiguration;
import org.springframework.data.valkey.test.extension.LettuceTestClientResources;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ContextConfiguration(classes = { LettuceContextConfiguration.class })
public class TransactionalLettuceIntegrationTests extends AbstractTransactionalTestBase {

	@Configuration
	public static class LettuceContextConfiguration extends ValkeyContextConfiguration {

		@Override
		@Bean
		public LettuceConnectionFactory redisConnectionFactory() {

			LettuceConnectionFactory factory = new LettuceConnectionFactory();
			factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
			factory.setHostName(SettingsUtils.getHost());
			factory.setPort(SettingsUtils.getPort());
			return factory;
		}
	}
}
