/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.valkey.connection.jedis;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.springframework.data.valkey.ConnectionFactoryTracker;
import org.springframework.data.valkey.connection.ValkeySentinelConfiguration;
import org.springframework.data.valkey.connection.ValkeySentinelConnection;
import org.springframework.data.valkey.connection.ValkeyStandaloneConfiguration;
import org.springframework.data.valkey.test.condition.EnabledOnValkeyAvailable;
import org.springframework.data.valkey.test.condition.EnabledOnValkeySentinelAvailable;
import org.springframework.data.valkey.test.condition.EnabledOnValkeyVersion;
import org.springframework.data.valkey.util.ConnectionVerifier;

/**
 * Integration tests for Valkey 6 ACL.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@EnabledOnValkeyVersion("6.0")
@EnabledOnValkeyAvailable(6382)
class JedisAclIntegrationTests {

	@Test
	void shouldConnectWithDefaultAuthentication() {

		ValkeyStandaloneConfiguration standaloneConfiguration = new ValkeyStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setPassword("foobared");

		ConnectionVerifier.create(new JedisConnectionFactory(standaloneConfiguration)) //
				.execute(connection -> {
					assertThat(connection.ping()).isEqualTo("PONG");
				}) //
				.verifyAndClose();
	}

	@Test // DATAREDIS-1046
	void shouldConnectStandaloneWithAclAuthentication() {

		ValkeyStandaloneConfiguration standaloneConfiguration = new ValkeyStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setUsername("spring");
		standaloneConfiguration.setPassword("data");

		ConnectionVerifier.create(new JedisConnectionFactory(standaloneConfiguration)) //
				.execute(connection -> {
					assertThat(connection.ping()).isEqualTo("PONG");
				}) //
				.verifyAndClose();
	}

	@Test // DATAREDIS-1145
	@EnabledOnValkeySentinelAvailable(26382)
	void shouldConnectSentinelWithAclAuthentication() throws IOException {

		// Note: As per https://github.com/valkey/redis/issues/7708, Sentinel does not support ACL authentication yet.

		ValkeySentinelConfiguration sentinelConfiguration = new ValkeySentinelConfiguration("mymaster",
				Collections.singleton("localhost:26382"));
		sentinelConfiguration.setSentinelPassword("foobared");

		JedisConnectionFactory connectionFactory = new JedisConnectionFactory(sentinelConfiguration);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		try (ValkeySentinelConnection connection = connectionFactory.getSentinelConnection()) {
			assertThat(connection.masters()).isNotEmpty();
		}

		connectionFactory.destroy();
	}

	@Test // DATAREDIS-1046
	void shouldConnectStandaloneWithAclAuthenticationAndPooling() {

		ValkeyStandaloneConfiguration standaloneConfiguration = new ValkeyStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setUsername("spring");
		standaloneConfiguration.setPassword("data");

		JedisConnectionFactory connectionFactory = new JedisConnectionFactory(standaloneConfiguration,
				JedisClientConfiguration.builder().usePooling().build());

		ConnectionVerifier.create(connectionFactory) //
				.execute(connection -> {
					assertThat(connection.ping()).isEqualTo("PONG");
				}) //
				.verifyAndClose();
	}
}
