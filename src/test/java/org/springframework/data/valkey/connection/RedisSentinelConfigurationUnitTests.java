/*
 * Copyright 2015-2024 the original author or authors.
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
package org.springframework.data.valkey.connection;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

import org.springframework.mock.env.MockPropertySource;
import org.springframework.util.StringUtils;

/**
 * Unit tests for {@link ValkeySentinelConfiguration}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Vikas Garg
 * @author Samuel Klose
 * @author Mustapha Zorgati
 */
class ValkeySentinelConfigurationUnitTests {

	private static final String HOST_AND_PORT_1 = "127.0.0.1:123";
	private static final String HOST_AND_PORT_2 = "localhost:456";
	private static final String HOST_AND_PORT_3 = "localhost:789";

	@Test // DATAREDIS-372
	void shouldCreateValkeySentinelConfigurationCorrectlyGivenMasterAndSingleHostAndPortString() {

		ValkeySentinelConfiguration config = new ValkeySentinelConfiguration("mymaster",
				Collections.singleton(HOST_AND_PORT_1));

		assertThat(config.getSentinels()).hasSize(1);
		assertThat(config.getSentinels()).contains(new ValkeyNode("127.0.0.1", 123));
	}

	@Test // GH-2418
	void shouldCreateValkeySentinelConfigurationCorrectlyGivenMasterAndSingleIPV6HostAndPortString() {

		ValkeySentinelConfiguration config = new ValkeySentinelConfiguration("mymaster",
				Collections.singleton("[ca:fee::1]:123"));

		assertThat(config.getSentinels()).hasSize(1);
		assertThat(config.getSentinels()).contains(new ValkeyNode("ca:fee::1", 123));
	}

	@Test // DATAREDIS-372
	void shouldCreateValkeySentinelConfigurationCorrectlyGivenMasterAndMultipleHostAndPortStrings() {

		ValkeySentinelConfiguration config = new ValkeySentinelConfiguration("mymaster",
				new HashSet<>(Arrays.asList(HOST_AND_PORT_1, HOST_AND_PORT_2, HOST_AND_PORT_3)));

		assertThat(config.getSentinels()).hasSize(3);
		assertThat(config.getSentinels()).contains(new ValkeyNode("127.0.0.1", 123), new ValkeyNode("localhost", 456),
				new ValkeyNode("localhost", 789));
	}

	@Test // DATAREDIS-372
	void shouldThrowExceptionWhenListOfHostAndPortIsNull() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ValkeySentinelConfiguration("mymaster", Collections.singleton(null)));
	}

	@Test // DATAREDIS-372
	void shouldNotFailWhenListOfHostAndPortIsEmpty() {

		ValkeySentinelConfiguration config = new ValkeySentinelConfiguration("mymaster", Collections.emptySet());

		assertThat(config.getSentinels()).isEmpty();
	}

	@Test // DATAREDIS-372
	void shouldThrowExceptionGivenNullPropertySource() {
		assertThatIllegalArgumentException().isThrownBy(() -> ValkeySentinelConfiguration.of(null));
	}

	@Test // DATAREDIS-372
	void shouldNotFailWhenGivenPropertySourceNotContainingRelevantProperties() {

		ValkeySentinelConfiguration config = ValkeySentinelConfiguration.of(new MockPropertySource());

		assertThat(config.getMaster()).isNull();
		assertThat(config.getSentinels()).isEmpty();
	}

	@Test // DATAREDIS-372
	void shouldBeCreatedCorrectlyGivenValidPropertySourceWithMasterAndSingleHostPort() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.master", "myMaster");
		propertySource.setProperty("spring.redis.sentinel.nodes", HOST_AND_PORT_1);

		ValkeySentinelConfiguration config = ValkeySentinelConfiguration.of(propertySource);

		assertThat(config.getMaster()).isNotNull();
		assertThat(config.getMaster().getName()).isEqualTo("myMaster");
		assertThat(config.getSentinels()).hasSize(1);
		assertThat(config.getSentinels()).contains(new ValkeyNode("127.0.0.1", 123));
	}

	@Test // DATAREDIS-372
	void shouldBeCreatedCorrectlyGivenValidPropertySourceWithMasterAndMultipleHostPort() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.master", "myMaster");
		propertySource.setProperty("spring.redis.sentinel.nodes",
				StringUtils.collectionToCommaDelimitedString(Arrays.asList(HOST_AND_PORT_1, HOST_AND_PORT_2, HOST_AND_PORT_3)));

		ValkeySentinelConfiguration config = ValkeySentinelConfiguration.of(propertySource);

		assertThat(config.getSentinels()).hasSize(3);
		assertThat(config.getSentinels()).contains(new ValkeyNode("127.0.0.1", 123), new ValkeyNode("localhost", 456),
				new ValkeyNode("localhost", 789));
	}

	@Test // DATAREDIS-1060
	void dataNodePasswordDoesNotAffectSentinelPassword() {

		ValkeyPassword password = ValkeyPassword.of("88888888-8x8-getting-creative-now");
		ValkeySentinelConfiguration configuration = new ValkeySentinelConfiguration("myMaster",
				Collections.singleton(HOST_AND_PORT_1));
		configuration.setPassword(password);

		assertThat(configuration.getSentinelPassword()).isEqualTo(ValkeyPassword.none());
	}

	@Test // GH-2218
	void dataNodeUsernameDoesNotAffectSentinelUsername() {

		ValkeySentinelConfiguration configuration = new ValkeySentinelConfiguration("myMaster",
				Collections.singleton(HOST_AND_PORT_1));
		configuration.setUsername("data-admin");
		configuration.setSentinelUsername("sentinel-admin");

		assertThat(configuration.getDataNodeUsername()).isEqualTo("data-admin");
		assertThat(configuration.getSentinelUsername()).isEqualTo("sentinel-admin");
	}

	@Test // DATAREDIS-1060
	void readSentinelPasswordFromConfigProperty() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.master", "myMaster");
		propertySource.setProperty("spring.redis.sentinel.nodes", HOST_AND_PORT_1);
		propertySource.setProperty("spring.redis.sentinel.password", "computer-says-no");

		ValkeySentinelConfiguration config = ValkeySentinelConfiguration.of(propertySource);

		assertThat(config.getSentinelPassword()).isEqualTo(ValkeyPassword.of("computer-says-no"));
		assertThat(config.getSentinels()).hasSize(1).contains(new ValkeyNode("127.0.0.1", 123));
	}

	@Test // GH-2218
	void readSentinelUsernameFromConfigProperty() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.master", "myMaster");
		propertySource.setProperty("spring.redis.sentinel.nodes", HOST_AND_PORT_1);
		propertySource.setProperty("spring.redis.sentinel.username", "sentinel-admin");
		propertySource.setProperty("spring.redis.sentinel.password", "foo");

		ValkeySentinelConfiguration config = ValkeySentinelConfiguration.of(propertySource);

		assertThat(config.getSentinelUsername()).isEqualTo("sentinel-admin");
		assertThat(config.getSentinelPassword()).isEqualTo(ValkeyPassword.of("foo"));
		assertThat(config.getSentinels()).hasSize(1).contains(new ValkeyNode("127.0.0.1", 123));
	}

	@Test // GH-2860
	void readSentinelDataNodeUsernameFromConfigProperty() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.dataNode.username", "datanode-user");

		ValkeySentinelConfiguration config = ValkeySentinelConfiguration.of(propertySource);

		assertThat(config.getDataNodeUsername()).isEqualTo("datanode-user");
	}

	@Test // GH-2860
	void readSentinelDataNodePasswordFromConfigProperty() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.dataNode.password", "datanode-password");

		ValkeySentinelConfiguration config = ValkeySentinelConfiguration.of(propertySource);

		assertThat(config.getDataNodePassword()).isEqualTo(ValkeyPassword.of("datanode-password"));
	}

	@Test // GH-2860
	void readSentinelDataNodeDatabaseFromConfigProperty() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.dataNode.database", "5");

		ValkeySentinelConfiguration config = ValkeySentinelConfiguration.of(propertySource);

		assertThat(config.getDatabase()).isEqualTo(5);
	}

	@Test // GH-2860
	void shouldThrowErrorWhen() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.dataNode.database", "thisIsNotAnInteger");

		ThrowableAssert.ThrowingCallable call = () -> ValkeySentinelConfiguration.of(propertySource);

		assertThatThrownBy(call).isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Invalid DB index '%s'; integer required", "thisIsNotAnInteger");
	}

	@Test // GH-2860
	void shouldThrowErrorWhen2() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.dataNode.database", "null");

		ThrowableAssert.ThrowingCallable call = () -> ValkeySentinelConfiguration.of(propertySource);

		assertThatThrownBy(call).isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Invalid DB index '%s'; integer required", "null");
	}
}