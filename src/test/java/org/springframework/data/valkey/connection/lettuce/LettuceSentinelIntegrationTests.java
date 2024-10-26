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
package org.springframework.data.valkey.connection.lettuce;

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.ReadFrom;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.valkey.ConnectionFactoryTracker;
import org.springframework.data.valkey.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.valkey.connection.DefaultStringValkeyConnection;
import org.springframework.data.valkey.connection.ValkeyConnection;
import org.springframework.data.valkey.connection.ValkeySentinelConfiguration;
import org.springframework.data.valkey.connection.ValkeySentinelConnection;
import org.springframework.data.valkey.connection.ValkeyServer;
import org.springframework.data.valkey.connection.StringValkeyConnection;
import org.springframework.data.valkey.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.valkey.test.condition.EnabledOnValkeySentinelAvailable;
import org.springframework.data.valkey.test.extension.LettuceTestClientResources;
import org.springframework.data.valkey.test.extension.ValkeySentinel;

/**
 * Integration tests for Lettuce and Valkey Sentinel interaction.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith(LettuceConnectionFactoryExtension.class)
@EnabledOnValkeySentinelAvailable
public class LettuceSentinelIntegrationTests extends AbstractConnectionIntegrationTests {

	private static final String MASTER_NAME = "mymaster";
	private static final ValkeyServer SENTINEL_0 = new ValkeyServer("127.0.0.1", 26379);
	private static final ValkeyServer SENTINEL_1 = new ValkeyServer("127.0.0.1", 26380);

	private static final ValkeyServer REPLICA_0 = new ValkeyServer("127.0.0.1", 6380);
	private static final ValkeyServer REPLICA_1 = new ValkeyServer("127.0.0.1", 6381);

	private static final ValkeySentinelConfiguration SENTINEL_CONFIG;
	static {

		SENTINEL_CONFIG = new ValkeySentinelConfiguration() //
				.master(MASTER_NAME).sentinel(SENTINEL_0).sentinel(SENTINEL_1);

		SENTINEL_CONFIG.setDatabase(5);
	}

	public LettuceSentinelIntegrationTests(@ValkeySentinel LettuceConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@AfterEach
	public void tearDown() {

		try {

			// since we use more than one db we're required to flush them all
			connection.flushAll();
		} catch (Exception ignore) {
			// Connection may be closed in certain cases, like after pub/sub tests
		}
		connection.close();
	}

	@Test
	@Disabled("SELECT not allowed on shared connections")
	@Override
	public void testMove() {}

	@Test
	@Disabled("SELECT not allowed on shared connections")
	@Override
	public void testSelect() {
		super.testSelect();
	}

	@Test // DATAREDIS-348
	void shouldReadMastersCorrectly() {

		List<ValkeyServer> servers = (List<ValkeyServer>) connectionFactory.getSentinelConnection().masters();
		assertThat(servers.size()).isEqualTo(1);
		assertThat(servers.get(0).getName()).isEqualTo(MASTER_NAME);
	}

	@Test // DATAREDIS-842, DATAREDIS-973
	void shouldUseSpecifiedDatabase() {

		ValkeyConnection connection = connectionFactory.getConnection();

		connection.flushAll();
		connection.set("foo".getBytes(), "bar".getBytes());
		connection.close();

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.setShareNativeConnection(false);
		connectionFactory.setDatabase(5);
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		try(ValkeyConnection directConnection = connectionFactory.getConnection()) {

			assertThat(directConnection.exists("foo".getBytes())).isFalse();
			directConnection.select(0);

			assertThat(directConnection.exists("foo".getBytes())).isTrue();
		} finally {
			connectionFactory.destroy();
		}


	}

	@Test // DATAREDIS-973
	void reactiveShouldUseSpecifiedDatabase() {

		ValkeyConnection connection = connectionFactory.getConnection();

		connection.flushAll();
		connection.set("foo".getBytes(), "bar".getBytes());

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.setShareNativeConnection(false);
		connectionFactory.setDatabase(5);
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		try(LettuceReactiveValkeyConnection reactiveConnection = connectionFactory.getReactiveConnection()) {

			reactiveConnection.keyCommands().exists(ByteBuffer.wrap("foo".getBytes())) //
					.as(StepVerifier::create) //
					.expectNext(false) //
					.verifyComplete();

		} finally {
			connectionFactory.destroy();
		}

	}

	@Test
	// DATAREDIS-348
	void shouldReadReplicasOfMastersCorrectly() {

		ValkeySentinelConnection sentinelConnection = connectionFactory.getSentinelConnection();

		List<ValkeyServer> servers = (List<ValkeyServer>) sentinelConnection.masters();
		assertThat(servers.size()).isEqualTo(1);

		Collection<ValkeyServer> replicas = sentinelConnection.replicas(servers.get(0));
		assertThat(replicas).containsAnyOf(REPLICA_0, REPLICA_1);
	}

	@Test // DATAREDIS-462
	@Disabled("Until Lettuce has moved to Sinks API")
	void factoryWorksWithoutClientResources() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG);
		factory.setShutdownTimeout(0);
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringValkeyConnection connection = new DefaultStringValkeyConnection(factory.getConnection());

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-576
	void connectionAppliesClientName() {

		LettuceClientConfiguration clientName = LettuceTestClientConfiguration.builder().clientName("clientName").build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG, clientName);
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringValkeyConnection connection = new DefaultStringValkeyConnection(factory.getConnection());

		try {
			assertThat(connection.getClientName()).isEqualTo("clientName");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-580
	void factoryWithReadFromMasterSettings() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG,
				LettuceTestClientConfiguration.builder().readFrom(ReadFrom.MASTER).build());
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringValkeyConnection connection = new DefaultStringValkeyConnection(factory.getConnection());

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("master");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-580
	void factoryWithReadFromReplicaSettings() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG,
				LettuceTestClientConfiguration.builder().readFrom(ReadFrom.REPLICA).build());
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringValkeyConnection connection = new DefaultStringValkeyConnection(factory.getConnection());

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("slave");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-580
	void factoryUsesMasterReplicaConnections() {

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().readFrom(ReadFrom.SLAVE)
				.build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG, configuration);
		factory.afterPropertiesSet();
		factory.start();

		try(ValkeyConnection connection = factory.getConnection()) {

			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("slave");
		} finally {
			factory.destroy();
		}
	}
}