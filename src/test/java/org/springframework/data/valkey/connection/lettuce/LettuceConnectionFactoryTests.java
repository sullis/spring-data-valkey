/*
 * Copyright 2011-2024 the original author or authors.
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
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

import io.lettuce.core.EpollProvider;
import io.lettuce.core.KqueueProvider;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.ValkeyException;
import io.lettuce.core.api.async.ValkeyAsyncCommands;
import io.lettuce.core.api.reactive.BaseValkeyReactiveCommands;
import reactor.test.StepVerifier;

import java.io.File;
import java.time.Duration;
import java.util.function.Consumer;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.data.valkey.ConnectionFactoryTracker;
import org.springframework.data.valkey.ValkeyConnectionFailureException;
import org.springframework.data.valkey.ValkeySystemException;
import org.springframework.data.valkey.SettingsUtils;
import org.springframework.data.valkey.connection.ClusterCommandExecutor;
import org.springframework.data.valkey.connection.DefaultStringValkeyConnection;
import org.springframework.data.valkey.connection.ValkeyConnection;
import org.springframework.data.valkey.connection.ValkeyStandaloneConfiguration;
import org.springframework.data.valkey.connection.ValkeyStaticMasterReplicaConfiguration;
import org.springframework.data.valkey.connection.StringValkeyConnection;
import org.springframework.data.valkey.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.valkey.test.condition.EnabledOnValkeyClusterAvailable;
import org.springframework.data.valkey.test.extension.LettuceTestClientResources;

/**
 * Integration test of {@link LettuceConnectionFactory}
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(LettuceConnectionFactoryExtension.class)
class LettuceConnectionFactoryTests {

	private LettuceConnectionFactory factory;

	private StringValkeyConnection connection;

	@BeforeEach
	void setUp() {

		factory = new LettuceConnectionFactory(SettingsUtils.standaloneConfiguration());
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory.afterPropertiesSet();
		factory.setShutdownTimeout(0);
		factory.start();
		connection = new DefaultStringValkeyConnection(factory.getConnection());
	}

	@AfterEach
	void tearDown() {

		if (connection != null) {
			connection.close();
		}

		factory.destroy();
	}

	@SuppressWarnings("rawtypes")
	@Test
	void testGetNewConnectionOnError() throws Exception {
		factory.setValidateConnection(true);
		connection.lPush("alist", "baz");
		ValkeyAsyncCommands nativeConn = (ValkeyAsyncCommands) connection.getNativeConnection();
		nativeConn.getStatefulConnection().close();
		// Give some time for async channel close
		Thread.sleep(500);
		connection.bLPop(1, "alist".getBytes());
		try {
			connection.get("test3");
			fail("Expected exception using natively closed conn");
		} catch (ValkeySystemException expected) {
			// expected, shared conn is closed
		}
		DefaultStringValkeyConnection conn2 = new DefaultStringValkeyConnection(factory.getConnection());
		assertThat(conn2.getNativeConnection()).isNotSameAs(nativeConn);
		conn2.set("anotherkey", "anothervalue");
		assertThat(conn2.get("anotherkey")).isEqualTo("anothervalue");
		conn2.close();
	}

	@SuppressWarnings("rawtypes")
	@Test
	void testConnectionErrorNoValidate() throws Exception {
		connection.lPush("ablist", "baz");
		((ValkeyAsyncCommands) connection.getNativeConnection()).getStatefulConnection().close();
		// Give some time for async channel close
		Thread.sleep(500);
		DefaultStringValkeyConnection conn2 = new DefaultStringValkeyConnection(factory.getConnection());
		try {
			conn2.set("anotherkey", "anothervalue");
			fail("Expected exception using natively closed conn");
		} catch (ValkeySystemException expected) {
			// expected, as we are re-using the natively closed conn
		} finally {
			conn2.close();
		}
	}

	@Test
	void testValidateNoError() {
		factory.setValidateConnection(true);
		ValkeyConnection conn2 = factory.getConnection();
		assertThat(conn2.getNativeConnection()).isSameAs(connection.getNativeConnection());
	}

	@Test // DATAREDIS-973
	void testSelectDb() {

		// put an item in database 0
		connection.set("sometestkey", "sometestvalue");

		LettuceConnectionFactory sharingConnectionFactory = newConnectionFactory(cf -> cf.setDatabase(1));
		runSelectDbTest(sharingConnectionFactory);

		LettuceConnectionFactory nonSharingConnectionFactory = newConnectionFactory(cf -> {
			cf.setDatabase(1);
			cf.setShareNativeConnection(false);
		});
		runSelectDbTest(nonSharingConnectionFactory);
	}

	@Test // DATAREDIS-973
	void testSelectDbReactive() {

		LettuceConnectionFactory sharingConnectionFactory = newConnectionFactory(cf -> cf.setDatabase(1));
		runSelectDbReactiveTest(sharingConnectionFactory);

		LettuceConnectionFactory nonSharingConnectionFactory = newConnectionFactory(cf -> {
			cf.setDatabase(1);
			cf.setShareNativeConnection(false);
		});
		runSelectDbTest(nonSharingConnectionFactory);
	}

	private void runSelectDbTest(LettuceConnectionFactory factory2) {

		ConnectionFactoryTracker.add(factory2);

		StringValkeyConnection separateDatabase = new DefaultStringValkeyConnection(factory2.getConnection());
		separateDatabase.flushDb();

		// put an item in database 0
		connection.set("sometestkey", "sometestvalue");

		try {
			// there should still be nothing in database 1
			assertThat(separateDatabase.dbSize()).isEqualTo(Long.valueOf(0));
		} finally {
			separateDatabase.close();
			factory2.destroy();
		}
	}

	private void runSelectDbReactiveTest(LettuceConnectionFactory factory2) {

		ConnectionFactoryTracker.add(factory2);

		LettuceReactiveValkeyConnection separateDatabase = factory2.getReactiveConnection();

		separateDatabase.serverCommands().flushDb() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		// put an item in database 0
		connection.set("sometestkey", "sometestvalue");

		try {
			// there should still be nothing in database 1
			separateDatabase.serverCommands().dbSize() //
					.as(StepVerifier::create).expectNext(0L) //
					.verifyComplete();
		} finally {
			separateDatabase.close();
			factory2.destroy();
		}
	}

	private static LettuceConnectionFactory newConnectionFactory(Consumer<LettuceConnectionFactory> customizer) {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.setShutdownTimeout(0);
		customizer.accept(connectionFactory);
		connectionFactory.start();

		return connectionFactory;
	}

	@SuppressWarnings("unchecked")
	@Test
	void testDisableSharedConnection() throws Exception {
		factory.setShareNativeConnection(false);
		ValkeyConnection conn2 = factory.getConnection();
		assertThat(conn2.getNativeConnection()).isNotSameAs(connection.getNativeConnection());
		Thread.sleep(100);
		conn2.close();
		assertThat(conn2.isClosed()).isTrue();

		assertThatExceptionOfType(ValkeySystemException.class).isThrownBy(conn2::getNativeConnection);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testResetConnection() {
		ValkeyAsyncCommands<byte[], byte[]> nativeConn = (ValkeyAsyncCommands<byte[], byte[]>) connection
				.getNativeConnection();
		factory.resetConnection();
		assertThat(factory.getConnection().getNativeConnection()).isNotSameAs(nativeConn);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testInitConnection() {
		ValkeyAsyncCommands<byte[], byte[]> nativeConn = (ValkeyAsyncCommands<byte[], byte[]>) connection
				.getNativeConnection();
		factory.initConnection();
		ValkeyConnection newConnection = factory.getConnection();
		assertThat(newConnection.getNativeConnection()).isNotSameAs(nativeConn);
		newConnection.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	void testResetAndInitConnection() {
		ValkeyAsyncCommands<byte[], byte[]> nativeConn = (ValkeyAsyncCommands<byte[], byte[]>) connection
				.getNativeConnection();
		factory.resetConnection();
		factory.initConnection();
		ValkeyConnection newConnection = factory.getConnection();
		assertThat(newConnection.getNativeConnection()).isNotSameAs(nativeConn);
		newConnection.close();
	}

	@Test
	void testGetConnectionException() {
		factory.stop();
		factory.setHostName("fakeHost");
		factory.start();
		try {
			factory.getConnection();
			fail("Expected connection failure exception");
		} catch (ValkeyConnectionFailureException expected) {
		}
	}

	@Test
	void testGetConnectionNotSharedBadHostname() {
		factory.setShareNativeConnection(false);
		factory.setHostName("fakeHost");
		factory.start();
		factory.getConnection();
	}

	@Test
	void testGetSharedConnectionNotShared() {
		factory.setShareNativeConnection(false);
		factory.setHostName("fakeHost");
		factory.start();
		assertThat(factory.getSharedConnection()).isNull();
	}

	@Test // DATAREDIS-431
	void dbIndexShouldBePropagatedCorrectly() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory();
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory.setDatabase(2);
		factory.start();

		ConnectionFactoryTracker.add(factory);

		StringValkeyConnection connectionToDbIndex2 = new DefaultStringValkeyConnection(factory.getConnection());

		try {

			String key = "key-in-db-2";
			connectionToDbIndex2.set(key, "the wheel of time");

			assertThat(connection.get(key)).isNull();
			assertThat(connectionToDbIndex2.get(key)).isNotNull();
		} finally {
			connectionToDbIndex2.close();
		}
	}

	@Test // DATAREDIS-462
	@Disabled("Until Lettuce upgrades to Sinks")
	void factoryWorksWithoutClientResources() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory();
		factory.setShutdownTimeout(0);
		factory.start();

		ConnectionFactoryTracker.add(factory);

		StringValkeyConnection connection = new DefaultStringValkeyConnection(factory.getConnection());

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-525
	void factoryShouldReturnReactiveConnectionWhenCorrectly() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory();
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory.start();

		ConnectionFactoryTracker.add(factory);

		assertThat(factory.getReactiveConnection().execute(BaseValkeyReactiveCommands::ping).blockFirst()).isEqualTo("PONG");
	}

	@Test // DATAREDIS-667
	void factoryCreatesPooledConnections() {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

		LettuceClientConfiguration configuration = LettucePoolingClientConfiguration.builder().poolConfig(poolConfig)
				.clientResources(LettuceTestClientResources.getSharedClientResources()).shutdownTimeout(Duration.ZERO)
				.shutdownQuietPeriod(Duration.ZERO).build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(new ValkeyStandaloneConfiguration(), configuration);
		factory.setShareNativeConnection(false);
		factory.start();

		ConnectionFactoryTracker.add(factory);

		ValkeyConnection initial = factory.getConnection();
		Object initialNativeConnection = initial.getNativeConnection();

		initial.close();

		ValkeyConnection subsequent = factory.getConnection();
		Object subsequentNativeConnection = subsequent.getNativeConnection();

		subsequent.close();

		assertThat(initialNativeConnection).isEqualTo(subsequentNativeConnection);

		factory.destroy();
	}

	@Test // DATAREDIS-687
	void connectsThroughValkeySocket() {

		assumeTrue(EpollProvider.isAvailable() || KqueueProvider.isAvailable());
		assumeTrue(new File(SettingsUtils.getSocket()).exists());

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder()
				.clientResources(LettuceTestClientResources.getSharedClientResources()).build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SettingsUtils.socketConfiguration(), configuration);
		factory.setShareNativeConnection(false);
		factory.start();

		ValkeyConnection connection = factory.getConnection();
		assertThat(connection.ping()).isEqualTo("PONG");

		connection.close();
		factory.destroy();
	}

	@Test // DATAREDIS-762, DATAREDIS-869
	void factoryUsesElastiCacheMasterReplicaConnections() {

		assumeTrue("No replicas connected to %s:%d".formatted(SettingsUtils.getHost(), SettingsUtils.getPort()),
				connection.info("replication").getProperty("connected_slaves", "0").compareTo("0") > 0);

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().readFrom(ReadFrom.REPLICA)
				.clientResources(LettuceTestClientResources.getSharedClientResources()).build();

		ValkeyStaticMasterReplicaConfiguration elastiCache = new ValkeyStaticMasterReplicaConfiguration(
				SettingsUtils.getHost()).node(SettingsUtils.getHost(), SettingsUtils.getPort() + 1);

		LettuceConnectionFactory factory = new LettuceConnectionFactory(elastiCache, configuration);
		factory.start();

		ValkeyConnection connection = factory.getConnection();

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("slave");
		} finally {
			connection.close();
		}

		factory.destroy();
	}

	@Test // DATAREDIS-1093
	void pubSubDoesNotSupportMasterReplicaConnections() {

		assumeTrue("No replicas connected to %s:%d".formatted(SettingsUtils.getHost(), SettingsUtils.getPort()),
				connection.info("replication").getProperty("connected_slaves", "0").compareTo("0") > 0);

		ValkeyStaticMasterReplicaConfiguration elastiCache = new ValkeyStaticMasterReplicaConfiguration(
				SettingsUtils.getHost()).node(SettingsUtils.getHost(), SettingsUtils.getPort() + 1);

		LettuceConnectionFactory factory = new LettuceConnectionFactory(elastiCache,
				LettuceTestClientConfiguration.defaultConfiguration());
		factory.start();

		ValkeyConnection connection = factory.getConnection();

		assertThatThrownBy(() -> connection.pSubscribe((message, pattern) -> {}, "foo".getBytes()))
				.isInstanceOf(ValkeyConnectionFailureException.class).hasCauseInstanceOf(UnsupportedOperationException.class);

		connection.close();
		factory.destroy();
	}

	@Test // DATAREDIS-762, DATAREDIS-869
	void factoryUsesElastiCacheMasterWithoutMaster() {

		assumeTrue("No replicas connected to %s:%d.".formatted(SettingsUtils.getHost(), SettingsUtils.getPort()),
				connection.info("replication").getProperty("connected_slaves", "0").compareTo("0") > 0);

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().readFrom(ReadFrom.MASTER)
				.build();

		ValkeyStaticMasterReplicaConfiguration elastiCache = new ValkeyStaticMasterReplicaConfiguration(
				SettingsUtils.getHost(), SettingsUtils.getPort() + 1);

		LettuceConnectionFactory factory = new LettuceConnectionFactory(elastiCache, configuration);
		factory.start();

		ValkeyConnection connection = factory.getConnection();

		try {
			connection.ping();
			fail("Expected ValkeyException: Master is currently unknown");
		} catch (ValkeySystemException ex) {
			assertThat(ex.getCause()).isInstanceOf(ValkeyException.class);
			assertThat(ex.getCause().getMessage()).contains("Master is currently unknown");
		} finally {
			connection.close();
		}

		factory.destroy();
	}

	@Test // DATAREDIS-580, DATAREDIS-869
	void factoryUsesMasterReplicaConnections() {

		assumeTrue("No replicas connected to %s:%d".formatted(SettingsUtils.getHost(), SettingsUtils.getPort()),
				connection.info("replication").getProperty("connected_slaves", "0").compareTo("0") > 0);

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().readFrom(ReadFrom.SLAVE)
				.build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SettingsUtils.standaloneConfiguration(),
				configuration);

		factory.start();

		ValkeyConnection connection = factory.getConnection();

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("slave");
		} finally {
			connection.close();
		}

		factory.destroy();
	}

	@Test // DATAREDIS-576
	void connectionAppliesClientName() {

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().clientName("clientName")
				.build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(new ValkeyStandaloneConfiguration(), configuration);
		factory.setShareNativeConnection(false);
		factory.start();

		ConnectionFactoryTracker.add(factory);

		ValkeyConnection connection = factory.getConnection();

		assertThat(connection.getClientName()).isEqualTo("clientName");
		connection.close();
	}

	@Test // DATAREDIS-576
	void getClientNameShouldEqualWithFactorySetting() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(new ValkeyStandaloneConfiguration());
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory.setClientName("clientName");
		factory.start();

		ConnectionFactoryTracker.add(factory);

		ValkeyConnection connection = factory.getConnection();
		assertThat(connection.getClientName()).isEqualTo("clientName");

		connection.close();
	}

	@Test // GH-2186
	void shouldInitializeMasterReplicaConnectionsEagerly() {

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().build();

		ValkeyStaticMasterReplicaConfiguration elastiCache = new ValkeyStaticMasterReplicaConfiguration(
				SettingsUtils.getHost()).node(SettingsUtils.getHost(), SettingsUtils.getPort() + 1);

		LettuceConnectionFactory factory = new LettuceConnectionFactory(elastiCache, configuration);
		factory.setEagerInitialization(true);
		factory.start();

		assertThat(factory.getSharedConnection()).isNotNull();
		assertThat(factory.getSharedClusterConnection()).isNull();

		factory.getConnection().close();
		factory.destroy();
	}

	@Test // GH-2186
	@EnabledOnValkeyClusterAvailable
	void shouldInitializeClusterConnectionsEagerly() {

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SettingsUtils.clusterConfiguration(),
				configuration);
		factory.setEagerInitialization(true);
		factory.start();

		assertThat(factory.getSharedConnection()).isNull();
		assertThat(factory.getSharedClusterConnection()).isNotNull();

		factory.getConnection().close();
		factory.destroy();
	}

	@Test // GH-2594
	@EnabledOnValkeyClusterAvailable
	void configuresExecutorCorrectly() {

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().build();
		AsyncTaskExecutor mockTaskExecutor = mock(AsyncTaskExecutor.class);

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SettingsUtils.clusterConfiguration(),
				configuration);
		factory.setExecutor(mockTaskExecutor);
		factory.start();

		ClusterCommandExecutor clusterCommandExecutor = factory.getRequiredClusterCommandExecutor();
		assertThat(clusterCommandExecutor).extracting("executor").isEqualTo(mockTaskExecutor);

		factory.destroy();
	}

	@Test // GH-2503
	void startStopStartConnectionFactory() {

		assertThat(factory.isRunning()).isTrue();
		factory.stop();

		assertThat(factory.isRunning()).isFalse();
		assertThatIllegalStateException().isThrownBy(() -> factory.getConnection());

		factory.start();
		assertThat(factory.isRunning()).isTrue();

		try (ValkeyConnection connection = factory.getConnection()) {
			assertThat(connection.ping()).isEqualTo("PONG");
		}
	}
}