/*
 * Copyright 2019-2024 the original author or authors.
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

import io.lettuce.core.ValkeyClient;
import io.lettuce.core.ValkeyURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulValkeyConnection;
import io.lettuce.core.api.sync.ValkeyCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulValkeyClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulValkeyClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulValkeyPubSubConnection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.valkey.SettingsUtils;
import org.springframework.data.valkey.connection.ClusterCommandExecutor;
import org.springframework.data.valkey.connection.ClusterTopologyProvider;
import org.springframework.data.valkey.connection.MessageListener;
import org.springframework.data.valkey.connection.ValkeyClusterConnection;
import org.springframework.data.valkey.connection.ValkeyConfiguration;
import org.springframework.data.valkey.test.condition.EnabledOnValkeyClusterAvailable;
import org.springframework.data.valkey.test.extension.LettuceTestClientResources;
import org.springframework.lang.Nullable;

/**
 * Integration tests to listen for keyspace notifications.
 *
 * @author Mark Paluch
 */
@EnabledOnValkeyClusterAvailable
class LettuceClusterKeyspaceNotificationsTests {

	private static CustomLettuceConnectionFactory factory;
	private String keyspaceConfig;

	// maps to 127.0.0.1:7381/slot hash 13477
	private String key = "10923";

	@BeforeAll
	static void beforeAll() throws Exception {

		factory = new CustomLettuceConnectionFactory(SettingsUtils.clusterConfiguration());
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory.afterPropertiesSet();
		factory.start();
	}

	@BeforeEach
	void before() {

		// enable keyspace events on a specific node.
		withConnection("127.0.0.1", 7381, commands -> {

			keyspaceConfig = commands.configGet("*").get("notify-keyspace-events");
			commands.configSet("notify-keyspace-events", "KEx");
		});

		assertThat(SlotHash.getSlot(key)).isEqualTo(13477);
	}

	@AfterEach
	void tearDown() {

		// Restore previous settings.
		withConnection("127.0.0.1", 7381, commands -> {
			commands.configSet("notify-keyspace-events", keyspaceConfig);
		});
	}

	@AfterAll
	static void afterAll() {
		factory.destroy();
	}

	@Test // DATAREDIS-976
	void shouldListenForKeyspaceNotifications() throws Exception {

		CompletableFuture<String> expiry = new CompletableFuture<>();

		ValkeyClusterConnection connection = factory.getClusterConnection();

		connection.pSubscribe((message, pattern) -> {
			expiry.complete(new String(message.getBody()) + ":" + new String(message.getChannel()));
		}, "__keyspace*@*".getBytes());

		withConnection("127.0.0.1", 7381, commands -> {
			commands.set(key, "foo", SetArgs.Builder.px(1));
		});

		assertThat(expiry.get(2, TimeUnit.SECONDS)).isEqualTo("expired:__keyspace@0__:10923");

		connection.getSubscription().close();
		connection.close();
	}

	private void withConnection(String hostname, int port, Consumer<ValkeyCommands<String, String>> commandsConsumer) {

		ValkeyClient client = ValkeyClient.create(LettuceTestClientResources.getSharedClientResources(),
				ValkeyURI.create(hostname, port));

		StatefulValkeyConnection<String, String> connection = client.connect();
		commandsConsumer.accept(connection.sync());

		connection.close();
		client.shutdownAsync();
	}

	static class CustomLettuceConnectionFactory extends LettuceConnectionFactory {

		CustomLettuceConnectionFactory(ValkeyConfiguration redisConfiguration) {
			super(redisConfiguration);
		}

		@Override
		protected LettuceClusterConnection doCreateLettuceClusterConnection(
				StatefulValkeyClusterConnection<byte[], byte[]> sharedConnection, LettuceConnectionProvider connectionProvider,
				ClusterTopologyProvider topologyProvider, ClusterCommandExecutor clusterCommandExecutor,
				Duration commandTimeout) {
			return new CustomLettuceClusterConnection(sharedConnection, connectionProvider, topologyProvider,
					clusterCommandExecutor, commandTimeout);
		}
	}

	static class CustomLettuceClusterConnection extends LettuceClusterConnection {

		CustomLettuceClusterConnection(@Nullable StatefulValkeyClusterConnection<byte[], byte[]> sharedConnection,
				LettuceConnectionProvider connectionProvider, ClusterTopologyProvider clusterTopologyProvider,
				ClusterCommandExecutor executor, Duration timeout) {
			super(sharedConnection, connectionProvider, clusterTopologyProvider, executor, timeout);
		}

		@Override
		protected LettuceSubscription doCreateSubscription(MessageListener listener,
				StatefulValkeyPubSubConnection<byte[], byte[]> connection, LettuceConnectionProvider connectionProvider) {
			return new CustomLettuceSubscription(listener, (StatefulValkeyClusterPubSubConnection<byte[], byte[]>) connection,
					connectionProvider);
		}
	}

	/**
	 * Customized {@link LettuceSubscription}. Enables
	 * {@link StatefulValkeyClusterPubSubConnection#setNodeMessagePropagation(boolean)} and uses
	 * {@link io.lettuce.core.cluster.api.sync.NodeSelection} to subscribe to all master nodes.
	 */
	static class CustomLettuceSubscription extends LettuceSubscription {

		private final StatefulValkeyClusterPubSubConnection<byte[], byte[]> connection;

		CustomLettuceSubscription(MessageListener listener, StatefulValkeyClusterPubSubConnection<byte[], byte[]> connection,
				LettuceConnectionProvider connectionProvider) {
			super(listener, connection, connectionProvider);
			this.connection = connection;

			// Must be enabled for keyspace notification propagation
			this.connection.setNodeMessagePropagation(true);
		}

		@Override
		protected void doPsubscribe(byte[]... patterns) {
			connection.sync().all().commands().psubscribe(patterns);
		}

		@Override
		protected void doPUnsubscribe(boolean all, byte[]... patterns) {
			connection.sync().all().commands().punsubscribe();
		}

		@Override
		protected void doSubscribe(byte[]... channels) {
			connection.sync().all().commands().subscribe(channels);
		}

		@Override
		protected void doUnsubscribe(boolean all, byte[]... channels) {
			connection.sync().all().commands().unsubscribe();
		}
	}
}