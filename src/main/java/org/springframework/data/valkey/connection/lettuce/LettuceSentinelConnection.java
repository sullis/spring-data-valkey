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

import io.lettuce.core.ValkeyClient;
import io.lettuce.core.ValkeyURI;
import io.lettuce.core.ValkeyURI.Builder;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.sentinel.api.StatefulValkeySentinelConnection;
import io.lettuce.core.sentinel.api.sync.ValkeySentinelCommands;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.data.valkey.ExceptionTranslationStrategy;
import org.springframework.data.valkey.FallbackExceptionTranslationStrategy;
import org.springframework.data.valkey.connection.NamedNode;
import org.springframework.data.valkey.connection.ValkeyNode;
import org.springframework.data.valkey.connection.ValkeySentinelConnection;
import org.springframework.data.valkey.connection.ValkeyServer;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.5
 */
public class LettuceSentinelConnection implements ValkeySentinelConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			LettuceExceptionConverter.INSTANCE);

	private final LettuceConnectionProvider provider;
	private StatefulValkeySentinelConnection<String, String> connection; // no that should not be null

	/**
	 * Creates a {@link LettuceSentinelConnection} with a dedicated client for a supplied {@link ValkeyNode}.
	 *
	 * @param sentinel The sentinel to connect to.
	 */
	public LettuceSentinelConnection(ValkeyNode sentinel) {
		this(sentinel.getHost(), sentinel.getPort());
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} with a client for the supplied {@code host} and {@code port}.
	 *
	 * @param host must not be {@literal null}.
	 * @param port sentinel port.
	 */
	public LettuceSentinelConnection(String host, int port) {

		Assert.notNull(host, "Cannot create LettuceSentinelConnection using 'null' as host.");

		this.provider = new DedicatedClientConnectionProvider(host, port);
		init();
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} with a client for the supplied {@code host} and {@code port} and reuse
	 * existing {@link ClientResources}.
	 *
	 * @param host must not be {@literal null}.
	 * @param port sentinel port.
	 * @param clientResources must not be {@literal null}.
	 */
	public LettuceSentinelConnection(String host, int port, ClientResources clientResources) {

		Assert.notNull(clientResources, "Cannot create LettuceSentinelConnection using 'null' as ClientResources.");
		Assert.notNull(host, "Cannot create LettuceSentinelConnection using 'null' as host.");

		this.provider = new DedicatedClientConnectionProvider(host, port, clientResources);
		init();
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} using a supplied {@link ValkeyClient}.
	 *
	 * @param redisClient must not be {@literal null}.
	 */
	public LettuceSentinelConnection(ValkeyClient redisClient) {

		Assert.notNull(redisClient, "Cannot create LettuceSentinelConnection using 'null' as client.");
		this.provider = new LettuceConnectionProvider() {
			@Override
			public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> t) {
				return t.cast(redisClient.connectSentinel());
			}

			@Override
			public <T extends StatefulConnection<?, ?>> CompletableFuture<T> getConnectionAsync(Class<T> t) {
				return CompletableFuture.completedFuture(t.cast(connection));
			}
		};
		init();
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} using a supplied redis connection.
	 *
	 * @param connection native Lettuce connection, must not be {@literal null}
	 */
	protected LettuceSentinelConnection(StatefulValkeySentinelConnection<String, String> connection) {

		Assert.notNull(connection, "Cannot create LettuceSentinelConnection using 'null' as connection.");
		this.provider = new LettuceConnectionProvider() {
			@Override
			public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> t) {
				return t.cast(connection);
			}

			@Override
			public <T extends StatefulConnection<?, ?>> CompletableFuture<T> getConnectionAsync(Class<T> t) {
				return CompletableFuture.completedFuture(t.cast(connection));
			}
		};
		init();
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} using a {@link LettuceConnectionProvider}.
	 *
	 * @param connectionProvider must not be {@literal null}.
	 * @since 2.0
	 */
	public LettuceSentinelConnection(LettuceConnectionProvider connectionProvider) {

		Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null");
		this.provider = connectionProvider;
		init();
	}

	@Override
	public void failover(NamedNode master) {

		Assert.notNull(master, "Valkey node master must not be 'null' for failover.");
		Assert.hasText(master.getName(), "Valkey master name must not be 'null' or empty for failover.");
		getSentinelCommands().failover(master.getName());
	}

	@Override
	public List<ValkeyServer> masters() {
		try {
			return LettuceConverters.toListOfValkeyServer(getSentinelCommands().masters());
		} catch (Exception ex) {
			throw EXCEPTION_TRANSLATION.translate(ex);
		}
	}

	@Override
	public List<ValkeyServer> replicas(NamedNode master) {

		Assert.notNull(master, "Master node cannot be 'null' when loading replicas.");
		return slaves(master.getName());
	}

	/**
	 * @param masterName
	 * @see org.springframework.data.valkey.connection.ValkeySentinelCommands#replicas(org.springframework.data.valkey.connection.NamedNode)
	 * @return
	 */
	public List<ValkeyServer> slaves(String masterName) {

		Assert.hasText(masterName, "Name of redis master cannot be 'null' or empty when loading replicas.");
		try {
			return LettuceConverters.toListOfValkeyServer(getSentinelCommands().slaves(masterName));
		} catch (Exception ex) {
			throw EXCEPTION_TRANSLATION.translate(ex);
		}
	}

	@Override
	public void remove(NamedNode master) {

		Assert.notNull(master, "Master node cannot be 'null' when trying to remove.");
		remove(master.getName());
	}

	/**
	 * @param masterName
	 * @see org.springframework.data.valkey.connection.ValkeySentinelCommands#remove(org.springframework.data.valkey.connection.NamedNode)
	 */
	public void remove(String masterName) {

		Assert.hasText(masterName, "Name of redis master cannot be 'null' or empty when trying to remove.");
		getSentinelCommands().remove(masterName);
	}

	@Override
	public void monitor(ValkeyServer server) {

		Assert.notNull(server, "Cannot monitor 'null' server.");
		Assert.hasText(server.getName(), "Name of server to monitor must not be 'null' or empty");
		Assert.hasText(server.getHost(), "Host must not be 'null' for server to monitor.");
		Assert.notNull(server.getPort(), "Port must not be 'null' for server to monitor.");
		Assert.notNull(server.getQuorum(), "Quorum must not be 'null' for server to monitor.");
		getSentinelCommands().monitor(server.getName(), server.getHost(), server.getPort().intValue(),
				server.getQuorum().intValue());
	}

	@Override
	public void close() throws IOException {
		provider.release(connection);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void init() {

		if (connection == null) {
			connection = provider.getConnection(StatefulValkeySentinelConnection.class);
		}
	}

	private ValkeySentinelCommands<String, String> getSentinelCommands() {
		return connection.sync();
	}

	@Override
	public boolean isOpen() {
		return connection != null && connection.isOpen();
	}

	/**
	 * {@link LettuceConnectionProvider} for a dedicated client instance.
	 */
	private static class DedicatedClientConnectionProvider implements LettuceConnectionProvider {

		private final ValkeyClient redisClient;
		private final ValkeyURI uri;

		DedicatedClientConnectionProvider(String host, int port) {

			Assert.notNull(host, "Cannot create LettuceSentinelConnection using 'null' as host.");

			uri = Builder.redis(host, port).build();
			redisClient = ValkeyClient.create(uri);
		}

		DedicatedClientConnectionProvider(String host, int port, ClientResources clientResources) {

			Assert.notNull(clientResources, "Cannot create LettuceSentinelConnection using 'null' as ClientResources.");
			Assert.notNull(host, "Cannot create LettuceSentinelConnection using 'null' as host.");

			this.uri = Builder.redis(host, port).build();
			redisClient = ValkeyClient.create(clientResources, uri);
		}

		@Override
		public <T extends StatefulConnection<?, ?>> CompletableFuture<T> getConnectionAsync(Class<T> connectionType) {
			return redisClient.connectSentinelAsync(StringCodec.UTF8, uri).thenApply(connectionType::cast);
		}

		@Override
		public void release(StatefulConnection<?, ?> connection) {

			connection.close();
			redisClient.shutdown();
		}

		@Override
		public CompletableFuture<Void> releaseAsync(StatefulConnection<?, ?> connection) {
			return connection.closeAsync().exceptionally(LettuceFutureUtils.ignoreErrors())
					.thenCompose(it -> redisClient.shutdownAsync());
		}
	}
}
