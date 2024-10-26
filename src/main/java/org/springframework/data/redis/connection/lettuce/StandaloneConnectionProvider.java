/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.AbstractValkeyClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.ValkeyClient;
import io.lettuce.core.ValkeyURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulValkeyConnection;
import io.lettuce.core.codec.ValkeyCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulValkeyMasterReplicaConnection;
import io.lettuce.core.pubsub.StatefulValkeyPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulValkeySentinelConnection;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware;
import org.springframework.lang.Nullable;

/**
 * {@link LettuceConnectionProvider} implementation for a standalone Valkey setup.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class StandaloneConnectionProvider implements LettuceConnectionProvider, TargetAware, ValkeyClientProvider {

	private final ValkeyClient client;
	private final ValkeyCodec<?, ?> codec;
	private final Optional<ReadFrom> readFrom;
	private final Supplier<ValkeyURI> redisURISupplier;

	/**
	 * Create new {@link StandaloneConnectionProvider}.
	 *
	 * @param client must not be {@literal null}.
	 * @param codec must not be {@literal null}.
	 */
	StandaloneConnectionProvider(ValkeyClient client, ValkeyCodec<?, ?> codec) {
		this(client, codec, null);
	}

	/**
	 * Create new {@link StandaloneConnectionProvider}.
	 *
	 * @param client must not be {@literal null}.
	 * @param codec must not be {@literal null}.
	 * @param readFrom can be {@literal null}.
	 * @since 2.1
	 */
	StandaloneConnectionProvider(ValkeyClient client, ValkeyCodec<?, ?> codec, @Nullable ReadFrom readFrom) {

		this.client = client;
		this.codec = codec;
		this.readFrom = Optional.ofNullable(readFrom);

		redisURISupplier = new Supplier<ValkeyURI>() {

			AtomicReference<ValkeyURI> uriFieldReference = new AtomicReference<>();

			@Override
			public ValkeyURI get() {

				ValkeyURI uri = uriFieldReference.get();
				if (uri != null) {
					return uri;
				}

				uri = ValkeyURI.class.cast(new DirectFieldAccessor(client).getPropertyValue("redisURI"));

				return uriFieldReference.compareAndSet(null, uri) ? uri : uriFieldReference.get();
			}
		};
	}

	@Override
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {

		if (connectionType.equals(StatefulValkeySentinelConnection.class)) {
			return connectionType.cast(client.connectSentinel());
		}

		if (connectionType.equals(StatefulValkeyPubSubConnection.class)) {
			return connectionType.cast(client.connectPubSub(codec));
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {

			return connectionType.cast(readFrom.map(it -> this.masterReplicaConnection(redisURISupplier.get(), it))
					.orElseGet(() -> client.connect(codec)));
		}

		throw new UnsupportedOperationException("Connection type " + connectionType + " not supported");
	}

	@Override
	public <T extends StatefulConnection<?, ?>> CompletionStage<T> getConnectionAsync(Class<T> connectionType) {
		return getConnectionAsync(connectionType, redisURISupplier.get());
	}

	@SuppressWarnings({ "null", "unchecked", "rawtypes" })
	@Override
	public <T extends StatefulConnection<?, ?>> CompletionStage<T> getConnectionAsync(Class<T> connectionType,
			ValkeyURI redisURI) {

		if (connectionType.equals(StatefulValkeySentinelConnection.class)) {
			return client.connectSentinelAsync(StringCodec.UTF8, redisURI).thenApply(connectionType::cast);
		}

		if (connectionType.equals(StatefulValkeyPubSubConnection.class)) {
			return client.connectPubSubAsync(codec, redisURI).thenApply(connectionType::cast);
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {
			return readFrom.map(it -> this.masterReplicaConnectionAsync(redisURI, it)) //
					.orElseGet(() -> (CompletionStage) client.connectAsync(codec, redisURI)) //
					.thenApply(connectionType::cast);
		}

		return LettuceFutureUtils
				.failed(new UnsupportedOperationException("Connection type " + connectionType + " not supported"));
	}

	@Override
	public AbstractValkeyClient getValkeyClient() {
		return client;
	}

	private StatefulValkeyConnection masterReplicaConnection(ValkeyURI redisUri, ReadFrom readFrom) {

		StatefulValkeyMasterReplicaConnection<?, ?> connection = MasterReplica.connect(client, codec, redisUri);
		connection.setReadFrom(readFrom);

		return connection;
	}

	private CompletionStage<StatefulValkeyConnection<?, ?>> masterReplicaConnectionAsync(ValkeyURI redisUri,
			ReadFrom readFrom) {

		CompletableFuture<? extends StatefulValkeyMasterReplicaConnection<?, ?>> connection = MasterReplica
				.connectAsync(client,
				codec, redisUri);

		return connection.thenApply(conn -> {

			conn.setReadFrom(readFrom);

			return conn;
		});
	}
}
