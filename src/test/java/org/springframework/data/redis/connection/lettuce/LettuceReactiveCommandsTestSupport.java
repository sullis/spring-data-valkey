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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.AbstractValkeyClient;
import io.lettuce.core.ValkeyClient;
import io.lettuce.core.api.StatefulValkeyConnection;
import io.lettuce.core.api.sync.ValkeyCommands;
import io.lettuce.core.cluster.ValkeyClusterClient;
import io.lettuce.core.cluster.api.StatefulValkeyClusterConnection;
import io.lettuce.core.cluster.api.sync.ValkeyAdvancedClusterCommands;
import io.lettuce.core.cluster.api.sync.ValkeyClusterCommands;
import io.lettuce.core.codec.StringCodec;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.lettuce.LettuceReactiveValkeyConnection.ByteBufferCodec;
import org.springframework.data.redis.test.condition.ValkeyDetector;
import org.springframework.data.redis.test.extension.LettuceExtension;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@MethodSource("parameters")
public abstract class LettuceReactiveCommandsTestSupport {

	static final String KEY_1 = "key-1";
	static final String KEY_2 = "key-2";
	static final String KEY_3 = "key-3";
	static final String SAME_SLOT_KEY_1 = "{key}-1";
	static final String SAME_SLOT_KEY_2 = "{key}-2";
	static final String SAME_SLOT_KEY_3 = "{key}-3";
	static final String VALUE_1 = "value-1";
	static final String VALUE_2 = "value-2";
	static final String VALUE_3 = "value-3";

	static final byte[] SAME_SLOT_KEY_1_BYTES = SAME_SLOT_KEY_1.getBytes(StandardCharsets.UTF_8);
	static final byte[] SAME_SLOT_KEY_2_BYTES = SAME_SLOT_KEY_2.getBytes(StandardCharsets.UTF_8);
	static final byte[] SAME_SLOT_KEY_3_BYTES = SAME_SLOT_KEY_3.getBytes(StandardCharsets.UTF_8);
	static final byte[] KEY_1_BYTES = KEY_1.getBytes(StandardCharsets.UTF_8);
	static final byte[] KEY_2_BYTES = KEY_2.getBytes(StandardCharsets.UTF_8);
	static final byte[] KEY_3_BYTES = KEY_3.getBytes(StandardCharsets.UTF_8);
	static final byte[] VALUE_1_BYTES = VALUE_1.getBytes(StandardCharsets.UTF_8);
	static final byte[] VALUE_2_BYTES = VALUE_2.getBytes(StandardCharsets.UTF_8);
	static final byte[] VALUE_3_BYTES = VALUE_3.getBytes(StandardCharsets.UTF_8);

	static final ByteBuffer KEY_1_BBUFFER = ByteBuffer.wrap(KEY_1_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_1_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_1_BYTES);
	static final ByteBuffer VALUE_1_BBUFFER = ByteBuffer.wrap(VALUE_1_BYTES);

	static final ByteBuffer KEY_2_BBUFFER = ByteBuffer.wrap(KEY_2_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_2_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_2_BYTES);
	static final ByteBuffer VALUE_2_BBUFFER = ByteBuffer.wrap(VALUE_2_BYTES);

	static final ByteBuffer KEY_3_BBUFFER = ByteBuffer.wrap(KEY_3_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_3_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_3_BYTES);
	static final ByteBuffer VALUE_3_BBUFFER = ByteBuffer.wrap(VALUE_3_BYTES);

	public final LettuceConnectionProvider connectionProvider;
	public final LettuceConnectionProvider nativeConnectionProvider;
	public final LettuceConnectionProvider nativeBinaryConnectionProvider;

	LettuceReactiveValkeyConnection connection;
	ValkeyClusterCommands<String, String> nativeCommands;
	ValkeyClusterCommands<ByteBuffer, ByteBuffer> nativeBinaryCommands;

	public LettuceReactiveCommandsTestSupport(Fixture fixture) {
		this.connectionProvider = fixture.connectionProvider;
		this.nativeConnectionProvider = fixture.nativeConnectionProvider;
		this.nativeBinaryConnectionProvider = fixture.nativeBinaryConnectionProvider;
	}

	public static List<Fixture> parameters() {

		LettuceExtension extension = new LettuceExtension();

		List<Fixture> parameters = new ArrayList<>();

		StandaloneConnectionProvider standaloneProvider = new StandaloneConnectionProvider(
				extension.getInstance(ValkeyClient.class), LettuceReactiveValkeyConnection.CODEC);
		StandaloneConnectionProvider nativeConnectionProvider = new StandaloneConnectionProvider(
				extension.getInstance(ValkeyClient.class), StringCodec.UTF8);
		StandaloneConnectionProvider nativeBinaryConnectionProvider = new StandaloneConnectionProvider(
				extension.getInstance(ValkeyClient.class), ByteBufferCodec.INSTANCE);

		/*parameters
				.add(new Fixture(standaloneProvider, nativeConnectionProvider, nativeBinaryConnectionProvider, "Standalone"));*/
		LettucePoolingClientConfiguration poolingClientConfiguration = LettucePoolingClientConfiguration.builder()
				.shutdownQuietPeriod(Duration.ZERO).shutdownTimeout(Duration.ZERO).build();
		parameters.add(new Fixture(new LettucePoolingConnectionProvider(standaloneProvider, poolingClientConfiguration),
				new LettucePoolingConnectionProvider(nativeConnectionProvider, poolingClientConfiguration),
				new LettucePoolingConnectionProvider(nativeBinaryConnectionProvider, poolingClientConfiguration), "Pooling"));

		ClusterConnectionProvider clusterProvider = new ClusterConnectionProvider(
				extension.getInstance(ValkeyClusterClient.class), LettuceReactiveValkeyConnection.CODEC);
		ClusterConnectionProvider nativeClusterConnectionProvider = new ClusterConnectionProvider(
				extension.getInstance(ValkeyClusterClient.class), StringCodec.UTF8);
		ClusterConnectionProvider nativeBinaryClusterConnectionProvider = new ClusterConnectionProvider(
				extension.getInstance(ValkeyClusterClient.class), ByteBufferCodec.INSTANCE);

		parameters.add(new Fixture(new LettucePoolingConnectionProvider(clusterProvider, poolingClientConfiguration),
				new LettucePoolingConnectionProvider(nativeClusterConnectionProvider, poolingClientConfiguration),
				new LettucePoolingConnectionProvider(nativeBinaryClusterConnectionProvider, poolingClientConfiguration),
				"Cluster"));

		return parameters;
	}

	static class Fixture implements Closeable {

		final LettuceConnectionProvider connectionProvider;
		final LettuceConnectionProvider nativeConnectionProvider;
		final LettuceConnectionProvider nativeBinaryConnectionProvider;
		final String label;

		Fixture(LettuceConnectionProvider connectionProvider, LettuceConnectionProvider nativeConnectionProvider,
				LettuceConnectionProvider nativeBinaryConnectionProvider, String label) {
			this.connectionProvider = connectionProvider;
			this.nativeConnectionProvider = nativeConnectionProvider;
			this.nativeBinaryConnectionProvider = nativeBinaryConnectionProvider;
			this.label = label;
		}

		@Override
		public String toString() {
			return label;
		}

		@Override
		public void close() throws IOException {

			try {
				if (connectionProvider instanceof DisposableBean disposableBean) {
					disposableBean.destroy();
				}

				if (nativeConnectionProvider instanceof DisposableBean disposableBean) {
					disposableBean.destroy();
				}

				if (nativeBinaryConnectionProvider instanceof DisposableBean disposableBean) {
					disposableBean.destroy();
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	@BeforeEach
	public void setUp() {

		AbstractValkeyClient redisClient = ((ValkeyClientProvider) nativeConnectionProvider).getValkeyClient();

		if (redisClient instanceof ValkeyClient) {

			nativeCommands = nativeConnectionProvider.getConnection(StatefulValkeyConnection.class).sync();
			nativeBinaryCommands = nativeBinaryConnectionProvider.getConnection(StatefulValkeyConnection.class).sync();
			this.connection = new LettuceReactiveValkeyConnection(connectionProvider);
		} else {

			Assumptions.assumeThat(ValkeyDetector.isClusterAvailable()).isTrue();

			nativeCommands = nativeConnectionProvider.getConnection(StatefulValkeyClusterConnection.class).sync();
			nativeBinaryCommands = nativeBinaryConnectionProvider.getConnection(StatefulValkeyClusterConnection.class).sync();
			this.connection = new LettuceReactiveValkeyClusterConnection(connectionProvider, (ValkeyClusterClient) redisClient);
		}
	}

	@AfterEach
	public void tearDown() {

		if (nativeCommands != null) {
			flushAll();

			if (nativeCommands instanceof ValkeyCommands redisCommands) {
				nativeConnectionProvider.release((redisCommands).getStatefulConnection());
			}

			if (nativeCommands instanceof ValkeyAdvancedClusterCommands redisAdvancedClusterCommands) {
				nativeConnectionProvider.release((redisAdvancedClusterCommands).getStatefulConnection());
			}

			if (nativeBinaryCommands instanceof ValkeyCommands redisCommands) {
				nativeBinaryConnectionProvider.release((redisCommands).getStatefulConnection());
			}

			if (nativeBinaryCommands instanceof ValkeyAdvancedClusterCommands redisAdvancedClusterCommands) {
				nativeBinaryConnectionProvider
						.release((redisAdvancedClusterCommands).getStatefulConnection());
			}
		}

		if (connection != null) {
			connection.close();
		}
	}

	private void flushAll() {
		nativeCommands.flushall();
	}

}
