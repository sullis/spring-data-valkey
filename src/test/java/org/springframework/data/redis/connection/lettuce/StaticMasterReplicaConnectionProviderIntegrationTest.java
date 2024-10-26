/*
 * Copyright 2022-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.ValkeyClient;
import io.lettuce.core.ValkeyURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.masterreplica.StatefulValkeyMasterReplicaConnection;

import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.test.extension.LettuceExtension;

/**
 * Integration test for {@link StaticMasterReplicaConnectionProvider}.
 *
 * @author Mark Paluch
 */
@ExtendWith(LettuceExtension.class)
class StaticMasterReplicaConnectionProviderIntegrationTest {

	ValkeyURI uri = ValkeyURI.create(SettingsUtils.getHost(), SettingsUtils.getPort());

	@Test
	void shouldConnectToMasterReplicaSynchronously(ValkeyClient redisClient) {

		StaticMasterReplicaConnectionProvider connectionProvider = new StaticMasterReplicaConnectionProvider(redisClient,
				ByteArrayCodec.INSTANCE, Collections.singletonList(uri), ReadFrom.REPLICA);

		StatefulValkeyMasterReplicaConnection<?, ?> connection = connectionProvider
				.getConnection(StatefulValkeyMasterReplicaConnection.class);

		assertThat(connection.getReadFrom()).isEqualTo(ReadFrom.REPLICA);

		connectionProvider.release(connection);
	}

	@Test
	@SuppressWarnings("rawtypes")
	void shouldConnectToMasterReplicaAsync(ValkeyClient redisClient)
			throws ExecutionException, InterruptedException, TimeoutException {

		StaticMasterReplicaConnectionProvider connectionProvider = new StaticMasterReplicaConnectionProvider(redisClient,
				ByteArrayCodec.INSTANCE, Collections.singletonList(uri), ReadFrom.REPLICA);

		CompletionStage<StatefulValkeyMasterReplicaConnection> future = connectionProvider
				.getConnectionAsync(StatefulValkeyMasterReplicaConnection.class);

		StatefulValkeyMasterReplicaConnection<?, ?> connection = future.toCompletableFuture().get(5, TimeUnit.SECONDS);
		assertThat(connection.getReadFrom()).isEqualTo(ReadFrom.REPLICA);

		connectionProvider.release(connection);
	}
}
