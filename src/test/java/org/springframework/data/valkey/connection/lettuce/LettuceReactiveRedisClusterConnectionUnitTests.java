/*
 * Copyright 2018-2024 the original author or authors.
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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.springframework.data.valkey.connection.ClusterTestVariables.*;

import io.lettuce.core.api.StatefulValkeyConnection;
import io.lettuce.core.api.reactive.ValkeyReactiveCommands;
import io.lettuce.core.cluster.ValkeyClusterClient;
import io.lettuce.core.cluster.api.StatefulValkeyClusterConnection;
import io.lettuce.core.cluster.api.reactive.ValkeyAdvancedClusterReactiveCommands;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.valkey.connection.ValkeyClusterNode;
import org.springframework.data.valkey.connection.lettuce.LettuceConnectionProvider.TargetAware;

/**
 * Unit tests for {@link LettuceReactiveValkeyClusterConnection}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class LettuceReactiveValkeyClusterConnectionUnitTests {

	static final ValkeyClusterNode NODE1 = new ValkeyClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT);

	@Mock StatefulValkeyClusterConnection<ByteBuffer, ByteBuffer> sharedConnection;
	@Mock StatefulValkeyConnection<ByteBuffer, ByteBuffer> nodeConnection;

	@Mock ValkeyClusterClient clusterClient;
	@Mock ValkeyAdvancedClusterReactiveCommands<ByteBuffer, ByteBuffer> reactiveCommands;
	@Mock ValkeyReactiveCommands<ByteBuffer, ByteBuffer> reactiveNodeCommands;
	@Mock(extraInterfaces = TargetAware.class) LettuceConnectionProvider connectionProvider;

	@BeforeEach
	public void before() {

		when(connectionProvider.getConnectionAsync(any())).thenReturn(CompletableFuture.completedFuture(sharedConnection));
		when(sharedConnection.getConnectionAsync(anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(nodeConnection));
		when(nodeConnection.reactive()).thenReturn(reactiveNodeCommands);
	}

	@Test // DATAREDIS-659, DATAREDIS-708
	public void bgReWriteAofShouldRespondCorrectly() {

		LettuceReactiveValkeyClusterConnection connection = new LettuceReactiveValkeyClusterConnection(connectionProvider,
				clusterClient);

		when(reactiveNodeCommands.bgrewriteaof()).thenReturn(Mono.just("OK"));

		connection.serverCommands().bgReWriteAof(NODE1).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659, DATAREDIS-708
	public void bgSaveShouldRespondCorrectly() {

		LettuceReactiveValkeyClusterConnection connection = new LettuceReactiveValkeyClusterConnection(connectionProvider,
				clusterClient);

		when(reactiveNodeCommands.bgsave()).thenReturn(Mono.just("OK"));

		connection.serverCommands().bgSave(NODE1).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}
}
