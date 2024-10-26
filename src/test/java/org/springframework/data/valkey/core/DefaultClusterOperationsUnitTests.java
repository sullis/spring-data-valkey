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
package org.springframework.data.valkey.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.valkey.connection.ValkeyClusterCommands.AddSlots;
import org.springframework.data.valkey.connection.ValkeyClusterConnection;
import org.springframework.data.valkey.connection.ValkeyClusterNode;
import org.springframework.data.valkey.connection.ValkeyClusterNode.SlotRange;
import org.springframework.data.valkey.connection.ValkeyConnectionFactory;
import org.springframework.data.valkey.connection.ValkeyServerCommands.FlushOption;
import org.springframework.data.valkey.connection.ValkeyServerCommands.MigrateOption;
import org.springframework.data.valkey.serializer.ValkeySerializer;
import org.springframework.data.valkey.serializer.StringValkeySerializer;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Dennis Neufeld
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultClusterOperationsUnitTests {

	private static final ValkeyClusterNode NODE_1 = ValkeyClusterNode.newValkeyClusterNode().listeningAt("127.0.0.1", 6379)
			.withId("d1861060fe6a534d42d8a19aeb36600e18785e04").build();

	private static final ValkeyClusterNode NODE_2 = ValkeyClusterNode.newValkeyClusterNode().listeningAt("127.0.0.1", 6380)
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84").build();

	@Mock ValkeyConnectionFactory connectionFactory;
	@Mock ValkeyClusterConnection connection;

	private ValkeySerializer<String> serializer;

	private DefaultClusterOperations<String, String> clusterOps;

	@BeforeEach
	void setUp() {

		when(connectionFactory.getConnection()).thenReturn(connection);
		when(connectionFactory.getClusterConnection()).thenReturn(connection);

		serializer = StringValkeySerializer.UTF_8;

		ValkeyTemplate<String, String> template = new ValkeyTemplate<>();
		template.setConnectionFactory(connectionFactory);
		template.setValueSerializer(serializer);
		template.setKeySerializer(serializer);
		template.afterPropertiesSet();

		this.clusterOps = new DefaultClusterOperations<>(template);
	}

	@Test // DATAREDIS-315
	void keysShouldDelegateToConnectionCorrectly() {

		Set<byte[]> keys = new HashSet<>(Arrays.asList(serializer.serialize("key-1"), serializer.serialize("key-2")));
		when(connection.keys(any(ValkeyClusterNode.class), any(byte[].class))).thenReturn(keys);

		assertThat(clusterOps.keys(NODE_1, "*")).contains("key-1", "key-2");
	}

	@Test // DATAREDIS-315
	void keysShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.keys(null, "*"));
	}

	@Test // DATAREDIS-315
	void keysShouldReturnEmptySetWhenNoKeysAvailable() {

		when(connection.keys(any(ValkeyClusterNode.class), any(byte[].class))).thenReturn(null);

		assertThat(clusterOps.keys(NODE_1, "*")).isNotNull();
	}

	@Test // DATAREDIS-315
	void randomKeyShouldDelegateToConnection() {

		when(connection.randomKey(any(ValkeyClusterNode.class))).thenReturn(serializer.serialize("key-1"));

		assertThat(clusterOps.randomKey(NODE_1)).isEqualTo("key-1");
	}

	@Test // DATAREDIS-315
	void randomKeyShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.randomKey(null));
	}

	@Test // DATAREDIS-315
	void randomKeyShouldReturnNullWhenNoKeyAvailable() {

		when(connection.randomKey(any(ValkeyClusterNode.class))).thenReturn(null);

		assertThat(clusterOps.randomKey(NODE_1)).isNull();
	}

	@Test // DATAREDIS-315
	void pingShouldDelegateToConnection() {

		when(connection.ping(any(ValkeyClusterNode.class))).thenReturn("PONG");

		assertThat(clusterOps.ping(NODE_1)).isEqualTo("PONG");
	}

	@Test // DATAREDIS-315
	void pingShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.ping(null));
	}

	@Test // DATAREDIS-315
	void addSlotsShouldDelegateToConnection() {

		clusterOps.addSlots(NODE_1, 1, 2, 3);

		verify(connection, times(1)).clusterAddSlots(eq(NODE_1), any(int[].class));
	}

	@Test // DATAREDIS-315
	void addSlotsShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.addSlots(null));
	}

	@Test // DATAREDIS-315
	void addSlotsWithRangeShouldDelegateToConnection() {

		clusterOps.addSlots(NODE_1, new SlotRange(1, 3));

		verify(connection, times(1)).clusterAddSlots(eq(NODE_1), any(int[].class));
	}

	@Test // DATAREDIS-315
	void addSlotsWithRangeShouldThrowExceptionWhenRangeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.addSlots(NODE_1, (SlotRange) null));
	}

	@Test // DATAREDIS-315
	void bgSaveShouldDelegateToConnection() {

		clusterOps.bgSave(NODE_1);

		verify(connection, times(1)).bgSave(eq(NODE_1));
	}

	@Test // DATAREDIS-315
	void bgSaveShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.bgSave(null));
	}

	@Test // DATAREDIS-315
	void meetShouldDelegateToConnection() {

		clusterOps.meet(NODE_1);

		verify(connection, times(1)).clusterMeet(eq(NODE_1));
	}

	@Test // DATAREDIS-315
	void meetShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.meet(null));
	}

	@Test // DATAREDIS-315
	void forgetShouldDelegateToConnection() {

		clusterOps.forget(NODE_1);

		verify(connection, times(1)).clusterForget(eq(NODE_1));
	}

	@Test // DATAREDIS-315
	void forgetShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.forget(null));
	}

	@Test // DATAREDIS-315
	void flushDbShouldDelegateToConnection() {

		clusterOps.flushDb(NODE_1);

		verify(connection, times(1)).flushDb(eq(NODE_1));
	}

	@Test // GH-2187
	void flushDbSyncShouldDelegateToConnection() {

		clusterOps.flushDb(NODE_1, FlushOption.SYNC);

		verify(connection, times(1)).flushDb(eq(NODE_1), eq(FlushOption.SYNC));
	}

	@Test // GH-2187
	void flushDbAsyncShouldDelegateToConnection() {

		clusterOps.flushDb(NODE_1, FlushOption.ASYNC);

		verify(connection, times(1)).flushDb(eq(NODE_1), eq(FlushOption.ASYNC));
	}

	@Test // DATAREDIS-315
	void flushDbShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.flushDb(null));
	}

	@Test // DATAREDIS-315
	void getReplicasShouldDelegateToConnection() {

		clusterOps.getReplicas(NODE_1);

		verify(connection, times(1)).clusterGetReplicas(eq(NODE_1));
	}

	@Test // DATAREDIS-315
	void getReplicasShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.getReplicas(null));
	}

	@Test // DATAREDIS-315
	void saveShouldDelegateToConnection() {

		clusterOps.save(NODE_1);

		verify(connection, times(1)).save(eq(NODE_1));
	}

	@Test // DATAREDIS-315
	void saveShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.save(null));
	}

	@Test // DATAREDIS-315
	void shutdownShouldDelegateToConnection() {

		clusterOps.shutdown(NODE_1);

		verify(connection, times(1)).shutdown(eq(NODE_1));
	}

	@Test // DATAREDIS-315
	void shutdownShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.shutdown(null));
	}

	@Test // DATAREDIS-315
	void executeShouldDelegateToConnection() {

		final byte[] key = serializer.serialize("foo");
		clusterOps.execute(connection -> serializer.deserialize(connection.get(key)));

		verify(connection, times(1)).get(eq(key));
	}

	@Test // DATAREDIS-315
	void executeShouldThrowExceptionWhenCallbackIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> clusterOps.execute(null));
	}

	@Test // DATAREDIS-315
	void reshardShouldExecuteCommandsCorrectly() {

		byte[] key = "foo".getBytes();
		when(connection.clusterGetKeysInSlot(eq(100), anyInt())).thenReturn(Collections.singletonList(key));
		clusterOps.reshard(NODE_1, 100, NODE_2);

		verify(connection, times(1)).clusterSetSlot(eq(NODE_2), eq(100), eq(AddSlots.IMPORTING));
		verify(connection, times(1)).clusterSetSlot(eq(NODE_1), eq(100), eq(AddSlots.MIGRATING));
		verify(connection, times(1)).clusterGetKeysInSlot(eq(100), anyInt());
		verify(connection, times(1)).migrate(any(byte[].class), eq(NODE_1), eq(0), eq(MigrateOption.COPY));
		verify(connection, times(1)).clusterSetSlot(eq(NODE_2), eq(100), eq(AddSlots.NODE));

	}
}
