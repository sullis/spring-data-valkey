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
package org.springframework.data.valkey.connection.lettuce;

import io.lettuce.core.ValkeyException;
import io.lettuce.core.api.reactive.ValkeyKeyReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.List;

import org.reactivestreams.Publisher;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.valkey.ValkeySystemException;
import org.springframework.data.valkey.connection.ClusterSlotHashUtil;
import org.springframework.data.valkey.connection.ReactiveClusterKeyCommands;
import org.springframework.data.valkey.connection.ReactiveValkeyConnection.BooleanResponse;
import org.springframework.data.valkey.connection.ValkeyClusterNode;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceReactiveClusterKeyCommands extends LettuceReactiveKeyCommands implements ReactiveClusterKeyCommands {

	private LettuceReactiveValkeyClusterConnection connection;

	/**
	 * Create new {@link LettuceReactiveClusterKeyCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveClusterKeyCommands(LettuceReactiveValkeyClusterConnection connection) {

		super(connection);

		this.connection = connection;
	}

	@Override
	public Mono<List<ByteBuffer>> keys(ValkeyClusterNode node, ByteBuffer pattern) {

		return connection.execute(node, cmd -> {

			Assert.notNull(pattern, "Pattern must not be null");

			return cmd.keys(pattern).collectList();
		}).next();
	}

	@Override
	public Mono<ByteBuffer> randomKey(ValkeyClusterNode node) {

		return connection.execute(node, ValkeyKeyReactiveCommands::randomkey).next();
	}

	@Override
	public Flux<BooleanResponse<RenameCommand>> rename(Publisher<RenameCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Old key must not be null");
			Assert.notNull(command.getNewKey(), "New key must not be null");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getNewKey())) {
				return super.rename(Mono.just(command));
			}

			Mono<Boolean> result = cmd.dump(command.getKey())
					.switchIfEmpty(Mono.error(new ValkeySystemException("Cannot rename key that does not exist",
							new ValkeyException("ERR no such key."))))
					.flatMap(value -> cmd.restore(command.getNewKey(), 0, value).flatMap(res -> cmd.del(command.getKey())))
					.map(LettuceConverters.longToBooleanConverter()::convert);

			return result.map(val -> new BooleanResponse<>(command, val));
		}));
	}

	@Override
	public Flux<BooleanResponse<RenameCommand>> renameNX(Publisher<RenameCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getNewKey(), "NewName must not be null");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getNewKey())) {
				return super.renameNX(Mono.just(command));
			}

			Mono<Boolean> result = cmd.exists(command.getNewKey()).flatMap(exists -> {

				if (exists == 1) {
					return Mono.just(Boolean.FALSE);
				}

				return cmd.dump(command.getKey())
						.switchIfEmpty(Mono.error(new ValkeySystemException("Cannot rename key that does not exist",
								new ValkeyException("ERR no such key."))))
						.flatMap(value -> cmd.restore(command.getNewKey(), 0, value).flatMap(res -> cmd.del(command.getKey())))
						.map(LettuceConverters::toBoolean);
			});

			return result.map(val -> new BooleanResponse<>(command, val));
		}));
	}

	@Override
	public Flux<BooleanResponse<MoveCommand>> move(Publisher<MoveCommand> commands) {
		throw new InvalidDataAccessApiUsageException("MOVE not supported in CLUSTER mode");
	}
}