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
package org.springframework.data.valkey.connection.lettuce;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.ValkeyFuture;
import io.lettuce.core.api.async.ValkeyKeyAsyncCommands;
import io.lettuce.core.api.async.ValkeyServerAsyncCommands;
import io.lettuce.core.cluster.api.sync.ValkeyClusterCommands;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.data.valkey.connection.ValkeyNode;
import org.springframework.data.valkey.connection.ValkeyServerCommands;
import org.springframework.data.valkey.core.types.ValkeyClientInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @author Dennis Neufeld
 * @since 2.0
 */
class LettuceServerCommands implements ValkeyServerCommands {

	private final LettuceConnection connection;

	LettuceServerCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public void bgReWriteAof() {
		connection.invokeStatus().just(ValkeyServerAsyncCommands::bgrewriteaof);
	}

	@Override
	public void bgSave() {
		connection.invokeStatus().just(ValkeyServerAsyncCommands::bgsave);
	}

	@Override
	public Long lastSave() {
		return connection.invoke().from(ValkeyServerAsyncCommands::lastsave).get(LettuceConverters::toLong);
	}

	@Override
	public void save() {
		connection.invokeStatus().just(ValkeyServerAsyncCommands::save);
	}

	@Override
	public Long dbSize() {
		return connection.invoke().just(ValkeyServerAsyncCommands::dbsize);
	}

	@Override
	public void flushDb() {
		connection.invokeStatus().just(ValkeyServerAsyncCommands::flushdb);
	}

	@Override
	public void flushDb(FlushOption option) {
		connection.invokeStatus().just(ValkeyServerAsyncCommands::flushdb, LettuceConverters.toFlushMode(option));
	}

	@Override
	public void flushAll() {
		connection.invokeStatus().just(ValkeyServerAsyncCommands::flushall);
	}

	@Override
	public void flushAll(FlushOption option) {
		connection.invokeStatus().just(ValkeyServerAsyncCommands::flushall, LettuceConverters.toFlushMode(option));
	}

	@Override
	public Properties info() {
		return connection.invoke().from(ValkeyServerAsyncCommands::info).get(LettuceConverters.stringToProps());
	}

	@Override
	public Properties info(String section) {

		Assert.hasText(section, "Section must not be null or empty");

		return connection.invoke().from(ValkeyServerAsyncCommands::info, section).get(LettuceConverters.stringToProps());
	}

	@Override
	public void shutdown() {
		connection.invokeStatus().just(it -> {

			it.shutdown(true);

			return new CompletedValkeyFuture<>(null);
		});
	}

	@Override
	public void shutdown(ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		boolean save = ShutdownOption.SAVE.equals(option);

		connection.invokeStatus().just(it -> {

			it.shutdown(save);

			return new CompletedValkeyFuture<>(null);
		});
	}

	@Override
	public Properties getConfig(String pattern) {

		Assert.hasText(pattern, "Pattern must not be null or empty");

		return connection.invoke().from(ValkeyServerAsyncCommands::configGet, pattern)
				.get(LettuceConverters.mapToPropertiesConverter());
	}

	@Override
	public void setConfig(String param, String value) {

		Assert.hasText(param, "Parameter must not be null or empty");
		Assert.notNull(value, "Value must not be null");

		connection.invokeStatus().just(ValkeyServerAsyncCommands::configSet, param, value);
	}

	@Override
	public void resetConfigStats() {
		connection.invokeStatus().just(ValkeyServerAsyncCommands::configResetstat);
	}

	@Override
	public void rewriteConfig() {
		connection.invokeStatus().just(ValkeyServerAsyncCommands::configRewrite);
	}

	@Override
	public Long time(TimeUnit timeUnit) {

		Assert.notNull(timeUnit, "TimeUnit must not be null");

		return connection.invoke().from(ValkeyServerAsyncCommands::time).get(LettuceConverters.toTimeConverter(timeUnit));
	}

	@Override
	public void killClient(String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'");

		String client = "%s:%d".formatted(host, port);

		connection.invoke().just(ValkeyServerAsyncCommands::clientKill, client);
	}

	@Override
	public void setClientName(byte[] name) {

		Assert.notNull(name, "Name must not be null");

		connection.invoke().just(ValkeyServerAsyncCommands::clientSetname, name);
	}

	@Override
	public String getClientName() {
		return connection.invoke().from(ValkeyServerAsyncCommands::clientGetname).get(LettuceConverters::toString);
	}

	@Override
	public List<ValkeyClientInfo> getClientList() {
		return connection.invoke().from(ValkeyServerAsyncCommands::clientList)
				.get(LettuceConverters.stringToValkeyClientListConverter());
	}

	@Override
	public void replicaOf(String host, int port) {

		Assert.hasText(host, "Host must not be null for 'REPLICAOF' command");

		connection.invoke().just(ValkeyServerAsyncCommands::slaveof, host, port);
	}

	@Override
	public void replicaOfNoOne() {
		connection.invoke().just(ValkeyServerAsyncCommands::slaveofNoOne);
	}

	@Override
	public void migrate(byte[] key, ValkeyNode target, int dbIndex, @Nullable MigrateOption option) {
		migrate(key, target, dbIndex, option, Long.MAX_VALUE);
	}

	@Override
	public void migrate(byte[] key, ValkeyNode target, int dbIndex, @Nullable MigrateOption option, long timeout) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(target, "Target node must not be null");

		connection.invoke().just(ValkeyKeyAsyncCommands::migrate, target.getHost(), target.getPort(), key, dbIndex, timeout);
	}

	public ValkeyClusterCommands<byte[], byte[]> getConnection() {
		return connection.getConnection();
	}

	static class CompletedValkeyFuture<T> extends CompletableFuture<T> implements ValkeyFuture<T> {

		public CompletedValkeyFuture(T value) {
			complete(value);
		}

		@Override
		public String getError() {
			return "";
		}

		@Override
		public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
			return LettuceFutures.awaitAll(timeout, unit, this);
		}
	}
}
