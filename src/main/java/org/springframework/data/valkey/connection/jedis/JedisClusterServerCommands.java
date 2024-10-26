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
package org.springframework.data.valkey.connection.jedis;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.valkey.connection.ClusterCommandExecutor.MultiNodeResult;
import org.springframework.data.valkey.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.valkey.connection.ValkeyClusterNode;
import org.springframework.data.valkey.connection.ValkeyClusterServerCommands;
import org.springframework.data.valkey.connection.ValkeyNode;
import org.springframework.data.valkey.connection.convert.Converters;
import org.springframework.data.valkey.connection.jedis.JedisClusterConnection.JedisClusterCommandCallback;
import org.springframework.data.valkey.core.types.ValkeyClientInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Mark Paluch
 * @author Dennis Neufeld
 * @since 2.0
 */
class JedisClusterServerCommands implements ValkeyClusterServerCommands {

	private final JedisClusterConnection connection;

	JedisClusterServerCommands(JedisClusterConnection connection) {
		this.connection = connection;
	}

	@Override
	public void bgReWriteAof(ValkeyClusterNode node) {
		executeCommandOnSingleNode(Jedis::bgrewriteaof, node);
	}

	@Override
	public void bgReWriteAof() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::bgrewriteaof);
	}

	@Override
	public void bgSave() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::bgsave);
	}

	@Override
	public void bgSave(ValkeyClusterNode node) {
		executeCommandOnSingleNode(Jedis::bgsave, node);
	}

	@Override
	public Long lastSave() {

		List<Long> result = new ArrayList<>(executeCommandOnAllNodes(Jedis::lastsave).resultsAsList());

		if (CollectionUtils.isEmpty(result)) {
			return null;
		}

		Collections.sort(result, Collections.reverseOrder());
		return result.get(0);
	}

	@Override
	public Long lastSave(ValkeyClusterNode node) {
		return executeCommandOnSingleNode(Jedis::lastsave, node).getValue();
	}

	@Override
	public void save() {
		executeCommandOnAllNodes(Jedis::save);
	}

	@Override
	public void save(ValkeyClusterNode node) {
		executeCommandOnSingleNode(Jedis::save, node);
	}

	@Override
	public Long dbSize() {

		Collection<Long> dbSizes = executeCommandOnAllNodes(Jedis::dbSize).resultsAsList();

		if (CollectionUtils.isEmpty(dbSizes)) {
			return 0L;
		}

		Long size = 0L;
		for (Long value : dbSizes) {
			size += value;
		}
		return size;
	}

	@Override
	public Long dbSize(ValkeyClusterNode node) {
		return executeCommandOnSingleNode(Jedis::dbSize, node).getValue();
	}

	@Override
	public void flushDb() {
		executeCommandOnAllNodes(Jedis::flushDB);
	}

	@Override
	public void flushDb(FlushOption option) {
		executeCommandOnAllNodes(it -> it.flushDB(JedisConverters.toFlushMode(option)));
	}

	@Override
	public void flushDb(ValkeyClusterNode node) {
		executeCommandOnSingleNode(Jedis::flushDB, node);
	}

	@Override
	public void flushDb(ValkeyClusterNode node, FlushOption option) {
		executeCommandOnSingleNode(it -> it.flushDB(JedisConverters.toFlushMode(option)), node);
	}

	@Override
	public void flushAll() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::flushAll);
	}

	@Override
	public void flushAll(FlushOption option) {
		connection.getClusterCommandExecutor().executeCommandOnAllNodes(
				(JedisClusterCommandCallback<String>) it -> it.flushAll(JedisConverters.toFlushMode(option)));
	}

	@Override
	public void flushAll(ValkeyClusterNode node) {
		executeCommandOnSingleNode(Jedis::flushAll, node);
	}

	@Override
	public void flushAll(ValkeyClusterNode node, FlushOption option) {
		executeCommandOnSingleNode(it -> it.flushAll(JedisConverters.toFlushMode(option)), node);
	}

	@Override
	public Properties info() {

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(
						(JedisClusterCommandCallback<Properties>) client -> JedisConverters.toProperties(client.info()))
				.getResults();

		for (NodeResult<Properties> nodeProperties : nodeResults) {
			for (Entry<Object, Object> entry : nodeProperties.getValue().entrySet()) {
				infos.put(nodeProperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	@Override
	public Properties info(ValkeyClusterNode node) {
		return JedisConverters.toProperties(executeCommandOnSingleNode(Jedis::info, node).getValue());
	}

	@Override
	public Properties info(String section) {

		Assert.notNull(section, "Section must not be null");

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(
						(JedisClusterCommandCallback<Properties>) client -> JedisConverters.toProperties(client.info(section)))
				.getResults();

		for (NodeResult<Properties> nodeProperties : nodeResults) {
			for (Entry<Object, Object> entry : nodeProperties.getValue().entrySet()) {
				infos.put(nodeProperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	@Override
	public Properties info(ValkeyClusterNode node, String section) {

		Assert.notNull(section, "Section must not be null");

		return JedisConverters.toProperties(executeCommandOnSingleNode(client -> client.info(section), node).getValue());
	}

	@Override
	public void shutdown() {
		connection.getClusterCommandExecutor().executeCommandOnAllNodes((JedisClusterCommandCallback<String>) jedis -> {
			jedis.shutdown();
			return null;
		});
	}

	@Override
	public void shutdown(ValkeyClusterNode node) {
		executeCommandOnSingleNode(jedis -> {
			jedis.shutdown();
			return null;
		}, node);
	}

	@Override
	public void shutdown(ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		throw new IllegalArgumentException("Shutdown with options is not supported for jedis");
	}

	@Override
	public Properties getConfig(String pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		JedisClusterCommandCallback<Map<String, String>> command = jedis -> jedis.configGet(pattern);

		List<NodeResult<Map<String, String>>> nodeResults = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(command)
				.getResults();

		Properties nodesConfiguration = new Properties();

		for (NodeResult<Map<String, String>> nodeResult : nodeResults) {

			String prefix = nodeResult.getNode().asString();

			for (Entry<String, String> entry : nodeResult.getValue().entrySet()) {
				String newKey = prefix.concat(".").concat(entry.getKey());
				String value = entry.getValue();
				nodesConfiguration.setProperty(newKey, value);
			}
		}

		return nodesConfiguration;
	}

	@Override
	public Properties getConfig(ValkeyClusterNode node, String pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode(
						(JedisClusterCommandCallback<Properties>) client -> Converters.toProperties(client.configGet(pattern)),
						node)
				.getValue();
	}

	@Override
	public void setConfig(String param, String value) {

		Assert.notNull(param, "Parameter must not be null");
		Assert.notNull(value, "Value must not be null");

		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) client -> client.configSet(param, value));
	}

	@Override
	public void setConfig(ValkeyClusterNode node, String param, String value) {

		Assert.notNull(param, "Parameter must not be null");
		Assert.notNull(value, "Value must not be null");

		executeCommandOnSingleNode(client -> client.configSet(param, value), node);
	}

	@Override
	public void resetConfigStats() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::configResetStat);
	}

	@Override
	public void rewriteConfig() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::configRewrite);
	}

	@Override
	public void resetConfigStats(ValkeyClusterNode node) {
		executeCommandOnSingleNode(Jedis::configResetStat, node);
	}

	@Override
	public void rewriteConfig(ValkeyClusterNode node) {
		executeCommandOnSingleNode(Jedis::configRewrite, node);
	}

	@Override
	public Long time(TimeUnit timeUnit) {

		return convertListOfStringToTime(
				connection.getClusterCommandExecutor()
						.executeCommandOnArbitraryNode((JedisClusterCommandCallback<List<String>>) Jedis::time).getValue(),
				timeUnit);
	}

	@Override
	public Long time(ValkeyClusterNode node, TimeUnit timeUnit) {

		return convertListOfStringToTime(
				connection.getClusterCommandExecutor()
						.executeCommandOnSingleNode((JedisClusterCommandCallback<List<String>>) Jedis::time, node).getValue(),
				timeUnit);
	}

	@Override
	public void killClient(String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'");
		String hostAndPort = "%s:%d".formatted(host, port);

		JedisClusterCommandCallback<String> command = client -> client.clientKill(hostAndPort);

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(command);
	}

	@Override
	public void setClientName(byte[] name) {
		throw new InvalidDataAccessApiUsageException("CLIENT SETNAME is not supported in cluster environment");
	}

	@Override
	public String getClientName() {
		throw new InvalidDataAccessApiUsageException("CLIENT GETNAME is not supported in cluster environment");
	}

	@Override
	public List<ValkeyClientInfo> getClientList() {

		Collection<String> map = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::clientList).resultsAsList();

		ArrayList<ValkeyClientInfo> result = new ArrayList<>();
		for (String infos : map) {
			result.addAll(JedisConverters.toListOfValkeyClientInformation(infos));
		}
		return result;
	}

	@Override
	public List<ValkeyClientInfo> getClientList(ValkeyClusterNode node) {

		return JedisConverters
				.toListOfValkeyClientInformation(executeCommandOnSingleNode(Jedis::clientList, node).getValue());
	}

	@Override
	public void replicaOf(String host, int port) {
		throw new InvalidDataAccessApiUsageException(
				"REPLICAOF is not supported in cluster environment; Please use CLUSTER REPLICATE");
	}

	@Override
	public void replicaOfNoOne() {
		throw new InvalidDataAccessApiUsageException(
				"REPLICAOF is not supported in cluster environment; Please use CLUSTER REPLICATE");
	}

	@Override
	public void migrate(byte[] key, ValkeyNode target, int dbIndex, @Nullable MigrateOption option) {
		migrate(key, target, dbIndex, option, Long.MAX_VALUE);
	}

	@Override
	public void migrate(byte[] key, ValkeyNode target, int dbIndex, @Nullable MigrateOption option, long timeout) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(target, "Target node must not be null");
		int timeoutToUse = timeout <= Integer.MAX_VALUE ? (int) timeout : Integer.MAX_VALUE;

		ValkeyClusterNode node = connection.getTopologyProvider().getTopology().lookup(target.getHost(), target.getPort());

		executeCommandOnSingleNode(client -> client.migrate(target.getHost(), target.getPort(), key, dbIndex, timeoutToUse),
				node);
	}

	private Long convertListOfStringToTime(List<String> serverTimeInformation, TimeUnit timeUnit) {

		Assert.notEmpty(serverTimeInformation, "Received invalid result from server; Expected 2 items in collection");
		Assert.isTrue(serverTimeInformation.size() == 2,
				"Received invalid number of arguments from redis server; Expected 2 received " + serverTimeInformation.size());

		return Converters.toTimeMillis(serverTimeInformation.get(0), serverTimeInformation.get(1), timeUnit);
	}

	private <T> NodeResult<T> executeCommandOnSingleNode(JedisClusterCommandCallback<T> cmd, ValkeyClusterNode node) {
		return connection.getClusterCommandExecutor().executeCommandOnSingleNode(cmd, node);
	}

	private <T> MultiNodeResult<T> executeCommandOnAllNodes(JedisClusterCommandCallback<T> cmd) {
		return connection.getClusterCommandExecutor().executeCommandOnAllNodes(cmd);
	}

}
