/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.valkey.test.condition;

import io.lettuce.core.api.StatefulValkeyConnection;
import io.lettuce.core.api.sync.ValkeyCommands;
import io.lettuce.core.cluster.api.StatefulValkeyClusterConnection;
import io.lettuce.core.cluster.api.sync.ValkeyClusterCommands;
import io.lettuce.core.models.command.CommandDetail;
import io.lettuce.core.models.command.CommandDetailParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.springframework.data.util.Version;

/**
 * Collection of utility methods to test conditions during test execution.
 *
 * @author Mark Paluch
 */
class ValkeyConditions {

	private final Map<String, Integer> commands;
	private final Version version;

	private ValkeyConditions(ValkeyClusterCommands<String, String> commands) {

		List<CommandDetail> result = CommandDetailParser.parse(commands.command());

		this.commands = result.stream()
				.collect(Collectors.toMap(commandDetail -> commandDetail.getName().toUpperCase(), CommandDetail::getArity));

		String info = commands.info("server");

		try {

			ByteArrayInputStream inputStream = new ByteArrayInputStream(info.getBytes());
			Properties p = new Properties();
			p.load(inputStream);

			version = Version.parse(p.getProperty("redis_version"));
		} catch (IOException ex) {
			throw new IllegalStateException(ex);
		}
	}

	/**
	 * Create {@link ValkeyCommands} given {@link StatefulValkeyConnection}.
	 *
	 * @param connection must not be {@literal null}.
	 * @return
	 */
	public static ValkeyConditions of(StatefulValkeyConnection<String, String> connection) {
		return new ValkeyConditions(connection.sync());
	}

	/**
	 * Create {@link ValkeyCommands} given {@link StatefulValkeyClusterConnection}.
	 *
	 * @param connection must not be {@literal null}.
	 * @return
	 */
	public static ValkeyConditions of(StatefulValkeyClusterConnection<String, String> connection) {
		return new ValkeyConditions(connection.sync());
	}

	/**
	 * Create {@link ValkeyConditions} given {@link ValkeyCommands}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	public static ValkeyConditions of(ValkeyClusterCommands<String, String> commands) {
		return new ValkeyConditions(commands);
	}

	/**
	 * @return the Valkey {@link Version}.
	 */
	public Version getValkeyVersion() {
		return version;
	}

	/**
	 * @param command
	 * @return {@code true} if the command is present.
	 */
	public boolean hasCommand(String command) {
		return commands.containsKey(command.toUpperCase());
	}

	/**
	 * @param command command name.
	 * @param arity expected arity.
	 * @return {@code true} if the command is present with the given arity.
	 */
	public boolean hasCommandArity(String command, int arity) {

		if (!hasCommand(command)) {
			throw new IllegalStateException("Unknown command: " + command + " in " + commands);
		}

		return commands.get(command.toUpperCase()) == arity;
	}

	/**
	 * @param versionNumber
	 * @return {@code true} if the version number is met.
	 */
	public boolean hasVersionGreaterOrEqualsTo(String versionNumber) {
		return version.isGreaterThanOrEqualTo(Version.parse(versionNumber));
	}

}