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
package org.springframework.data.valkey.connection;

import org.springframework.data.valkey.connection.ValkeyConfiguration.WithDatabaseIndex;
import org.springframework.data.valkey.connection.ValkeyConfiguration.WithHostAndPort;
import org.springframework.data.valkey.connection.ValkeyConfiguration.WithPassword;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Configuration class used to set up a {@link ValkeyConnection} with {@link ValkeyConnectionFactory} for connecting
 * to a single node <a href="https://redis.io/">Valkey</a> instance.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author John Blum
 * @since 2.0
 */
public class ValkeyStandaloneConfiguration
		implements ValkeyConfiguration, WithHostAndPort, WithPassword, WithDatabaseIndex {

	private static final int DEFAULT_PORT = 6379;

	private static final String DEFAULT_HOST = "localhost";

	private int database;
	private int port = DEFAULT_PORT;

	private ValkeyPassword password = ValkeyPassword.none();

	private String hostName = DEFAULT_HOST;
	private @Nullable String username = null;

	/**
	 * Create a new default {@link ValkeyStandaloneConfiguration}.
	 */
	public ValkeyStandaloneConfiguration() {}

	/**
	 * Create a new {@link ValkeyStandaloneConfiguration} given {@code hostName}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 */
	public ValkeyStandaloneConfiguration(String hostName) {
		this(hostName, DEFAULT_PORT);
	}

	/**
	 * Create a new {@link ValkeyStandaloneConfiguration} given {@code hostName} and {@code port}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 * @param port a valid TCP port (1-65535).
	 */
	public ValkeyStandaloneConfiguration(String hostName, int port) {

		Assert.hasText(hostName, "Host name must not be null or empty");
		Assert.isTrue(port >= 1 && port <= 65535,
				"Port %d must be a valid TCP port in the range between 1-65535".formatted(port));

		this.hostName = hostName;
		this.port = port;
	}

	@Override
	public String getHostName() {
		return hostName;
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public int getDatabase() {
		return database;
	}

	@Override
	public void setDatabase(int index) {

		Assert.isTrue(index >= 0, "Invalid DB index '%d'; non-negative index required".formatted(index));

		this.database = index;
	}

	@Override
	public void setUsername(@Nullable String username) {
		this.username = username;
	}

	@Nullable
	@Override
	public String getUsername() {
		return this.username;
	}

	@Override
	public ValkeyPassword getPassword() {
		return password;
	}

	@Override
	public void setPassword(ValkeyPassword password) {

		Assert.notNull(password, "ValkeyPassword must not be null");

		this.password = password;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ValkeyStandaloneConfiguration that)) {
			return false;
		}
		if (port != that.port) {
			return false;
		}
		if (database != that.database) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(hostName, that.hostName)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(username, that.username)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(password, that.password);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(hostName);
		result = 31 * result + port;
		result = 31 * result + database;
		result = 31 * result + ObjectUtils.nullSafeHashCode(username);
		result = 31 * result + ObjectUtils.nullSafeHashCode(password);
		return result;
	}
}