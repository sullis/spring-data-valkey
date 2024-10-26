/*
 * Copyright 2014-2024 the original author or authors.
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

import static org.springframework.util.StringUtils.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.data.valkey.connection.ValkeyConfiguration.SentinelConfiguration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * {@link ValkeyConfiguration Configuration} class used to set up a {@link ValkeyConnection} with
 * {@link ValkeyConnectionFactory} for connecting to <a href="https://redis.io/topics/sentinel">Valkey Sentinel(s)</a>.
 * Useful when setting up a highly available Valkey environment.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Vikas Garg
 * @author John Blum
 * @author Samuel Klose
 * @author Mustapha Zorgati
 * @since 1.4
 */
public class ValkeySentinelConfiguration implements ValkeyConfiguration, SentinelConfiguration {

	private static final String VALKEY_SENTINEL_MASTER_CONFIG_PROPERTY = "spring.valkey.sentinel.master";
	private static final String VALKEY_SENTINEL_NODES_CONFIG_PROPERTY = "spring.valkey.sentinel.nodes";
	private static final String VALKEY_SENTINEL_USERNAME_CONFIG_PROPERTY = "spring.valkey.sentinel.username";
	private static final String VALKEY_SENTINEL_PASSWORD_CONFIG_PROPERTY = "spring.valkey.sentinel.password";
	private static final String VALKEY_SENTINEL_DATA_NODE_USERNAME_CONFIG_PROPERTY = "spring.valkey.sentinel.dataNode.username";
	private static final String VALKEY_SENTINEL_DATA_NODE_PASSWORD_CONFIG_PROPERTY = "spring.valkey.sentinel.dataNode.password";
	private static final String VALKEY_SENTINEL_DATA_NODE_DATABASE_CONFIG_PROPERTY = "spring.valkey.sentinel.dataNode.database";

	private int database;

	private @Nullable NamedNode master;

	private ValkeyPassword dataNodePassword = ValkeyPassword.none();
	private ValkeyPassword sentinelPassword = ValkeyPassword.none();

	private final Set<ValkeyNode> sentinels;

	private @Nullable String dataNodeUsername = null;
	private @Nullable String sentinelUsername = null;

	/**
	 * Creates a new, default {@link ValkeySentinelConfiguration}.
	 */
	public ValkeySentinelConfiguration() {
		this(new MapPropertySource("ValkeySentinelConfiguration", Collections.emptyMap()));
	}

	/**
	 * Creates a new {@link ValkeySentinelConfiguration} for given {@link String hostPort} combinations.
	 *
	 * <pre class="code">
	 * sentinelHostAndPorts[0] = 127.0.0.1:23679
	 * sentinelHostAndPorts[1] = 127.0.0.1:23680 ...
	 * </pre>
	 *
	 * @param sentinelHostAndPorts must not be {@literal null}.
	 * @since 1.5
	 */
	public ValkeySentinelConfiguration(String master, Set<String> sentinelHostAndPorts) {
		this(new MapPropertySource("ValkeySentinelConfiguration", asMap(master, sentinelHostAndPorts)));
	}

	/**
	 * Creates a new {@link ValkeySentinelConfiguration} looking up configuration values from the given
	 * {@link PropertySource}.
	 *
	 * <pre class="code">
	 * spring.valkey.sentinel.master=myMaster
	 * spring.valkey.sentinel.nodes=127.0.0.1:23679,127.0.0.1:23680,127.0.0.1:23681
	 * </pre>
	 *
	 * @param propertySource must not be {@literal null}.
	 * @since 1.5
	 * @deprecated since 3.3, use {@link ValkeySentinelConfiguration#of(PropertySource)} instead. This constructor will be
	 *             made private in the next major release.
	 */
	@Deprecated(since = "3.3")
	public ValkeySentinelConfiguration(PropertySource<?> propertySource) {

		Assert.notNull(propertySource, "PropertySource must not be null");

		this.sentinels = new LinkedHashSet<>();

		if (propertySource.containsProperty(VALKEY_SENTINEL_MASTER_CONFIG_PROPERTY)) {
			String sentinelMaster = String.valueOf(propertySource.getProperty(VALKEY_SENTINEL_MASTER_CONFIG_PROPERTY));
			this.setMaster(sentinelMaster);
		}

		if (propertySource.containsProperty(VALKEY_SENTINEL_NODES_CONFIG_PROPERTY)) {
			String sentinelNodes = String.valueOf(propertySource.getProperty(VALKEY_SENTINEL_NODES_CONFIG_PROPERTY));
			appendSentinels(commaDelimitedListToSet(sentinelNodes));
		}

		if (propertySource.containsProperty(VALKEY_SENTINEL_PASSWORD_CONFIG_PROPERTY)) {
			String sentinelPassword = String.valueOf(propertySource.getProperty(VALKEY_SENTINEL_PASSWORD_CONFIG_PROPERTY));
			this.setSentinelPassword(sentinelPassword);
		}

		if (propertySource.containsProperty(VALKEY_SENTINEL_USERNAME_CONFIG_PROPERTY)) {
			String sentinelUsername = String.valueOf(propertySource.getProperty(VALKEY_SENTINEL_USERNAME_CONFIG_PROPERTY));
			this.setSentinelUsername(sentinelUsername);
		}

		if (propertySource.containsProperty(VALKEY_SENTINEL_DATA_NODE_USERNAME_CONFIG_PROPERTY)) {
			String dataNodeUsername = String
					.valueOf(propertySource.getProperty(VALKEY_SENTINEL_DATA_NODE_USERNAME_CONFIG_PROPERTY));
			this.setUsername(dataNodeUsername);
		}

		if (propertySource.containsProperty(VALKEY_SENTINEL_DATA_NODE_PASSWORD_CONFIG_PROPERTY)) {
			String dataNodePassword = String
					.valueOf(propertySource.getProperty(VALKEY_SENTINEL_DATA_NODE_PASSWORD_CONFIG_PROPERTY));
			this.setPassword(dataNodePassword);
		}

		if (propertySource.containsProperty(VALKEY_SENTINEL_DATA_NODE_DATABASE_CONFIG_PROPERTY)) {
			String databaseSource = String
					.valueOf(propertySource.getProperty(VALKEY_SENTINEL_DATA_NODE_DATABASE_CONFIG_PROPERTY));
			int database;
			try {
				database = Integer.parseInt(databaseSource);
			} catch (NumberFormatException ex) {
				throw new IllegalArgumentException("Invalid DB index '%s'; integer required".formatted(databaseSource));
			}
			this.setDatabase(database);
		}
	}

	/**
	 * Construct a new {@link ValkeySentinelConfiguration} from the given {@link PropertySource}.
	 *
	 * @param propertySource must not be {@literal null}.
	 * @return a new {@link ValkeySentinelConfiguration} configured from the given {@link PropertySource}.
	 * @since 3.3
	 */
	public static ValkeySentinelConfiguration of(PropertySource<?> propertySource) {
		return new ValkeySentinelConfiguration(propertySource);
	}

	/**
	 * Set {@literal Sentinels} to connect to.
	 *
	 * @param sentinels must not be {@literal null}.
	 */
	public void setSentinels(Iterable<ValkeyNode> sentinels) {

		Assert.notNull(sentinels, "Cannot set sentinels to null");

		this.sentinels.clear();

		for (ValkeyNode sentinel : sentinels) {
			addSentinel(sentinel);
		}
	}

	public Set<ValkeyNode> getSentinels() {
		return Collections.unmodifiableSet(sentinels);
	}

	/**
	 * Add sentinel.
	 *
	 * @param sentinel must not be {@literal null}.
	 */
	public void addSentinel(ValkeyNode sentinel) {

		Assert.notNull(sentinel, "Sentinel must not be null");

		this.sentinels.add(sentinel);
	}

	public void setMaster(NamedNode master) {

		Assert.notNull(master, "Sentinel master node must not be null");

		this.master = master;
	}

	@Nullable
	public NamedNode getMaster() {
		return master;
	}

	/**
	 * @see #setMaster(String)
	 * @param master The master node name.
	 * @return this.
	 */
	public ValkeySentinelConfiguration master(String master) {
		this.setMaster(master);
		return this;
	}

	/**
	 * @see #setMaster(NamedNode)
	 * @param master the master node
	 * @return this.
	 */
	public ValkeySentinelConfiguration master(NamedNode master) {
		this.setMaster(master);
		return this;
	}

	/**
	 * @see #addSentinel(ValkeyNode)
	 * @param sentinel the node to add as sentinel.
	 * @return this.
	 */
	public ValkeySentinelConfiguration sentinel(ValkeyNode sentinel) {
		this.addSentinel(sentinel);
		return this;
	}

	/**
	 * @see #sentinel(ValkeyNode)
	 * @param host redis sentinel node host name or ip.
	 * @param port redis sentinel port.
	 * @return this.
	 */
	public ValkeySentinelConfiguration sentinel(String host, Integer port) {
		return sentinel(new ValkeyNode(host, port));
	}

	private void appendSentinels(Set<String> hostAndPorts) {

		for (String hostAndPort : hostAndPorts) {
			addSentinel(ValkeyNode.fromString(hostAndPort, ValkeyNode.DEFAULT_SENTINEL_PORT));
		}
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
		this.dataNodeUsername = username;
	}

	@Nullable
	@Override
	public String getUsername() {
		return this.dataNodeUsername;
	}

	@Override
	public ValkeyPassword getPassword() {
		return dataNodePassword;
	}

	@Override
	public void setPassword(ValkeyPassword password) {

		Assert.notNull(password, "ValkeyPassword must not be null");

		this.dataNodePassword = password;
	}

	@Nullable
	@Override
	public String getSentinelUsername() {
		return this.sentinelUsername;
	}

	@Override
	public void setSentinelUsername(@Nullable String sentinelUsername) {
		this.sentinelUsername = sentinelUsername;
	}

	@Override
	public void setSentinelPassword(ValkeyPassword sentinelPassword) {

		Assert.notNull(sentinelPassword, "SentinelPassword must not be null");
		this.sentinelPassword = sentinelPassword;
	}

	@Override
	public ValkeyPassword getSentinelPassword() {
		return sentinelPassword;
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof ValkeySentinelConfiguration that)) {
			return false;
		}

		return this.database == that.database && ObjectUtils.nullSafeEquals(this.master, that.master)
				&& ObjectUtils.nullSafeEquals(this.sentinels, that.sentinels)
				&& ObjectUtils.nullSafeEquals(this.dataNodeUsername, that.dataNodeUsername)
				&& ObjectUtils.nullSafeEquals(this.dataNodePassword, that.dataNodePassword)
				&& ObjectUtils.nullSafeEquals(this.sentinelUsername, that.sentinelUsername)
				&& ObjectUtils.nullSafeEquals(this.sentinelPassword, that.sentinelPassword);
	}

	@Override
	public int hashCode() {

		int result = ObjectUtils.nullSafeHashCode(master);

		result = 31 * result + ObjectUtils.nullSafeHashCode(sentinels);
		result = 31 * result + database;
		result = 31 * result + ObjectUtils.nullSafeHashCode(dataNodeUsername);
		result = 31 * result + ObjectUtils.nullSafeHashCode(dataNodePassword);
		result = 31 * result + ObjectUtils.nullSafeHashCode(sentinelUsername);
		result = 31 * result + ObjectUtils.nullSafeHashCode(sentinelPassword);

		return result;
	}

	/**
	 * @param master must not be {@literal null} or empty.
	 * @param sentinelHostAndPorts must not be {@literal null}.
	 * @return configuration map.
	 */
	private static Map<String, Object> asMap(String master, Set<String> sentinelHostAndPorts) {

		Assert.hasText(master, "Master address must not be null or empty");
		Assert.notNull(sentinelHostAndPorts, "SentinelHostAndPorts must not be null");
		Assert.noNullElements(sentinelHostAndPorts, "ClusterHostAndPorts must not contain null elements");

		Map<String, Object> map = new HashMap<>();

		map.put(VALKEY_SENTINEL_MASTER_CONFIG_PROPERTY, master);
		map.put(VALKEY_SENTINEL_NODES_CONFIG_PROPERTY, StringUtils.collectionToCommaDelimitedString(sentinelHostAndPorts));

		return map;
	}
}
