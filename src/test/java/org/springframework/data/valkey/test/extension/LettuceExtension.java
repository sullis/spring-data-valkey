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
package org.springframework.data.valkey.test.extension;

import io.lettuce.core.AbstractValkeyClient;
import io.lettuce.core.ValkeyClient;
import io.lettuce.core.ValkeyURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulValkeyConnection;
import io.lettuce.core.api.sync.ValkeyCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ValkeyClusterClient;
import io.lettuce.core.cluster.api.StatefulValkeyClusterConnection;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.pubsub.StatefulValkeyPubSubConnection;
import io.lettuce.core.resource.ClientResources;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.springframework.core.ResolvableType;
import org.springframework.data.valkey.SettingsUtils;
import org.springframework.data.util.Lazy;

/**
 * JUnit 5 {@link Extension} using Lettuce providing parameter resolution for connection resources and that reacts to
 * callbacks. The following resource types are supported by this extension:
 * <ul>
 * <li>{@link ClientResources} (singleton)</li>
 * <li>{@link ValkeyClient} (singleton)</li>
 * <li>{@link ValkeyClusterClient} (singleton)</li>
 * <li>{@link StatefulValkeyConnection}</li>
 * <li>{@link StatefulValkeyPubSubConnection}</li>
 * <li>{@link StatefulValkeyClusterConnection}</li>
 * </ul>
 *
 * <pre class="code">
 * &#064;ExtendWith(LettuceExtension.class)
 * public class CustomCommandTest {
 *
 * 	private final ValkeyCommands&lt;String, String&gt; redis;
 *
 * 	public CustomCommandTest(StatefulValkeyConnection&lt;String, String&gt; connection) {
 * 		this.redis = connection.sync();
 * 	}
 *
 * }
 * </pre>
 *
 * <h3>Resource lifecycle</h3> This extension allocates resources lazily and stores them in its {@link ExtensionContext}
 * {@link ExtensionContext.Store} for reuse across multiple tests. Client and {@link ClientResources} are allocated
 * through default {@link ValkeyClient} respective {@link ValkeyClientSupplier} so shutdown is managed by the actual
 * suppliers. Singleton connection resources are closed after the test class (test container) is finished.
 *
 * @author Mark Paluch
 * @see ParameterResolver
 * @see BeforeEachCallback
 * @see AfterEachCallback
 * @see AfterAllCallback
 */
public class LettuceExtension implements ParameterResolver, AfterAllCallback, AfterEachCallback {

	private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(LettuceExtension.class);

	private static final ClusterClientOptions DEFAULT_OPTIONS = ClusterClientOptions.builder()
			.protocolVersion(ProtocolVersion.RESP2).pingBeforeActivateConnection(false).build();

	private static final Set<Class<?>> SUPPORTED_INJECTABLE_TYPES = new HashSet<>(
			Arrays.asList(StatefulValkeyConnection.class, StatefulValkeyPubSubConnection.class, ValkeyCommands.class,
					ValkeyClient.class, ClientResources.class, StatefulValkeyClusterConnection.class, ValkeyClusterClient.class));

	private static final Set<Class<?>> CLOSE_AFTER_EACH = new HashSet<>(Arrays.asList(StatefulValkeyConnection.class,
			StatefulValkeyPubSubConnection.class, StatefulValkeyClusterConnection.class));

	private static final List<Supplier<?>> SUPPLIERS = Arrays.asList(ClientResourcesSupplier.INSTANCE,
			ValkeyClusterClientSupplier.INSTANCE, ValkeyClientSupplier.INSTANCE, StatefulValkeyConnectionSupplier.INSTANCE,
			StatefulValkeyPubSubConnectionSupplier.INSTANCE, StatefulValkeyClusterConnectionSupplier.INSTANCE);

	private static final List<Function<?, ?>> RESOURCE_FUNCTIONS = Collections
			.singletonList(ValkeyCommandsFunction.INSTANCE);

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return SUPPORTED_INJECTABLE_TYPES.contains(parameterContext.getParameter().getType());
	}

	/**
	 * Attempt to resolve the {@code requestedResourceType}.
	 *
	 * @param extensionContext
	 * @param requestedResourceType
	 * @param <T>
	 * @return
	 */
	public <T> T resolve(ExtensionContext extensionContext, Class<T> requestedResourceType) {

		ExtensionContext.Store store = getStore(extensionContext);

		return (T) store.getOrComputeIfAbsent(requestedResourceType, it -> findSupplier(requestedResourceType).get());
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		ExtensionContext.Store store = getStore(extensionContext);
		Parameter parameter = parameterContext.getParameter();
		Type parameterizedType = parameter.getParameterizedType();

		return store.getOrComputeIfAbsent(parameter.getType(), it -> doGetInstance(parameterizedType));
	}

	@Override
	public void afterEach(ExtensionContext context) {

		ExtensionContext.Store store = getStore(context);

		ValkeyClient redisClient = store.get(ValkeyClient.class, ValkeyClient.class);
		if (redisClient != null) {
			redisClient.setOptions(DEFAULT_OPTIONS);
		}
	}

	@Override
	public void afterAll(ExtensionContext context) {

		ExtensionContext.Store store = getStore(context);

		CLOSE_AFTER_EACH.forEach(it -> {

			StatefulConnection connection = store.get(it, StatefulConnection.class);

			if (connection != null) {
				connection.close();
				store.remove(StatefulValkeyConnection.class);
			}
		});
	}

	@SuppressWarnings("unchecked")
	private static Supplier<Object> findSupplier(Type type) {

		ResolvableType requested = ResolvableType.forType(type);

		Supplier<?> supplier = SUPPLIERS.stream().filter(it -> {

			ResolvableType providedType = ResolvableType.forType(it.getClass()).as(Supplier.class).getGeneric(0);

			if (requested.isAssignableFrom(providedType)) {
				return true;
			}
			return false;
		}).findFirst().orElseThrow(() -> new NoSuchElementException("Cannot find a factory for " + type));

		return (Supplier) supplier;
	}

	public <T> T getInstance(Class<T> resourceType) {
		return (T) doGetInstance(resourceType);
	}

	private Object doGetInstance(Type parameterizedType) {

		Optional<ResourceFunction> resourceFunction = findFunction(parameterizedType);
		return resourceFunction.map(it -> it.function.apply(findSupplier(it.dependsOn.getType()).get()))
				.orElseGet(() -> findSupplier(parameterizedType).get());
	}

	private ExtensionContext.Store getStore(ExtensionContext extensionContext) {
		return extensionContext.getStore(NAMESPACE);
	}

	private static Optional<ResourceFunction> findFunction(Type type) {

		ResolvableType requested = ResolvableType.forType(type);

		return RESOURCE_FUNCTIONS.stream().map(it -> {

			ResolvableType dependsOn = ResolvableType.forType(it.getClass()).as(Function.class).getGeneric(0);
			ResolvableType providedType = ResolvableType.forType(it.getClass()).as(Function.class).getGeneric(1);

			return new ResourceFunction(dependsOn, providedType, it);
		}).filter(it -> requested.isAssignableFrom(it.provides)).findFirst();
	}

	static class ResourceFunction {

		final ResolvableType dependsOn;
		final ResolvableType provides;
		final Function<Object, Object> function;

		public ResourceFunction(ResolvableType dependsOn, ResolvableType provides, Function<?, ?> function) {
			this.dependsOn = dependsOn;
			this.provides = provides;
			this.function = (Function) function;
		}
	}

	enum ClientResourcesSupplier implements Supplier<ClientResources> {

		INSTANCE;

		@Override
		public ClientResources get() {
			return LettuceTestClientResources.getSharedClientResources();
		}
	}

	record ValkeyClientCloseable(AbstractValkeyClient client) implements Closeable {

		@Override
		public void close() throws IOException {
			client.shutdown(0, 0, TimeUnit.MILLISECONDS);
		}
	}

	enum ValkeyClientSupplier implements Supplier<ValkeyClient> {

		INSTANCE;

		final Lazy<ValkeyClient> lazy = Lazy.of(() -> {

			ValkeyClient client = ValkeyClient.create(LettuceTestClientResources.getSharedClientResources(),
					ValkeyURI.create(SettingsUtils.getHost(), SettingsUtils.getPort()));
			client.setOptions(DEFAULT_OPTIONS);

			ShutdownQueue.register(new ValkeyClientCloseable(client));
			return client;
		});

		@Override
		public ValkeyClient get() {
			return lazy.get();
		}
	}

	enum ValkeyClusterClientSupplier implements Supplier<ValkeyClusterClient> {

		INSTANCE;

		final Lazy<ValkeyClusterClient> lazy = Lazy.of(() -> {
			ValkeyClusterClient client = ValkeyClusterClient.create(LettuceTestClientResources.getSharedClientResources(),
					ValkeyURI.create(SettingsUtils.getHost(), SettingsUtils.getClusterPort()));
			client.setOptions(DEFAULT_OPTIONS);

			ShutdownQueue.register(new ValkeyClientCloseable(client));
			return client;
		});

		@Override
		public ValkeyClusterClient get() {
			return lazy.get();
		}
	}

	enum StatefulValkeyConnectionSupplier implements Supplier<StatefulValkeyConnection<String, String>> {

		INSTANCE;

		@Override
		public StatefulValkeyConnection<String, String> get() {
			return ValkeyClientSupplier.INSTANCE.get().connect();
		}
	}

	enum StatefulValkeyPubSubConnectionSupplier implements Supplier<StatefulValkeyPubSubConnection<String, String>> {

		INSTANCE;

		@Override
		public StatefulValkeyPubSubConnection<String, String> get() {
			return ValkeyClientSupplier.INSTANCE.get().connectPubSub();
		}
	}

	enum StatefulValkeyClusterConnectionSupplier implements Supplier<StatefulValkeyClusterConnection<String, String>> {

		INSTANCE;

		@Override
		public StatefulValkeyClusterConnection<String, String> get() {
			return ValkeyClusterClientSupplier.INSTANCE.get().connect();
		}
	}

	enum ValkeyCommandsFunction
			implements Function<StatefulValkeyConnection<String, String>, ValkeyCommands<String, String>> {
		INSTANCE;

		@Override
		public ValkeyCommands<String, String> apply(StatefulValkeyConnection<String, String> connection) {
			return connection.sync();
		}
	}

}
