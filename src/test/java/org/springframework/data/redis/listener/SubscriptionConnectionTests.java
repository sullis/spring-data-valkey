/*
 * Copyright 2011-2024 the original author or authors.
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
package org.springframework.data.redis.listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.ValkeyConnection;
import org.springframework.data.redis.connection.ValkeyConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.test.extension.ValkeyStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedValkeyTest;

/**
 * Integration tests confirming that {@link ValkeyMessageListenerContainer} closes connections after unsubscribing
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@MethodSource("testParams")
public class SubscriptionConnectionTests {

	private static final Log logger = LogFactory.getLog(SubscriptionConnectionTests.class);
	private static final String CHANNEL = "pubsub::test";

	private ValkeyConnectionFactory connectionFactory;

	private List<ValkeyMessageListenerContainer> containers = new ArrayList<>();

	private final Object handler = new Object() {
		@SuppressWarnings("unused")
		public void handleMessage(String message) {
			logger.debug(message);
		}
	};

	public SubscriptionConnectionTests(ValkeyConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public static Collection<Object[]> testParams() {

		// Jedis
		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);

		return Arrays.asList(new Object[][] { { jedisConnFactory }, { lettuceConnFactory } });
	}

	@AfterEach
	void tearDown() throws Exception {
		for (ValkeyMessageListenerContainer container : containers) {
			if (container.isActive()) {
				container.destroy();
			}
		}
	}

	@ParameterizedValkeyTest // GH-964
	void testStopMessageListenerContainers() throws Exception {

		// Grab all 8 connections from the pool. They should be released on
		// container stop
		for (int i = 0; i < 8; i++) {
			ValkeyMessageListenerContainer container = new ValkeyMessageListenerContainer();
			container.setConnectionFactory(connectionFactory);
			container.setBeanName("container" + i);
			container.addMessageListener(new MessageListenerAdapter(handler),
					Collections.singletonList(new ChannelTopic(CHANNEL)));
			container.setTaskExecutor(new SyncTaskExecutor());
			container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
			container.afterPropertiesSet();
			container.start();

			container.stop();
			containers.add(container);
		}

		// verify we can now get a connection from the pool
		ValkeyConnection connection = connectionFactory.getConnection();
		connection.close();
	}

	@ParameterizedValkeyTest
	void testRemoveLastListener() throws Exception {

		// Grab all 8 connections from the pool
		MessageListener listener = new MessageListenerAdapter(handler);
		for (int i = 0; i < 8; i++) {
			ValkeyMessageListenerContainer container = new ValkeyMessageListenerContainer();
			container.setConnectionFactory(connectionFactory);
			container.setBeanName("container" + i);
			container.addMessageListener(listener, Arrays.asList(new ChannelTopic(CHANNEL)));
			container.setTaskExecutor(new SyncTaskExecutor());
			container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
			container.afterPropertiesSet();
			container.start();
			containers.add(container);
		}

		// Removing the sole listener from the container should free up a
		// connection
		containers.get(0).removeMessageListener(listener);

		// verify we can now get a connection from the pool
		ValkeyConnection connection = connectionFactory.getConnection();
		connection.close();
	}

	@ParameterizedValkeyTest
	void testStopListening() throws InterruptedException {

		// Grab all 8 connections from the pool.
		MessageListener listener = new MessageListenerAdapter(handler);
		for (int i = 0; i < 8; i++) {
			ValkeyMessageListenerContainer container = new ValkeyMessageListenerContainer();
			container.setConnectionFactory(connectionFactory);
			container.setBeanName("container" + i);
			container.addMessageListener(listener, Arrays.asList(new ChannelTopic(CHANNEL)));
			container.setTaskExecutor(new SyncTaskExecutor());
			container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
			container.afterPropertiesSet();
			container.start();
			containers.add(container);
		}

		// Unsubscribe all listeners from all topics, freeing up a connection
		containers.get(0).stop();

		// verify we can now get a connection from the pool
		ValkeyConnection connection = connectionFactory.getConnection();
		connection.close();
	}

}
