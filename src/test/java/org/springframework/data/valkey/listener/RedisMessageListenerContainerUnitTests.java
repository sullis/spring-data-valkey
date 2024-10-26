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
package org.springframework.data.valkey.listener;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.valkey.ValkeyConnectionFailureException;
import org.springframework.data.valkey.connection.MessageListener;
import org.springframework.data.valkey.connection.ValkeyConnection;
import org.springframework.data.valkey.connection.ValkeyConnectionFactory;
import org.springframework.data.valkey.connection.Subscription;
import org.springframework.data.valkey.connection.SubscriptionListener;
import org.springframework.data.valkey.connection.jedis.JedisConnectionFactory;
import org.springframework.data.valkey.listener.adapter.MessageListenerAdapter;
import org.springframework.data.valkey.listener.adapter.ValkeyListenerExecutionFailedException;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Unit tests for {@link ValkeyMessageListenerContainer}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Seongjun Lee
 */
class ValkeyMessageListenerContainerUnitTests {

	private final Object handler = new Object() {

		@SuppressWarnings("unused")
		public void handleMessage(Object message) {}
	};

	private final MessageListenerAdapter adapter = new MessageListenerAdapter(handler);

	private ValkeyMessageListenerContainer container;

	private ValkeyConnectionFactory connectionFactoryMock;
	private ValkeyConnection connectionMock;
	private Subscription subscriptionMock;
	private Executor executorMock;

	@BeforeEach
	void setUp() {

		executorMock = mock(Executor.class);
		connectionFactoryMock = mock(JedisConnectionFactory.class);
		connectionMock = mock(ValkeyConnection.class);
		subscriptionMock = mock(Subscription.class);

		container = new ValkeyMessageListenerContainer();
		container.setConnectionFactory(connectionFactoryMock);
		container.setBeanName("container");
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(executorMock);
		container.setMaxSubscriptionRegistrationWaitingTime(1);
		container.afterPropertiesSet();
	}

	@Test // DATAREDIS-840
	void containerShouldStopGracefullyOnUnsubscribeErrors() {

		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
		doThrow(new IllegalStateException()).when(subscriptionMock).pUnsubscribe();

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			r.run();
			return null;
		}).when(executorMock).execute(any());

		doAnswer(it -> {

			SubscriptionListener listener = it.getArgument(0);
			when(connectionMock.isSubscribed()).thenReturn(true);

			listener.onChannelSubscribed("a".getBytes(StandardCharsets.UTF_8), 0);

			return null;
		}).when(connectionMock).subscribe(any(), any());

		container.addMessageListener(adapter, new ChannelTopic("a"));
		container.start();

		when(connectionMock.getSubscription()).thenReturn(subscriptionMock);

		container.stop();

		assertThat(container.isRunning()).isFalse();
		verify(connectionMock).close();
	}

	@Test // GH-2335
	void containerStartShouldReportFailureOnValkeyUnavailability() {

		when(connectionFactoryMock.getConnection()).thenThrow(new ValkeyConnectionFailureException("Booh"));

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			r.run();
			return null;
		}).when(executorMock).execute(any());

		container.addMessageListener(adapter, new ChannelTopic("a"));
		assertThatExceptionOfType(ValkeyListenerExecutionFailedException.class).isThrownBy(() -> container.start());

		assertThat(container.isRunning()).isTrue();
		assertThat(container.isListening()).isFalse();
	}

	@Test // GH-2335
	void containerListenShouldReportFailureOnValkeyUnavailability() {

		when(connectionFactoryMock.getConnection()).thenThrow(new ValkeyConnectionFailureException("Booh"));

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			r.run();
			return null;
		}).when(executorMock).execute(any());

		container.start();

		assertThatExceptionOfType(ValkeyListenerExecutionFailedException.class)
				.isThrownBy(() -> container.addMessageListener(adapter, new ChannelTopic("a")));

		assertThat(container.isRunning()).isTrue();
		assertThat(container.isListening()).isFalse();
	}

	@Test // GH-2335
	void shouldRecoverFromConnectionFailure() throws Exception {

		AtomicInteger requestCount = new AtomicInteger();
		AtomicBoolean shouldThrowSubscriptionException = new AtomicBoolean();

		container = new ValkeyMessageListenerContainer();
		container.setConnectionFactory(connectionFactoryMock);
		container.setBeanName("container");
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
		container.setMaxSubscriptionRegistrationWaitingTime(1000);
		container.setRecoveryBackoff(new FixedBackOff(1, 5));
		container.afterPropertiesSet();

		doAnswer(it -> {

			int req = requestCount.incrementAndGet();
			if (req == 1 || req == 3) {
				return connectionMock;
			}

			throw new ValkeyConnectionFailureException("Booh");
		}).when(connectionFactoryMock).getConnection();

		CountDownLatch exceptionWait = new CountDownLatch(1);
		CountDownLatch armed = new CountDownLatch(1);
		CountDownLatch recoveryArmed = new CountDownLatch(1);

		doAnswer(it -> {

			SubscriptionListener listener = it.getArgument(0);
			when(connectionMock.isSubscribed()).thenReturn(true);

			listener.onChannelSubscribed("a".getBytes(StandardCharsets.UTF_8), 1);

			armed.countDown();
			exceptionWait.await();

			if (shouldThrowSubscriptionException.compareAndSet(true, false)) {
				when(connectionMock.isSubscribed()).thenReturn(false);
				throw new ValkeyConnectionFailureException("Disconnected");
			}

			recoveryArmed.countDown();

			return null;
		}).when(connectionMock).subscribe(any(), any());

		container.start();
		container.addMessageListener(new MessageListenerAdapter(handler), new ChannelTopic("a"));
		armed.await();

		// let an exception happen
		shouldThrowSubscriptionException.set(true);
		exceptionWait.countDown();

		// wait for subscription recovery
		recoveryArmed.await();

		assertThat(recoveryArmed.getCount()).isZero();

	}

	@Test // GH-964
	void failsOnDuplicateInit() {
		assertThatIllegalStateException().isThrownBy(() -> container.afterPropertiesSet());
	}

	@Test // GH-3009
	void shouldRemoveAllListenersWhenListenerIsNull() {

		MessageListener listener1 = mock(MessageListener.class);
		MessageListener listener2 = mock(MessageListener.class);
		Topic topic = new ChannelTopic("topic1");

		container.addMessageListener(listener1, Collections.singletonList(topic));
		container.addMessageListener(listener2, Collections.singletonList(topic));

		container.removeMessageListener(null, Collections.singletonList(topic));

		assertThatNoException().isThrownBy(() -> container.removeMessageListener(null, Collections.singletonList(topic)));
	}
}
