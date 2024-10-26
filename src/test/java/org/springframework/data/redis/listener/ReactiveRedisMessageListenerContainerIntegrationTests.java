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
package org.springframework.data.redis.listener;

import static org.assertj.core.api.Assertions.*;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.ReactiveValkeyConnection;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.ReactiveSubscription.ChannelMessage;
import org.springframework.data.redis.connection.ReactiveSubscription.PatternMessage;
import org.springframework.data.redis.connection.ValkeyConnection;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveValkeyTemplate;
import org.springframework.data.redis.serializer.ValkeySerializationContext;
import org.springframework.data.redis.serializer.ValkeySerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.ValkeySerializer;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedValkeyTest;
import org.springframework.lang.Nullable;

/**
 * Integration tests for {@link ReactiveValkeyMessageListenerContainer} via Lettuce.
 *
 * @author Mark Paluch
 */
@MethodSource("testParams")
public class ReactiveValkeyMessageListenerContainerIntegrationTests {

	private static final String CHANNEL1 = "my-channel";
	private static final String PATTERN1 = "my-chan*";
	private static final String MESSAGE = "hello world";

	private final LettuceConnectionFactory connectionFactory;
	private @Nullable ValkeyConnection connection;
	private @Nullable ReactiveValkeyConnection reactiveConnection;

	/**
	 * @param connectionFactory
	 * @param label parameterized test label, no further use besides that.
	 */
	public ReactiveValkeyMessageListenerContainerIntegrationTests(LettuceConnectionFactory connectionFactory,
			String label) {
		this.connectionFactory = connectionFactory;
	}

	public static Collection<Object[]> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	@BeforeEach
	void before() {
		connection = connectionFactory.getConnection();
		reactiveConnection = connectionFactory.getReactiveConnection();
	}

	@AfterEach
	void tearDown() {

		if (connection != null) {
			connection.close();
		}

		if (reactiveConnection != null) {
			reactiveConnection.close();
		}
	}

	@ParameterizedValkeyTest // DATAREDIS-612, GH-1622
	void shouldReceiveChannelMessages() {

		ReactiveValkeyMessageListenerContainer container = new ReactiveValkeyMessageListenerContainer(connectionFactory);

		container.receiveLater(ChannelTopic.of(CHANNEL1)) //
				.doOnNext(it -> doPublish(CHANNEL1.getBytes(), MESSAGE.getBytes())) //
				.flatMapMany(Function.identity()) //
				.as(StepVerifier::create) //
				.assertNext(c -> {

					assertThat(c.getChannel()).isEqualTo(CHANNEL1);
					assertThat(c.getMessage()).isEqualTo(MESSAGE);
				}) //
				.thenCancel().verify();

		container.destroy();
	}

	@ParameterizedValkeyTest // GH-1622
	void receiveChannelShouldNotifySubscriptionListener() throws Exception {

		ReactiveValkeyMessageListenerContainer container = new ReactiveValkeyMessageListenerContainer(connectionFactory);

		AtomicReference<String> onSubscribe = new AtomicReference<>();
		AtomicReference<String> onUnsubscribe = new AtomicReference<>();
		CompletableFuture<Void> subscribe = new CompletableFuture<>();
		CompletableFuture<Void> unsubscribe = new CompletableFuture<>();

		CompositeListener listener = new CompositeListener() {
			@Override
			public void onMessage(Message message, @Nullable byte[] pattern) {

			}

			@Override
			public void onChannelSubscribed(byte[] channel, long count) {
				onSubscribe.set(new String(channel));
				subscribe.complete(null);
			}

			@Override
			public void onChannelUnsubscribed(byte[] channel, long count) {
				onUnsubscribe.set(new String(channel));
				unsubscribe.complete(null);
			}
		};

		container.receive(Collections.singletonList(ChannelTopic.of(CHANNEL1)), listener) //
				.as(StepVerifier::create) //
				.then(awaitSubscription(container::getActiveSubscriptions))
				.then(() -> doPublish(CHANNEL1.getBytes(), MESSAGE.getBytes())) //
				.assertNext(c -> {

					assertThat(c.getChannel()).isEqualTo(CHANNEL1);
					assertThat(c.getMessage()).isEqualTo(MESSAGE);
				}) //
				.thenCancel().verify();

		unsubscribe.get(10, TimeUnit.SECONDS);

		assertThat(onSubscribe).hasValue(CHANNEL1);
		assertThat(onUnsubscribe).hasValue(CHANNEL1);

		container.destroy();
	}

	@ParameterizedValkeyTest // DATAREDIS-612, GH-1622
	void shouldReceivePatternMessages() {

		ReactiveValkeyMessageListenerContainer container = new ReactiveValkeyMessageListenerContainer(connectionFactory);

		container.receiveLater(PatternTopic.of(PATTERN1)) //
				.doOnNext(it -> doPublish(CHANNEL1.getBytes(), MESSAGE.getBytes())).flatMapMany(Function.identity()) //
				.as(StepVerifier::create) //
				.assertNext(c -> {

					assertThat(c.getPattern()).isEqualTo(PATTERN1);
					assertThat(c.getChannel()).isEqualTo(CHANNEL1);
					assertThat(c.getMessage()).isEqualTo(MESSAGE);
				}) //
				.thenCancel().verify();

		container.destroy();
	}

	@ParameterizedValkeyTest // GH-1622
	void receivePatternShouldNotifySubscriptionListener() throws Exception {

		ReactiveValkeyMessageListenerContainer container = new ReactiveValkeyMessageListenerContainer(connectionFactory);

		AtomicReference<String> onPsubscribe = new AtomicReference<>();
		AtomicReference<String> onPunsubscribe = new AtomicReference<>();
		CompletableFuture<Void> psubscribe = new CompletableFuture<>();
		CompletableFuture<Void> punsubscribe = new CompletableFuture<>();

		CompositeListener listener = new CompositeListener() {
			@Override
			public void onMessage(Message message, @Nullable byte[] pattern) {

			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				onPsubscribe.set(new String(pattern));
				psubscribe.complete(null);
			}

			@Override
			public void onPatternUnsubscribed(byte[] pattern, long count) {
				onPunsubscribe.set(new String(pattern));
				punsubscribe.complete(null);
			}
		};

		container.receive(Collections.singletonList(PatternTopic.of(PATTERN1)), listener) //
				.cast(PatternMessage.class) //
				.as(StepVerifier::create) //
				.then(awaitSubscription(container::getActiveSubscriptions))
				.then(() -> doPublish(CHANNEL1.getBytes(), MESSAGE.getBytes())) //
				.assertNext(c -> {

					assertThat(c.getPattern()).isEqualTo(PATTERN1);
					assertThat(c.getChannel()).isEqualTo(CHANNEL1);
					assertThat(c.getMessage()).isEqualTo(MESSAGE);
				}) //
				.thenCancel().verify();

		punsubscribe.get(10, TimeUnit.SECONDS);

		assertThat(onPsubscribe).hasValue(PATTERN1);
		assertThat(onPunsubscribe).hasValue(PATTERN1);

		container.destroy();
	}

	@ParameterizedValkeyTest // DATAREDIS-612, GH-1622
	void shouldPublishAndReceiveMessage() throws Exception {

		ReactiveValkeyMessageListenerContainer container = new ReactiveValkeyMessageListenerContainer(connectionFactory);
		ReactiveValkeyTemplate<String, String> template = new ReactiveValkeyTemplate<>(connectionFactory,
				ValkeySerializationContext.string());

		BlockingQueue<PatternMessage<String, String, String>> messages = new LinkedBlockingDeque<>();
		CompletableFuture<Void> subscribed = new CompletableFuture<>();
		Disposable subscription = container.receiveLater(PatternTopic.of(PATTERN1))
				.doOnNext(it -> subscribed.complete(null)).flatMapMany(Function.identity()).doOnNext(messages::add).subscribe();

		subscribed.get(5, TimeUnit.SECONDS);

		template.convertAndSend(CHANNEL1, MESSAGE).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		PatternMessage<String, String, String> message = messages.poll(1, TimeUnit.SECONDS);

		assertThat(message).isNotNull();
		assertThat(message.getPattern()).isEqualTo(PATTERN1);
		assertThat(message.getChannel()).isEqualTo(CHANNEL1);
		assertThat(message.getMessage()).isEqualTo(MESSAGE);

		subscription.dispose();
		container.destroy();
	}

	@ParameterizedValkeyTest // DATAREDIS-612
	void listenToChannelShouldReceiveChannelMessagesCorrectly() throws InterruptedException {

		ReactiveValkeyTemplate<String, String> template = new ReactiveValkeyTemplate<>(connectionFactory,
				ValkeySerializationContext.string());

		template.listenToChannel(CHANNEL1).as(StepVerifier::create) //
				.thenAwait(Duration.ofMillis(100)) // just make sure we the subscription completed
				.then(() -> doPublish(CHANNEL1.getBytes(), MESSAGE.getBytes())) //
				.assertNext(message -> {

					assertThat(message).isInstanceOf(ChannelMessage.class);
					assertThat(message.getMessage()).isEqualTo(MESSAGE);
					assertThat(((ChannelMessage) message).getChannel()).isEqualTo(CHANNEL1);
				}) //
				.thenCancel() //
				.verify();
	}

	@ParameterizedValkeyTest // DATAREDIS-612
	void listenToPatternShouldReceiveMessagesCorrectly() {

		ReactiveValkeyTemplate<String, String> template = new ReactiveValkeyTemplate<>(connectionFactory,
				ValkeySerializationContext.string());

		template.listenToPattern(PATTERN1).as(StepVerifier::create) //
				.thenAwait(Duration.ofMillis(100)) // just make sure we the subscription completed
				.then(() -> doPublish(CHANNEL1.getBytes(), MESSAGE.getBytes())) //
				.assertNext(message -> {

					assertThat(message).isInstanceOf(PatternMessage.class);
					assertThat(((PatternMessage) message).getPattern()).isEqualTo(PATTERN1);
					assertThat(((PatternMessage) message).getChannel()).isEqualTo(CHANNEL1);
					assertThat(message.getMessage()).isEqualTo(MESSAGE);
				}) //
				.thenCancel() //
				.verify();
	}

	@ParameterizedValkeyTest // GH-2386
	void multipleListenShouldTrackSubscriptions() throws Exception {

		ReactiveValkeyMessageListenerContainer container = new ReactiveValkeyMessageListenerContainer(connectionFactory);

		Flux<? extends ReactiveSubscription.Message<String, String>> c1 = container.receiveLater(ChannelTopic.of(CHANNEL1))
				.block();
		Flux<? extends ReactiveSubscription.Message<String, String>> c1p1 = container
				.receiveLater(Arrays.asList(ChannelTopic.of(CHANNEL1), PatternTopic.of(PATTERN1)),
						SerializationPair.fromSerializer(ValkeySerializer.string()),
						SerializationPair.fromSerializer(ValkeySerializer.string()))
				.block();

		BlockingQueue<ReactiveSubscription.Message<String, String>> c1Collector = new LinkedBlockingDeque<>();
		BlockingQueue<ReactiveSubscription.Message<String, String>> c2Collector = new LinkedBlockingDeque<>();

		Disposable c1Subscription = c1.doOnNext(c1Collector::add).subscribe();
		Disposable c2Subscription = c1p1.doOnNext(c2Collector::add).subscribe();

		doPublish(CHANNEL1.getBytes(), MESSAGE.getBytes());

		assertThat(c1Collector.poll(5, TimeUnit.SECONDS)).isNotNull();
		assertThat(c2Collector.poll(5, TimeUnit.SECONDS)).isNotNull();
		c1Collector.clear();
		c2Collector.clear();

		c2Subscription.dispose();

		Thread.sleep(200);

		doPublish(CHANNEL1.getBytes(), MESSAGE.getBytes());

		assertThat(c1Collector.poll(5, TimeUnit.SECONDS)).isNotNull();
		assertThat(c2Collector.poll(100, TimeUnit.MILLISECONDS)).isNull();

		c1Subscription.dispose();

		doPublish(CHANNEL1.getBytes(), MESSAGE.getBytes());

		assertThat(c1Collector.poll(100, TimeUnit.MILLISECONDS)).isNull();
		assertThat(c2Collector.poll(100, TimeUnit.MILLISECONDS)).isNull();
	}

	private void doPublish(byte[] channel, byte[] message) {
		reactiveConnection.pubSubCommands().publish(ByteBuffer.wrap(channel), ByteBuffer.wrap(message)).subscribe();
	}

	private static Runnable awaitSubscription(Supplier<Collection<ReactiveSubscription>> activeSubscriptions) {

		return () -> {
			Awaitility.await().until(() -> !activeSubscriptions.get().isEmpty());
		};
	}

	interface CompositeListener extends MessageListener, SubscriptionListener {

	}
}
