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
package org.springframework.data.valkey.core;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.valkey.ObjectFactory;
import org.springframework.data.valkey.Person;
import org.springframework.data.valkey.PersonObjectFactory;
import org.springframework.data.valkey.StringObjectFactory;
import org.springframework.data.valkey.connection.DataType;
import org.springframework.data.valkey.connection.ReactiveValkeyClusterConnection;
import org.springframework.data.valkey.connection.ReactiveSubscription.ChannelMessage;
import org.springframework.data.valkey.connection.ReactiveSubscription.Message;
import org.springframework.data.valkey.connection.ReactiveSubscription.PatternMessage;
import org.springframework.data.valkey.connection.ValkeyConnection;
import org.springframework.data.valkey.connection.ValkeyConnectionFactory;
import org.springframework.data.valkey.core.ReactiveOperationsTestParams.Fixture;
import org.springframework.data.valkey.core.script.DefaultValkeyScript;
import org.springframework.data.valkey.serializer.Jackson2JsonValkeySerializer;
import org.springframework.data.valkey.serializer.JdkSerializationValkeySerializer;
import org.springframework.data.valkey.serializer.ValkeyElementReader;
import org.springframework.data.valkey.serializer.ValkeySerializationContext;
import org.springframework.data.valkey.serializer.ValkeySerializationContext.SerializationPair;
import org.springframework.data.valkey.serializer.StringValkeySerializer;
import org.springframework.data.valkey.test.condition.EnabledIfLongRunningTest;
import org.springframework.data.valkey.test.condition.EnabledOnCommand;
import org.springframework.data.valkey.test.extension.parametrized.MethodSource;
import org.springframework.data.valkey.test.extension.parametrized.ParameterizedValkeyTest;

/**
 * Integration tests for {@link ReactiveValkeyTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@MethodSource("testParams")
public class ReactiveValkeyTemplateIntegrationTests<K, V> {

	private final ReactiveValkeyTemplate<K, V> redisTemplate;

	private final ObjectFactory<K> keyFactory;

	private final ObjectFactory<V> valueFactory;

	public static Collection<Fixture<?, ?>> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	public ReactiveValkeyTemplateIntegrationTests(Fixture<K, V> fixture) {

		this.redisTemplate = fixture.getTemplate();
		this.keyFactory = fixture.getKeyFactory();
		this.valueFactory = fixture.getValueFactory();
	}

	@BeforeEach
	void before() {

		ValkeyConnectionFactory connectionFactory = (ValkeyConnectionFactory) redisTemplate.getConnectionFactory();
		ValkeyConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@ParameterizedValkeyTest // GH-2040
	@EnabledOnCommand("COPY")
	void copy() {

		try (ReactiveValkeyClusterConnection connection = redisTemplate.getConnectionFactory()
				.getReactiveClusterConnection()){
			assumeThat(connection).isNull();
		} catch (InvalidDataAccessApiUsageException ignore) {
		}

		K key = keyFactory.instance();
		K targetKey = keyFactory.instance();
		V value = valueFactory.instance();
		V nextValue = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();
		redisTemplate.copy(key, targetKey, false).as(StepVerifier::create).expectNext(true).verifyComplete();
		redisTemplate.opsForValue().get(targetKey).as(StepVerifier::create).expectNext(value).verifyComplete();

		redisTemplate.opsForValue().set(key, nextValue).as(StepVerifier::create).expectNext(true).verifyComplete();
		redisTemplate.copy(key, targetKey, true).as(StepVerifier::create).expectNext(true).verifyComplete();
		redisTemplate.opsForValue().get(targetKey).as(StepVerifier::create).expectNext(nextValue).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void exists() {

		K key = keyFactory.instance();

		redisTemplate.hasKey(key).as(StepVerifier::create).expectNext(false).verifyComplete();

		redisTemplate.opsForValue().set(key, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.hasKey(key).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-743
	void scan() {

		assumeThat(valueFactory.instance() instanceof Person).isFalse();

		Map<K, V> tuples = new HashMap<>();
		tuples.put(keyFactory.instance(), valueFactory.instance());
		tuples.put(keyFactory.instance(), valueFactory.instance());
		tuples.put(keyFactory.instance(), valueFactory.instance());

		redisTemplate.opsForValue().multiSet(tuples).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.scan().collectList().as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).containsAll(tuples.keySet())) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void type() {

		K key = keyFactory.instance();

		redisTemplate.type(key).as(StepVerifier::create).expectNext(DataType.NONE).verifyComplete();

		redisTemplate.opsForValue().set(key, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.type(key).as(StepVerifier::create).expectNext(DataType.STRING).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void rename() {

		K oldName = keyFactory.instance();
		K newName = keyFactory.instance();

		redisTemplate.opsForValue().set(oldName, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.rename(oldName, newName).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void renameNx() {

		K oldName = keyFactory.instance();
		K existing = keyFactory.instance();
		K newName = keyFactory.instance();

		redisTemplate.opsForValue().set(oldName, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		redisTemplate.opsForValue().set(existing, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.renameIfAbsent(oldName, newName).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		redisTemplate.opsForValue().set(existing, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.renameIfAbsent(newName, existing).as(StepVerifier::create).expectNext(false) //
				.expectComplete() //
				.verify();
	}

	@ParameterizedValkeyTest // DATAREDIS-693
	void unlink() {

		K single = keyFactory.instance();

		redisTemplate.opsForValue().set(single, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.unlink(single).as(StepVerifier::create).expectNext(1L).verifyComplete();

		redisTemplate.hasKey(single).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-693
	void unlinkMany() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		redisTemplate.opsForValue().set(key1, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		redisTemplate.opsForValue().set(key2, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.unlink(key1, key2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		redisTemplate.hasKey(key1).as(StepVerifier::create).expectNext(false).verifyComplete();
		redisTemplate.hasKey(key2).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-913
	void unlinkManyPublisher() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		assumeThat(key1 instanceof String && valueFactory instanceof StringObjectFactory).isTrue();

		redisTemplate.opsForValue().set(key1, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		redisTemplate.opsForValue().set(key2, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.unlink(redisTemplate.keys((K) "*")).as(StepVerifier::create).expectNext(2L).verifyComplete();

		redisTemplate.hasKey(key1).as(StepVerifier::create).expectNext(false).verifyComplete();
		redisTemplate.hasKey(key2).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-913
	void deleteManyPublisher() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		assumeThat(key1 instanceof String && valueFactory instanceof StringObjectFactory).isTrue();

		redisTemplate.opsForValue().set(key1, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		redisTemplate.opsForValue().set(key2, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.delete(redisTemplate.keys((K) "*")).as(StepVerifier::create).expectNext(2L).verifyComplete();

		redisTemplate.hasKey(key1).as(StepVerifier::create).expectNext(false).verifyComplete();
		redisTemplate.hasKey(key2).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-683
	@SuppressWarnings("unchecked")
	void executeScript() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value instanceof Long).isFalse();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		Flux<V> execute = redisTemplate.execute(
				new DefaultValkeyScript<>("return redis.call('get', KEYS[1])", (Class<V>) value.getClass()),
				Collections.singletonList(key));

		execute.as(StepVerifier::create).expectNext(value).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-683
	void executeScriptWithElementReaderAndWriter() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		SerializationPair json = SerializationPair.fromSerializer(new Jackson2JsonValkeySerializer<>(Person.class));
		ValkeyElementReader<String> resultReader = ValkeyElementReader.from(StringValkeySerializer.UTF_8);

		assumeThat(value instanceof Long).isFalse();

		Person person = new Person("Walter", "White", 51);
		redisTemplate
				.execute(new DefaultValkeyScript<>("return redis.call('set', KEYS[1], ARGV[1])", String.class),
						Collections.singletonList(key), Collections.singletonList(person), json.getWriter(), resultReader)
				.as(StepVerifier::create)
				.expectNext("OK").verifyComplete();

		Flux<Person> execute = redisTemplate.execute(
				new DefaultValkeyScript<>("return redis.call('get', KEYS[1])", Person.class), Collections.singletonList(key),
				Collections.emptyList(), json.getWriter(), json.getReader());

		execute.as(StepVerifier::create).expectNext(person).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void expire() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.expire(key, Duration.ofSeconds(10)).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void preciseExpire() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.expire(key, Duration.ofMillis(10_001)).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void expireAt() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		Instant expireAt = Instant.ofEpochSecond(Instant.now().plus(Duration.ofSeconds(10)).getEpochSecond());

		redisTemplate.expireAt(key, expireAt).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void preciseExpireAt() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		Instant expireAt = Instant.ofEpochSecond(Instant.now().plus(Duration.ofSeconds(10)).getEpochSecond(), 5);

		redisTemplate.expireAt(key, expireAt).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void getTtlForAbsentKeyShouldCompleteWithoutValue() {

		K key = keyFactory.instance();

		redisTemplate.getExpire(key).as(StepVerifier::create).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void getTtlForKeyWithoutExpiryShouldCompleteWithZeroDuration() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create).expectNext(Duration.ZERO).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void move() {

		try (ReactiveValkeyClusterConnection connection = redisTemplate.getConnectionFactory()
				.getReactiveClusterConnection()) {
			assumeThat(connection).isNull();
		} catch (InvalidDataAccessApiUsageException ignore) {
		}

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();
		redisTemplate.move(key, 5).as(StepVerifier::create).expectNext(true).verifyComplete();
		redisTemplate.hasKey(key).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void shouldApplyCustomSerializationContextToValues() {

		Person key = new PersonObjectFactory().instance();
		Person value = new PersonObjectFactory().instance();

		JdkSerializationValkeySerializer jdkSerializer = new JdkSerializationValkeySerializer();
		ValkeySerializationContext<Object, Object> objectSerializers = ValkeySerializationContext.newSerializationContext()
				.key(jdkSerializer) //
				.value(jdkSerializer) //
				.hashKey(jdkSerializer) //
				.hashValue(jdkSerializer) //
				.build();

		ReactiveValueOperations<Object, Object> valueOperations = redisTemplate.opsForValue(objectSerializers);

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.get(key).as(StepVerifier::create).expectNext(value).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void shouldApplyCustomSerializationContextToHash() {

		ValkeySerializationContext<K, V> serializationContext = redisTemplate.getSerializationContext();

		K key = keyFactory.instance();
		String hashField = "foo";
		Person hashValue = new PersonObjectFactory().instance();

		ValkeySerializationContext<K, V> objectSerializers = ValkeySerializationContext.<K, V> newSerializationContext()
				.key(serializationContext.getKeySerializationPair()) //
				.value(serializationContext.getValueSerializationPair()) //
				.hashKey(StringValkeySerializer.UTF_8) //
				.hashValue(new JdkSerializationValkeySerializer()) //
				.build();

		ReactiveHashOperations<K, String, Object> hashOperations = redisTemplate.opsForHash(objectSerializers);

		hashOperations.put(key, hashField, hashValue).as(StepVerifier::create).expectNext(true).verifyComplete();

		hashOperations.get(key, hashField).as(StepVerifier::create).expectNext(hashValue).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-612
	@EnabledIfLongRunningTest
	void listenToChannelShouldReceiveChannelMessagesCorrectly() throws InterruptedException {

		String channel = "my-channel";

		V message = valueFactory.instance();

		redisTemplate.listenToChannel(channel).as(StepVerifier::create) //
				.thenAwait(Duration.ofMillis(500)) // just make sure we the subscription completed
				.then(() -> redisTemplate.convertAndSend(channel, message).subscribe()) //
				.assertNext(received -> {

					assertThat(received).isInstanceOf(ChannelMessage.class);
					assertThat(received.getMessage()).isEqualTo(message);
					assertThat(received.getChannel()).isEqualTo(channel);
				}) //
				.thenAwait(Duration.ofMillis(10)) //
				.thenCancel() //
				.verify(Duration.ofSeconds(3));
	}

	@ParameterizedValkeyTest // GH-1622
	@EnabledIfLongRunningTest
	void listenToLaterChannelShouldReceiveChannelMessagesCorrectly() {

		String channel = "my-channel";

		V message = valueFactory.instance();

		redisTemplate.listenToChannelLater(channel) //
				.doOnNext(it -> redisTemplate.convertAndSend(channel, message).subscribe()).flatMapMany(Function.identity()) //
				.cast(Message.class)  // why? java16 why?
				.as(StepVerifier::create) //
				.assertNext(received -> {

					assertThat(received).isInstanceOf(ChannelMessage.class);
					assertThat(received.getMessage()).isEqualTo(message);
					assertThat(received.getChannel()).isEqualTo(channel);
				}) //
				.thenAwait(Duration.ofMillis(10)) //
				.thenCancel() //
				.verify(Duration.ofSeconds(3));
	}

	@ParameterizedValkeyTest // DATAREDIS-612
	void listenToPatternShouldReceiveChannelMessagesCorrectly() {

		String channel = "my-channel";
		String pattern = "my-*";

		V message = valueFactory.instance();

		Flux<? extends Message<String, V>> stream = redisTemplate.listenToPattern(pattern);

		stream.as(StepVerifier::create) //
				.thenAwait(Duration.ofMillis(500)) // just make sure we the subscription completed
				.then(() -> redisTemplate.convertAndSend(channel, message).subscribe()) //
				.assertNext(received -> {

					assertThat(received).isInstanceOf(PatternMessage.class);
					assertThat(received.getMessage()).isEqualTo(message);
					assertThat(received.getChannel()).isEqualTo(channel);
					assertThat(((PatternMessage) received).getPattern()).isEqualTo(pattern);
				}) //
				.thenCancel() //
				.verify(Duration.ofSeconds(3));
	}

	@ParameterizedValkeyTest // GH-1622
	void listenToPatternLaterShouldReceiveChannelMessagesCorrectly() {

		String channel = "my-channel";
		String pattern = "my-*";

		V message = valueFactory.instance();

		Mono<Flux<? extends Message<String, V>>> stream = redisTemplate.listenToPatternLater(pattern);

		stream.doOnNext(it -> redisTemplate.convertAndSend(channel, message).subscribe()) //
				.flatMapMany(Function.identity()) //
				.cast(Message.class) // why? java16 why?
				.as(StepVerifier::create) //
				.assertNext(received -> {

					assertThat(received).isInstanceOf(PatternMessage.class);
					assertThat(received.getMessage()).isEqualTo(message);
					assertThat(received.getChannel()).isEqualTo(channel);
					assertThat(((PatternMessage) received).getPattern()).isEqualTo(pattern);
				}) //
				.thenCancel() //
				.verify(Duration.ofSeconds(3));
	}
}
