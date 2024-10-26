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
import static org.junit.jupiter.api.condition.OS.*;

import org.junit.jupiter.api.condition.DisabledOnOs;
import org.springframework.data.valkey.connection.convert.Converters;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.valkey.ObjectFactory;
import org.springframework.data.valkey.RawObjectFactory;
import org.springframework.data.valkey.SettingsUtils;
import org.springframework.data.valkey.StringObjectFactory;
import org.springframework.data.valkey.connection.ValkeyConnection;
import org.springframework.data.valkey.connection.ValkeyConnectionFactory;
import org.springframework.data.valkey.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.valkey.serializer.ValkeySerializationContext;
import org.springframework.data.valkey.serializer.StringValkeySerializer;
import org.springframework.data.valkey.test.condition.EnabledOnCommand;
import org.springframework.data.valkey.test.extension.LettuceTestClientResources;
import org.springframework.data.valkey.test.extension.parametrized.MethodSource;
import org.springframework.data.valkey.test.extension.parametrized.ParameterizedValkeyTest;

/**
 * Integration tests for {@link DefaultReactiveHashOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@MethodSource("testParams")
@SuppressWarnings("unchecked")
public class DefaultReactiveHashOperationsIntegrationTests<K, HK, HV> {

	private final ReactiveValkeyTemplate<K, ?> valkeyTemplate;
	private final ReactiveHashOperations<K, HK, HV> hashOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<HK> hashKeyFactory;
	private final ObjectFactory<HV> hashValueFactory;

	public DefaultReactiveHashOperationsIntegrationTests(ReactiveValkeyTemplate<K, ?> valkeyTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<HK> hashKeyFactory, ObjectFactory<HV> hashValueFactory) {

		this.valkeyTemplate = valkeyTemplate;
		this.hashOperations = valkeyTemplate.opsForHash();
		this.keyFactory = keyFactory;
		this.hashKeyFactory = hashKeyFactory;
		this.hashValueFactory = hashValueFactory;
	}

	public static Collection<Object[]> testParams() {

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory();
		lettuceConnectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		lettuceConnectionFactory.setPort(SettingsUtils.getPort());
		lettuceConnectionFactory.setHostName(SettingsUtils.getHost());
		lettuceConnectionFactory.afterPropertiesSet();
		lettuceConnectionFactory.start();

		ValkeySerializationContext<String, String> serializationContext = ValkeySerializationContext
				.fromSerializer(StringValkeySerializer.UTF_8);
		ReactiveValkeyTemplate<String, String> stringTemplate = new ReactiveValkeyTemplate<>(lettuceConnectionFactory,
				serializationContext);

		ReactiveValkeyTemplate<byte[], byte[]> rawTemplate = new ReactiveValkeyTemplate(lettuceConnectionFactory,
				ValkeySerializationContext.raw());

		return Arrays.asList(new Object[][] { { stringTemplate, stringFactory, stringFactory, stringFactory, "String" },
				{ rawTemplate, rawFactory, rawFactory, rawFactory, "raw" } });
	}

	@BeforeEach
	void before() {

		ValkeyConnectionFactory connectionFactory = (ValkeyConnectionFactory) valkeyTemplate.getConnectionFactory();
		ValkeyConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void remove() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.remove(key, hashkey1, hashkey2) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void hasKey() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.hasKey(key, hashkey) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.hasKey(key, hashKeyFactory.instance()) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void get() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.get(key, hashkey) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-824
	void getAbsentKey() {

		hashOperations.get(keyFactory.instance(), hashKeyFactory.instance()).as(StepVerifier::create) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void multiGet() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.multiGet(key, Arrays.asList(hashkey1, hashkey2)).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).containsSequence(hashvalue1, hashvalue2);
				}) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-824
	void multiGetAbsentKeys() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		hashOperations.multiGet(keyFactory.instance(), Arrays.asList(hashKeyFactory.instance(), hashKeyFactory.instance()))
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).containsSequence(null, null);
				}) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void increment() {

		assumeThat(hashValueFactory instanceof StringObjectFactory).isTrue();

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = (HV) "1";

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.increment(key, hashkey, 1L) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		hashOperations.get(key, hashkey) //
				.as(StepVerifier::create) //
				.expectNext((HV) "2") //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // GH-2048
	@EnabledOnCommand("HRANDFIELD")
	void randomField() {

		assumeThat(hashValueFactory).isNotInstanceOf(RawObjectFactory.class);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		hashOperations.put(key, hashkey1, hashvalue1) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.put(key, hashkey2, hashvalue2) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.randomKey(key) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).isIn(hashkey1, hashkey2);
				}).verifyComplete();

		hashOperations.randomKeys(key, -10) //
				.collectList().as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).hasSize(10);
				}).verifyComplete();
	}

	@ParameterizedValkeyTest // GH-2048
	@EnabledOnCommand("HRANDFIELD")
	void randomValue() {

		assumeThat(hashValueFactory).isNotInstanceOf(RawObjectFactory.class);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		hashOperations.put(key, hashkey1, hashvalue1) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.put(key, hashkey2, hashvalue2) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.randomEntry(key) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					if (actual.getKey().equals(hashkey1)) {
						assertThat(actual.getValue()).isEqualTo(hashvalue1);
					} else {
						assertThat(actual.getValue()).isEqualTo(hashvalue2);
					}
				}).verifyComplete();

		hashOperations.randomEntries(key, -10) //
				.collectList().as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).hasSize(10);
				}).verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	@DisabledOnOs(value = MAC, architectures = "aarch64")
	@SuppressWarnings("unchecked")
	void incrementDouble() {

		assumeThat(hashValueFactory instanceof StringObjectFactory).isTrue();

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = (HV) "1";

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.increment(key, hashkey, 1.1d) //
				.as(StepVerifier::create) //
				.expectNext(2.1d) //
				.verifyComplete();

		hashOperations.get(key, hashkey) //
				.as(StepVerifier::create) //
				.expectNext((HV) "2.1") //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void keys() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory).isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.keys(key).buffer(2) //
				.as(StepVerifier::create) //
				.consumeNextWith(list -> assertThat(list).containsExactlyInAnyOrder(hashkey1, hashkey2)) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void size() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void putAll() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.hasKey(key, hashkey1) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void put() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void putIfAbsent() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		hashOperations.putIfAbsent(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.putIfAbsent(key, hashkey, hashvalue2) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void values() {

		assumeThat(hashValueFactory instanceof StringObjectFactory).isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.values(key) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void entries() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.entries(key).buffer(2) //
				.as(StepVerifier::create) //
				.consumeNextWith(list -> {

					Entry<HK, HV> entry1 = Converters.entryOf(hashkey1, hashvalue1);
					Entry<HK, HV> entry2 = Converters.entryOf(hashkey2, hashvalue2);

					assertThat(list).containsExactlyInAnyOrder(entry1, entry2);
				}) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-743
	void scan() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.scan(key).buffer(2) //
				.as(StepVerifier::create) //
				.consumeNextWith(list -> {

					Entry<HK, HV> entry1 = Converters.entryOf(hashkey1, hashvalue1);
					Entry<HK, HV> entry2 = Converters.entryOf(hashkey2, hashvalue2);

					assertThat(list).containsExactlyInAnyOrder(entry1, entry2);
				}) //
				.verifyComplete();
	}

	@ParameterizedValkeyTest // DATAREDIS-602
	void delete() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.delete(key) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	private void putAll(K key, HK hashkey1, HV hashvalue1, HK hashkey2, HV hashvalue2) {

		Map<HK, HV> map = new HashMap<>();
		map.put(hashkey1, hashvalue1);
		map.put(hashkey2, hashvalue2);

		hashOperations.putAll(key, map) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}
}
