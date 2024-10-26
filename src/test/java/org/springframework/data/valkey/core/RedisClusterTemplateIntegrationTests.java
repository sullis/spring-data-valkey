/*
 * Copyright 2015-2024 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.Disabled;
import org.junit.runners.Parameterized.Parameters;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.valkey.LongObjectFactory;
import org.springframework.data.valkey.ObjectFactory;
import org.springframework.data.valkey.Person;
import org.springframework.data.valkey.PersonObjectFactory;
import org.springframework.data.valkey.RawObjectFactory;
import org.springframework.data.valkey.StringObjectFactory;
import org.springframework.data.valkey.connection.jedis.JedisConnectionFactory;
import org.springframework.data.valkey.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.valkey.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.valkey.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.valkey.serializer.GenericToStringSerializer;
import org.springframework.data.valkey.serializer.Jackson2JsonValkeySerializer;
import org.springframework.data.valkey.serializer.OxmSerializer;
import org.springframework.data.valkey.serializer.StringValkeySerializer;
import org.springframework.data.valkey.test.XstreamOxmSerializerSingleton;
import org.springframework.data.valkey.test.condition.EnabledOnValkeyClusterAvailable;
import org.springframework.data.valkey.test.extension.ValkeyCluster;
import org.springframework.data.valkey.test.extension.parametrized.ParameterizedValkeyTest;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@EnabledOnValkeyClusterAvailable
public class ValkeyClusterTemplateIntegrationTests<K, V> extends ValkeyTemplateIntegrationTests<K, V> {

	public ValkeyClusterTemplateIntegrationTests(ValkeyTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {
		super(redisTemplate, keyFactory, valueFactory);
	}

	@ParameterizedValkeyTest
	@Disabled("Pipeline not supported in cluster mode")
	public void testExecutePipelinedNonNullValkeyCallback() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(super::testExecutePipelinedNonNullValkeyCallback);
	}

	@ParameterizedValkeyTest
	@Disabled("Pipeline not supported in cluster mode")
	public void testExecutePipelinedTx() {
		super.testExecutePipelinedTx();
	}

	@ParameterizedValkeyTest
	@Disabled("Watch only supported on same connection...")
	public void testWatch() {
		super.testWatch();
	}

	@ParameterizedValkeyTest
	@Disabled("Watch only supported on same connection...")
	public void testUnwatch() {
		super.testUnwatch();
	}

	@ParameterizedValkeyTest
	@Disabled("EXEC only supported on same connection...")
	public void testExec() {
		super.testExec();
	}

	@ParameterizedValkeyTest
	@Disabled("Pipeline not supported in cluster mode")
	public void testExecutePipelinedNonNullSessionCallback() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(super::testExecutePipelinedNonNullSessionCallback);
	}

	@ParameterizedValkeyTest
	@Disabled("PubSub not supported in cluster mode")
	public void testConvertAndSend() {
		super.testConvertAndSend();
	}

	@ParameterizedValkeyTest
	@Disabled("Watch only supported on same connection...")
	public void testExecConversionDisabled() {
		super.testExecConversionDisabled();
	}

	@ParameterizedValkeyTest
	@Disabled("Discard only supported on same connection...")
	public void testDiscard() {
		super.testDiscard();
	}

	@ParameterizedValkeyTest
	@Disabled("Pipleline not supported in cluster mode")
	public void testExecutePipelined() {
		super.testExecutePipelined();
	}

	@ParameterizedValkeyTest
	@Disabled("Watch only supported on same connection...")
	public void testWatchMultipleKeys() {
		super.testWatchMultipleKeys();
	}

	@ParameterizedValkeyTest
	@Disabled("This one fails when using GET options on numbers")
	public void testSortBulkMapper() {
		super.testSortBulkMapper();
	}

	@ParameterizedValkeyTest
	@Disabled("This one fails when using GET options on numbers")
	public void testGetExpireMillisUsingTransactions() {
		super.testGetExpireMillisUsingTransactions();
	}

	@ParameterizedValkeyTest
	@Disabled("This one fails when using GET options on numbers")
	public void testGetExpireMillisUsingPipelining() {
		super.testGetExpireMillisUsingPipelining();
	}

	@ParameterizedValkeyTest
	void testScan() {

		// Only Lettuce supports cluster-wide scanning
		assumeThat(redisTemplate.getConnectionFactory()).isInstanceOf(LettuceConnectionFactory.class);
		super.testScan();
	}

	@Parameters
	public static Collection<Object[]> testParams() {

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Long> longFactory = new LongObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();


		OxmSerializer serializer = XstreamOxmSerializerSingleton.getInstance();
		Jackson2JsonValkeySerializer<Person> jackson2JsonSerializer = new Jackson2JsonValkeySerializer<>(Person.class);

		// JEDIS
		JedisConnectionFactory jedisConnectionFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(ValkeyCluster.class);

		jedisConnectionFactory.afterPropertiesSet();

		ValkeyTemplate<String, String> jedisStringTemplate = new ValkeyTemplate<>();
		jedisStringTemplate.setDefaultSerializer(StringValkeySerializer.UTF_8);
		jedisStringTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisStringTemplate.afterPropertiesSet();

		ValkeyTemplate<String, Long> jedisLongTemplate = new ValkeyTemplate<>();
		jedisLongTemplate.setKeySerializer(StringValkeySerializer.UTF_8);
		jedisLongTemplate.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		jedisLongTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisLongTemplate.afterPropertiesSet();

		ValkeyTemplate<byte[], byte[]> jedisRawTemplate = new ValkeyTemplate<>();
		jedisRawTemplate.setEnableDefaultSerializer(false);
		jedisRawTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisRawTemplate.afterPropertiesSet();

		ValkeyTemplate<String, Person> jedisPersonTemplate = new ValkeyTemplate<>();
		jedisPersonTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisPersonTemplate.afterPropertiesSet();

		ValkeyTemplate<String, String> jedisXstreamStringTemplate = new ValkeyTemplate<>();
		jedisXstreamStringTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisXstreamStringTemplate.setDefaultSerializer(serializer);
		jedisXstreamStringTemplate.afterPropertiesSet();

		ValkeyTemplate<String, Person> jedisJackson2JsonPersonTemplate = new ValkeyTemplate<>();
		jedisJackson2JsonPersonTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisJackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		jedisJackson2JsonPersonTemplate.afterPropertiesSet();

		// LETTUCE

		LettuceConnectionFactory lettuceConnectionFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyCluster.class);

		ValkeyTemplate<String, String> lettuceStringTemplate = new ValkeyTemplate<>();
		lettuceStringTemplate.setDefaultSerializer(StringValkeySerializer.UTF_8);
		lettuceStringTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceStringTemplate.afterPropertiesSet();

		ValkeyTemplate<String, Long> lettuceLongTemplate = new ValkeyTemplate<>();
		lettuceLongTemplate.setKeySerializer(StringValkeySerializer.UTF_8);
		lettuceLongTemplate.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		lettuceLongTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceLongTemplate.afterPropertiesSet();

		ValkeyTemplate<byte[], byte[]> lettuceRawTemplate = new ValkeyTemplate<>();
		lettuceRawTemplate.setEnableDefaultSerializer(false);
		lettuceRawTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceRawTemplate.afterPropertiesSet();

		ValkeyTemplate<String, Person> lettucePersonTemplate = new ValkeyTemplate<>();
		lettucePersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettucePersonTemplate.afterPropertiesSet();

		ValkeyTemplate<String, String> lettuceXstreamStringTemplate = new ValkeyTemplate<>();
		lettuceXstreamStringTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceXstreamStringTemplate.setDefaultSerializer(serializer);
		lettuceXstreamStringTemplate.afterPropertiesSet();

		ValkeyTemplate<String, Person> lettuceJackson2JsonPersonTemplate = new ValkeyTemplate<>();
		lettuceJackson2JsonPersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceJackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		lettuceJackson2JsonPersonTemplate.afterPropertiesSet();

		return Arrays.asList(new Object[][] { //

				// JEDIS
				{ jedisStringTemplate, stringFactory, stringFactory }, //
				{ jedisLongTemplate, stringFactory, longFactory }, //
				{ jedisRawTemplate, rawFactory, rawFactory }, //
				{ jedisPersonTemplate, stringFactory, personFactory }, //
				{ jedisXstreamStringTemplate, stringFactory, stringFactory }, //
				{ jedisJackson2JsonPersonTemplate, stringFactory, personFactory }, //

				// LETTUCE
				{ lettuceStringTemplate, stringFactory, stringFactory }, //
				{ lettuceLongTemplate, stringFactory, longFactory }, //
				{ lettuceRawTemplate, rawFactory, rawFactory }, //
				{ lettucePersonTemplate, stringFactory, personFactory }, //
				{ lettuceXstreamStringTemplate, stringFactory, stringFactory }, //
				{ lettuceJackson2JsonPersonTemplate, stringFactory, personFactory }, //
		});
	}

}
