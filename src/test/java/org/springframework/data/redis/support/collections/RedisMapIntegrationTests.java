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
package org.springframework.data.redis.support.collections;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.ValkeyTemplate;
import org.springframework.data.redis.core.StringValkeyTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonValkeySerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringValkeySerializer;
import org.springframework.data.redis.test.XstreamOxmSerializerSingleton;
import org.springframework.data.redis.test.extension.ValkeyStanalone;

/**
 * Integration test for ValkeyMap.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ValkeyMapIntegrationTests extends AbstractValkeyMapIntegrationTests<Object, Object> {

	@SuppressWarnings("rawtypes")
	public ValkeyMapIntegrationTests(ObjectFactory<Object> keyFactory, ObjectFactory<Object> valueFactory,
			ValkeyTemplate template) {
		super(keyFactory, valueFactory, template);
	}

	@SuppressWarnings("unchecked")
	ValkeyMap<Object, Object> createMap() {
		String redisName = getClass().getSimpleName();
		return new DefaultValkeyMap<Object, Object>(redisName, template);
	}

	// DATAREDIS-241
	@SuppressWarnings("rawtypes")
	public static Collection<Object[]> testParams() {

		OxmSerializer serializer = XstreamOxmSerializerSingleton.getInstance();
		Jackson2JsonValkeySerializer<Person> jackson2JsonSerializer = new Jackson2JsonValkeySerializer<>(Person.class);
		Jackson2JsonValkeySerializer<String> jackson2JsonStringSerializer = new Jackson2JsonValkeySerializer<>(
				String.class);
		StringValkeySerializer stringSerializer = StringValkeySerializer.UTF_8;

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();
		ObjectFactory<String> doubleFactory = new DoubleAsStringObjectFactory();
		ObjectFactory<String> longFactory = new LongAsStringObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);

		ValkeyTemplate genericTemplate = new ValkeyTemplate();
		genericTemplate.setConnectionFactory(jedisConnFactory);
		genericTemplate.afterPropertiesSet();

		ValkeyTemplate<String, String> xstreamGenericTemplate = new ValkeyTemplate<>();
		xstreamGenericTemplate.setConnectionFactory(jedisConnFactory);
		xstreamGenericTemplate.setDefaultSerializer(serializer);
		xstreamGenericTemplate.afterPropertiesSet();

		ValkeyTemplate<String, Person> jackson2JsonPersonTemplate = new ValkeyTemplate<>();
		jackson2JsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jackson2JsonPersonTemplate.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplate.afterPropertiesSet();

		ValkeyTemplate<String, byte[]> rawTemplate = new ValkeyTemplate<>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(jedisConnFactory);
		rawTemplate.setKeySerializer(stringSerializer);
		rawTemplate.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class, false);

		ValkeyTemplate genericTemplateLettuce = new ValkeyTemplate();
		genericTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		genericTemplateLettuce.afterPropertiesSet();

		ValkeyTemplate<String, Person> xGenericTemplateLettuce = new ValkeyTemplate<>();
		xGenericTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		xGenericTemplateLettuce.setDefaultSerializer(serializer);
		xGenericTemplateLettuce.afterPropertiesSet();

		ValkeyTemplate<String, Person> jackson2JsonPersonTemplateLettuce = new ValkeyTemplate<>();
		jackson2JsonPersonTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		jackson2JsonPersonTemplateLettuce.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLettuce.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLettuce.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplateLettuce.afterPropertiesSet();

		ValkeyTemplate<String, String> stringTemplateLtc = new StringValkeyTemplate();
		stringTemplateLtc.setConnectionFactory(lettuceConnFactory);
		stringTemplateLtc.afterPropertiesSet();

		ValkeyTemplate<String, byte[]> rawTemplateLtc = new ValkeyTemplate<>();
		rawTemplateLtc.setEnableDefaultSerializer(false);
		rawTemplateLtc.setConnectionFactory(lettuceConnFactory);
		rawTemplateLtc.setKeySerializer(stringSerializer);
		rawTemplateLtc.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringFactory, genericTemplate },
				{ personFactory, personFactory, genericTemplate }, //
				{ stringFactory, personFactory, genericTemplate }, //
				{ personFactory, stringFactory, genericTemplate }, //
				{ personFactory, stringFactory, xstreamGenericTemplate }, //
				{ personFactory, stringFactory, jackson2JsonPersonTemplate }, //
				{ rawFactory, rawFactory, rawTemplate }, //

				// lettuce
				{ stringFactory, stringFactory, genericTemplateLettuce }, //
				{ personFactory, personFactory, genericTemplateLettuce }, //
				{ stringFactory, personFactory, genericTemplateLettuce }, //
				{ personFactory, stringFactory, genericTemplateLettuce }, //
				{ personFactory, stringFactory, xGenericTemplateLettuce }, //
				{ personFactory, stringFactory, jackson2JsonPersonTemplateLettuce }, //
				{ stringFactory, doubleFactory, stringTemplateLtc }, //
				{ stringFactory, longFactory, stringTemplateLtc }, //
				{ rawFactory, rawFactory, rawTemplateLtc } });
	}
}
