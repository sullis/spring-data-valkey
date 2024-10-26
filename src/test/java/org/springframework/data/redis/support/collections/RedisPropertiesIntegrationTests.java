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

import static org.assertj.core.api.Assertions.*;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.ValkeyTemplate;
import org.springframework.data.redis.core.StringValkeyTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonValkeySerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.test.XstreamOxmSerializerSingleton;
import org.springframework.data.redis.test.extension.ValkeyStanalone;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedValkeyTest;

/**
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ValkeyPropertiesIntegrationTests extends ValkeyMapIntegrationTests {

	private Properties defaults = new Properties();
	private ValkeyProperties props;

	/**
	 * Constructs a new <code>ValkeyPropertiesTests</code> instance.
	 *
	 * @param keyFactory
	 * @param valueFactory
	 * @param template
	 */
	public ValkeyPropertiesIntegrationTests(ObjectFactory<Object> keyFactory, ObjectFactory<Object> valueFactory,
			ValkeyTemplate template) {
		super(keyFactory, valueFactory, template);
	}

	ValkeyMap<Object, Object> createMap() {
		String redisName = getClass().getSimpleName();
		props = new ValkeyProperties(defaults, redisName, new StringValkeyTemplate(template.getConnectionFactory()));
		return props;
	}

	protected ValkeyStore copyStore(ValkeyStore store) {
		return new ValkeyProperties(store.getKey(), store.getOperations());
	}

	@ParameterizedValkeyTest
	public void testGetOperations() {
		assertThat(map.getOperations() instanceof StringValkeyTemplate).isTrue();
	}

	@ParameterizedValkeyTest
	void testPropertiesLoad() throws Exception {
		InputStream stream = getClass()
				.getResourceAsStream("/org/springframework/data/redis/support/collections/props.properties");

		assertThat(stream).isNotNull();

		int size = props.size();

		try {
			props.load(stream);
		} finally {
			stream.close();
		}

		assertThat(props.get("foo")).isEqualTo("bar");
		assertThat(props.get("bucket")).isEqualTo("head");
		assertThat(props.get("lotus")).isEqualTo("island");
		assertThat(props.size()).isEqualTo(size + 3);
	}

	@ParameterizedValkeyTest
	void testPropertiesSave() throws Exception {
		props.setProperty("x", "y");
		props.setProperty("a", "b");

		StringWriter writer = new StringWriter();
		props.store(writer, "no-comment");
	}

	@ParameterizedValkeyTest
	void testGetProperty() throws Exception {
		String property = props.getProperty("a");
		assertThat(property).isNull();
		defaults.put("a", "x");
		assertThat(props.getProperty("a")).isEqualTo("x");
	}

	@ParameterizedValkeyTest
	void testGetPropertyDefault() throws Exception {
		assertThat(props.getProperty("a", "x")).isEqualTo("x");
	}

	@ParameterizedValkeyTest
	void testSetProperty() throws Exception {
		assertThat(props.getProperty("a")).isNull();
		defaults.setProperty("a", "x");
		assertThat(props.getProperty("a")).isEqualTo("x");
	}

	@ParameterizedValkeyTest
	void testPropertiesList() throws Exception {
		defaults.setProperty("a", "b");
		props.setProperty("x", "y");
		StringWriter wr = new StringWriter();
		props.list(new PrintWriter(wr));
	}

	@ParameterizedValkeyTest
	void testPropertyNames() throws Exception {
		String key1 = "foo";
		String key2 = "x";
		String key3 = "d";

		String val = "o";

		defaults.setProperty(key3, val);
		props.setProperty(key1, val);
		props.setProperty(key2, val);

		Enumeration<?> names = props.propertyNames();
		Set<Object> keys = new LinkedHashSet<>();
		keys.add(names.nextElement());
		keys.add(names.nextElement());
		keys.add(names.nextElement());

		assertThat(names.hasMoreElements()).isFalse();
	}

	@ParameterizedValkeyTest
	void testDefaultInit() throws Exception {
		ValkeyProperties redisProperties = new ValkeyProperties("foo", template);
		redisProperties.propertyNames();
	}

	@ParameterizedValkeyTest
	void testStringPropertyNames() throws Exception {
		String key1 = "foo";
		String key2 = "x";
		String key3 = "d";

		String val = "o";

		defaults.setProperty(key3, val);
		props.setProperty(key1, val);
		props.setProperty(key2, val);

		Set<String> keys = props.stringPropertyNames();
		assertThat(keys.contains(key1)).isTrue();
		assertThat(keys.contains(key2)).isTrue();
		assertThat(keys.contains(key3)).isTrue();
	}

	@ParameterizedValkeyTest
	@Override
	public void testScanWorksCorrectly() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> super.testScanWorksCorrectly());
	}

	// DATAREDIS-241
	public static Collection<Object[]> testParams() {

		OxmSerializer serializer = XstreamOxmSerializerSingleton.getInstance();
		Jackson2JsonValkeySerializer<Person> jackson2JsonSerializer = new Jackson2JsonValkeySerializer<>(Person.class);
		Jackson2JsonValkeySerializer<String> jackson2JsonStringSerializer = new Jackson2JsonValkeySerializer<>(
				String.class);

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<String> longFactory = new LongAsStringObjectFactory();
		ObjectFactory<String> doubleFactory = new DoubleAsStringObjectFactory();

		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);

		ValkeyTemplate<String, String> genericTemplate = new StringValkeyTemplate(jedisConnFactory);

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

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class, false);

		ValkeyTemplate<String, String> genericTemplateLtc = new StringValkeyTemplate(lettuceConnFactory);
		ValkeyTemplate<String, Person> xGenericTemplateLtc = new ValkeyTemplate<>();
		xGenericTemplateLtc.setConnectionFactory(lettuceConnFactory);
		xGenericTemplateLtc.setDefaultSerializer(serializer);
		xGenericTemplateLtc.afterPropertiesSet();

		ValkeyTemplate<String, Person> jackson2JsonPersonTemplateLtc = new ValkeyTemplate<>();
		jackson2JsonPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		jackson2JsonPersonTemplateLtc.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLtc.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLtc.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplateLtc.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringFactory, genericTemplate }, //
				{ stringFactory, stringFactory, genericTemplate }, //
				{ stringFactory, stringFactory, genericTemplate }, //
				{ stringFactory, stringFactory, genericTemplate }, //
				{ stringFactory, stringFactory, xstreamGenericTemplate }, //
				{ stringFactory, stringFactory, jackson2JsonPersonTemplate }, //

						// lettuce
						{ stringFactory, stringFactory, genericTemplateLtc }, //
						{ stringFactory, stringFactory, genericTemplateLtc }, //
						{ stringFactory, stringFactory, genericTemplateLtc }, //
						{ stringFactory, stringFactory, genericTemplateLtc }, //
						{ stringFactory, doubleFactory, genericTemplateLtc }, //
						{ stringFactory, longFactory, genericTemplateLtc }, //
						{ stringFactory, stringFactory, xGenericTemplateLtc }, //
						{ stringFactory, stringFactory, jackson2JsonPersonTemplateLtc } });
	}

}
