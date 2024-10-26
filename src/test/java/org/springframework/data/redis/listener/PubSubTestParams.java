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
import java.util.Collection;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.ValkeyTemplate;
import org.springframework.data.redis.core.StringValkeyTemplate;
import org.springframework.data.redis.test.condition.ValkeyDetector;
import org.springframework.data.redis.test.extension.ValkeyCluster;
import org.springframework.data.redis.test.extension.ValkeyStanalone;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
public class PubSubTestParams {

	public static Collection<Object[]> testParams() {
		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getNewConnectionFactory(ValkeyStanalone.class);

		jedisConnFactory.afterPropertiesSet();

		ValkeyTemplate<String, String> stringTemplate = new StringValkeyTemplate(jedisConnFactory);
		ValkeyTemplate<String, Person> personTemplate = new ValkeyTemplate<>();
		personTemplate.setConnectionFactory(jedisConnFactory);
		personTemplate.afterPropertiesSet();
		ValkeyTemplate<byte[], byte[]> rawTemplate = new ValkeyTemplate<>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(jedisConnFactory);
		rawTemplate.afterPropertiesSet();

		// add Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);

		ValkeyTemplate<String, String> stringTemplateLtc = new StringValkeyTemplate(lettuceConnFactory);
		ValkeyTemplate<String, Person> personTemplateLtc = new ValkeyTemplate<>();
		personTemplateLtc.setConnectionFactory(lettuceConnFactory);
		personTemplateLtc.afterPropertiesSet();
		ValkeyTemplate<byte[], byte[]> rawTemplateLtc = new ValkeyTemplate<>();
		rawTemplateLtc.setEnableDefaultSerializer(false);
		rawTemplateLtc.setConnectionFactory(lettuceConnFactory);
		rawTemplateLtc.afterPropertiesSet();

		Collection<Object[]> parameters = new ArrayList<>();
		parameters.add(new Object[] { stringFactory, stringTemplate });
		parameters.add(new Object[] { personFactory, personTemplate });
		parameters.add(new Object[] { stringFactory, stringTemplateLtc });
		parameters.add(new Object[] { personFactory, personTemplateLtc });

		if (clusterAvailable()) {

			// add Jedis
			JedisConnectionFactory jedisClusterFactory = JedisConnectionFactoryExtension
					.getNewConnectionFactory(ValkeyCluster.class);

			ValkeyTemplate<String, String> jedisClusterStringTemplate = new StringValkeyTemplate(jedisClusterFactory);

			// add Lettuce
			LettuceConnectionFactory lettuceClusterFactory = LettuceConnectionFactoryExtension
					.getConnectionFactory(ValkeyCluster.class);

			ValkeyTemplate<String, String> lettuceClusterStringTemplate = new StringValkeyTemplate(lettuceClusterFactory);

			parameters.add(new Object[] { stringFactory, jedisClusterStringTemplate });
			parameters.add(new Object[] { stringFactory, lettuceClusterStringTemplate });
		}

		return parameters;
	}

	private static boolean clusterAvailable() {
		return ValkeyDetector.isClusterAvailable();
	}
}
