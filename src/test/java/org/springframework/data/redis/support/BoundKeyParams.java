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
package org.springframework.data.redis.support;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.StringValkeyTemplate;
import org.springframework.data.redis.support.atomic.ValkeyAtomicInteger;
import org.springframework.data.redis.support.atomic.ValkeyAtomicLong;
import org.springframework.data.redis.support.collections.DefaultValkeyMap;
import org.springframework.data.redis.support.collections.DefaultValkeySet;
import org.springframework.data.redis.support.collections.ValkeyList;
import org.springframework.data.redis.test.extension.ValkeyStanalone;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
public class BoundKeyParams {

	public static Collection<Object[]> testParams() {
		// Jedis
		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);

		StringValkeyTemplate templateJS = new StringValkeyTemplate(jedisConnFactory);
		DefaultValkeyMap mapJS = new DefaultValkeyMap("bound:key:map", templateJS);
		DefaultValkeySet setJS = new DefaultValkeySet("bound:key:set", templateJS);
		ValkeyList list = ValkeyList.create("bound:key:list", templateJS);

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);

		StringValkeyTemplate templateLT = new StringValkeyTemplate(lettuceConnFactory);
		DefaultValkeyMap mapLT = new DefaultValkeyMap("bound:key:mapLT", templateLT);
		DefaultValkeySet setLT = new DefaultValkeySet("bound:key:setLT", templateLT);
		ValkeyList listLT = ValkeyList.create("bound:key:listLT", templateLT);

		StringObjectFactory sof = new StringObjectFactory();

		return Arrays
				.asList(new Object[][] { { new ValkeyAtomicInteger("bound:key:int", jedisConnFactory), sof, templateJS },
						{ new ValkeyAtomicLong("bound:key:long", jedisConnFactory), sof, templateJS }, { list, sof, templateJS },
						{ setJS, sof, templateJS }, { mapJS, sof, templateJS },
						{ new ValkeyAtomicInteger("bound:key:intLT", lettuceConnFactory), sof, templateLT },
						{ new ValkeyAtomicLong("bound:key:longLT", lettuceConnFactory), sof, templateLT },
						{ listLT, sof, templateLT }, { setLT, sof, templateLT }, { mapLT, sof, templateLT } });
	}
}
