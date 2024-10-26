/*
 * Copyright 2016-2024 the original author or authors.
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
package org.springframework.data.valkey.core.convert;

import java.util.Map;

import org.springframework.data.valkey.core.ValkeyCallback;
import org.springframework.data.valkey.core.ValkeyKeyValueAdapter;
import org.springframework.data.valkey.core.ValkeyOperations;
import org.springframework.data.valkey.core.convert.BinaryConverters.StringToBytesConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ReferenceResolver} using {@link ValkeyKeyValueAdapter} to read raw data.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class ReferenceResolverImpl implements ReferenceResolver {

	private final ValkeyOperations<?, ?> redisOps;
	private final StringToBytesConverter converter;

	/**
	 * @param redisOperations must not be {@literal null}.
	 */
	public ReferenceResolverImpl(ValkeyOperations<?, ?> redisOperations) {

		Assert.notNull(redisOperations, "ValkeyOperations must not be null");

		this.redisOps = redisOperations;
		this.converter = new StringToBytesConverter();
	}

	@Override
	@Nullable
	public Map<byte[], byte[]> resolveReference(Object id, String keyspace) {

		byte[] key = converter.convert(keyspace + ":" + id);

		return redisOps.execute((ValkeyCallback<Map<byte[], byte[]>>) connection -> connection.hGetAll(key));
	}
}
