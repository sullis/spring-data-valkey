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
package org.springframework.data.valkey.hash;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.springframework.data.convert.CustomConversions;
import org.springframework.data.valkey.core.convert.IndexResolver;
import org.springframework.data.valkey.core.convert.IndexedData;
import org.springframework.data.valkey.core.convert.MappingValkeyConverter;
import org.springframework.data.valkey.core.convert.ValkeyConverter;
import org.springframework.data.valkey.core.convert.ValkeyCustomConversions;
import org.springframework.data.valkey.core.convert.ValkeyData;
import org.springframework.data.valkey.core.convert.ReferenceResolver;
import org.springframework.data.valkey.core.mapping.ValkeyMappingContext;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link HashMapper} based on {@link MappingValkeyConverter}. Supports nested properties and simple types like
 * {@link String}.
 *
 * <pre>
 * <code>
 * class Person {
 *
 *   String firstname;
 *   String lastname;
 *
 *   List&lt;String&gt; nicknames;
 *   List&lt;Person&gt; coworkers;
 *
 *   Address address;
 * }
 * </code>
 * </pre>
 *
 * The above is represented as:
 *
 * <pre>
 * <code>
 * _class=org.example.Person
 * firstname=rand
 * lastname=al'thor
 * coworkers.[0].firstname=mat
 * coworkers.[0].nicknames.[0]=prince of the ravens
 * coworkers.[1].firstname=perrin
 * coworkers.[1].address.city=two rivers
 * </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 */
public class ObjectHashMapper implements HashMapper<Object, byte[], byte[]> {

	@Nullable private volatile static ObjectHashMapper sharedInstance;

	private final ValkeyConverter converter;

	/**
	 * Creates new {@link ObjectHashMapper}.
	 */
	public ObjectHashMapper() {
		this(new ValkeyCustomConversions());
	}

	/**
	 * Creates a new {@link ObjectHashMapper} using the given {@link ValkeyConverter} for conversion.
	 *
	 * @param converter must not be {@literal null}.
	 * @throws IllegalArgumentException if the given {@literal converter} is {@literal null}.
	 * @since 2.4
	 */
	public ObjectHashMapper(ValkeyConverter converter) {

		Assert.notNull(converter, "Converter must not be null");
		this.converter = converter;
	}

	/**
	 * Creates new {@link ObjectHashMapper}.
	 *
	 * @param customConversions can be {@literal null}.
	 * @since 2.0
	 */
	public ObjectHashMapper(@Nullable CustomConversions customConversions) {

		MappingValkeyConverter mappingConverter = new MappingValkeyConverter(new ValkeyMappingContext(),
				new NoOpIndexResolver(), new NoOpReferenceResolver());
		mappingConverter.setCustomConversions(customConversions == null ? new ValkeyCustomConversions() : customConversions);
		mappingConverter.afterPropertiesSet();

		converter = mappingConverter;
	}

	/**
	 * Return a shared default {@link ObjectHashMapper} instance, lazily building it once needed.
	 * <p>
	 * <b>NOTE:</b> We highly recommend constructing individual {@link ObjectHashMapper} instances for customization
	 * purposes. This accessor is only meant as a fallback for code paths which need simple type coercion but cannot
	 * access a longer-lived {@link ObjectHashMapper} instance any other way.
	 *
	 * @return the shared {@link ObjectHashMapper} instance (never {@literal null}).
	 * @since 2.4
	 */
	public static ObjectHashMapper getSharedInstance() {

		ObjectHashMapper cs = sharedInstance;
		if (cs == null) {
			synchronized (ObjectHashMapper.class) {
				cs = sharedInstance;
				if (cs == null) {
					cs = new ObjectHashMapper();
					sharedInstance = cs;
				}
			}
		}
		return cs;
	}

	@Override
	public Map<byte[], byte[]> toHash(Object source) {

		if (source == null) {
			return Collections.emptyMap();
		}

		ValkeyData sink = new ValkeyData();
		converter.write(source, sink);
		return sink.getBucket().rawMap();
	}

	@Override
	public Object fromHash(Map<byte[], byte[]> hash) {

		if (hash == null || hash.isEmpty()) {
			return null;
		}

		return converter.read(Object.class, new ValkeyData(hash));
	}

	/**
	 * Convert a {@code hash} (map) to an object and return the casted result.
	 *
	 * @param hash
	 * @param type
	 * @param <T>
	 * @return
	 */
	public <T> T fromHash(Map<byte[], byte[]> hash, Class<T> type) {
		return type.cast(fromHash(hash));
	}

	/**
	 * {@link ReferenceResolver} implementation always returning an empty {@link Map}.
	 *
	 * @author Christoph Strobl
	 */
	private static class NoOpReferenceResolver implements ReferenceResolver {

		private static final Map<byte[], byte[]> NO_REFERENCE = Collections.emptyMap();

		@Override
		public Map<byte[], byte[]> resolveReference(Object id, String keyspace) {
			return NO_REFERENCE;
		}
	}

	/**
	 * {@link IndexResolver} always returning an empty {@link Set}.
	 *
	 * @author Christoph Strobl
	 */
	private static class NoOpIndexResolver implements IndexResolver {

		private static final Set<IndexedData> NO_INDEXES = Collections.emptySet();

		@Override
		public Set<IndexedData> resolveIndexesFor(TypeInformation<?> typeInformation, Object value) {
			return NO_INDEXES;
		}

		@Override
		public Set<IndexedData> resolveIndexesFor(String keyspace, String path, TypeInformation<?> typeInformation,
				Object value) {
			return NO_INDEXES;
		}
	}
}
