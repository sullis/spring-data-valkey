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
package org.springframework.data.redis.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.keyvalue.core.CriteriaAccessor;
import org.springframework.data.keyvalue.core.QueryEngine;
import org.springframework.data.keyvalue.core.SortAccessor;
import org.springframework.data.keyvalue.core.SpelSortAccessor;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.redis.connection.ValkeyConnection;
import org.springframework.data.redis.connection.ValkeyGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.ValkeyGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.core.convert.GeoIndexedPropertyValue;
import org.springframework.data.redis.core.convert.ValkeyConverter;
import org.springframework.data.redis.core.convert.ValkeyData;
import org.springframework.data.redis.core.mapping.ValkeyPersistentProperty;
import org.springframework.data.redis.repository.query.ValkeyOperationChain;
import org.springframework.data.redis.repository.query.ValkeyOperationChain.NearPath;
import org.springframework.data.redis.repository.query.ValkeyOperationChain.PathAndValue;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Valkey specific {@link QueryEngine} implementation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Junghoon Ban
 * @since 1.7
 */
class ValkeyQueryEngine extends QueryEngine<ValkeyKeyValueAdapter, ValkeyOperationChain, Comparator<?>> {

	/**
	 * Creates new {@link ValkeyQueryEngine} with defaults.
	 */
	ValkeyQueryEngine() {
		this(new ValkeyCriteriaAccessor(), new SpelSortAccessor(new SpelExpressionParser()));
	}

	/**
	 * Creates new {@link ValkeyQueryEngine}.
	 *
	 * @param criteriaAccessor
	 * @param sortAccessor
	 * @see QueryEngine#QueryEngine(CriteriaAccessor, SortAccessor)
	 */
	private ValkeyQueryEngine(CriteriaAccessor<ValkeyOperationChain> criteriaAccessor,
			@Nullable SortAccessor<Comparator<?>> sortAccessor) {
		super(criteriaAccessor, sortAccessor);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> execute(ValkeyOperationChain criteria, Comparator<?> sort, long offset, int rows, String keyspace,
			Class<T> type) {
		List<T> result = doFind(criteria, offset, rows, keyspace, type);

		if (sort != null) {
			result.sort((Comparator<? super T>) sort);
		}

		return result;
	}

	private <T> List<T> doFind(ValkeyOperationChain criteria, long offset, int rows, String keyspace, Class<T> type) {

		if (criteria == null
				|| (CollectionUtils.isEmpty(criteria.getOrSismember()) && CollectionUtils.isEmpty(criteria.getSismember()))
						&& criteria.getNear() == null) {
			return getRequiredAdapter().getAllOf(keyspace, type, offset, rows);
		}

		ValkeyCallback<Map<byte[], Map<byte[], byte[]>>> callback = connection -> {

			List<byte[]> keys = findKeys(criteria, rows, keyspace, type, connection);
			byte[] keyspaceBin = getRequiredAdapter().getConverter().getConversionService().convert(keyspace + ":",
					byte[].class);

			Map<byte[], Map<byte[], byte[]>> rawData = new LinkedHashMap<>();

			if (keys.isEmpty() || keys.size() < offset) {
				return Collections.emptyMap();
			}

			int offsetToUse = Math.max(0, (int) offset);
			if (rows > 0) {
				keys = keys.subList(Math.max(0, offsetToUse), Math.min(offsetToUse + rows, keys.size()));
			}
			for (byte[] id : keys) {

				byte[] singleKey = ByteUtils.concat(keyspaceBin, id);
				rawData.put(id, connection.hGetAll(singleKey));
			}

			return rawData;
		};

		Map<byte[], Map<byte[], byte[]>> raw = this.getRequiredAdapter().execute(callback);

		List<T> result = new ArrayList<>(raw.size());
		for (Map.Entry<byte[], Map<byte[], byte[]>> entry : raw.entrySet()) {

			if (CollectionUtils.isEmpty(entry.getValue())) {
				continue;
			}

			ValkeyData data = new ValkeyData(entry.getValue());
			data.setId(getRequiredAdapter().getConverter().getConversionService().convert(entry.getKey(), String.class));
			data.setKeyspace(keyspace);

			T converted = this.getRequiredAdapter().getConverter().read(type, data);

			result.add(converted);
		}
		return result;
	}

	private List<byte[]> findKeys(ValkeyOperationChain criteria, int rows, String keyspace, Class<?> domainType,
			ValkeyConnection connection) {

		List<byte[]> allKeys = new ArrayList<>();

		if (!criteria.getSismember().isEmpty()) {

			Set<PathAndValue> sismember = criteria.getSismember();
			if (sismember.size() == 1) {
				KeySelector keySelector = KeySelector.of(getRequiredAdapter().getConverter(), sismember, domainType);
				if (!keySelector.setValueLookup().isEmpty()) {
					allKeys.addAll(connection.sInter(keys(keyspace + ":", keySelector.setValueLookup())));
				}

				allKeys.addAll(keySelector.keys());
			} else {
				allKeys.addAll(connection.sInter(keys(keyspace + ":", sismember)));
			}
		}

		KeySelector keySelector = KeySelector.of(getRequiredAdapter().getConverter(), criteria.getOrSismember(),
				domainType);

		if (!keySelector.setValueLookup().isEmpty()) {
			allKeys.addAll(connection.sUnion(keys(keyspace + ":", criteria.getOrSismember())));
		}

		allKeys.addAll(keySelector.keys());

		if (criteria.getNear() != null) {

			GeoRadiusCommandArgs limit = GeoRadiusCommandArgs.newGeoRadiusArgs();

			if (rows > 0) {
				limit = limit.limit(rows);
			}

			GeoResults<GeoLocation<byte[]>> x = connection.geoRadius(geoKey(keyspace + ":", criteria.getNear()),
					new Circle(criteria.getNear().getPoint(), criteria.getNear().getDistance()), limit);
			for (GeoResult<GeoLocation<byte[]>> y : x) {
				allKeys.add(y.getContent().getName());
			}
		}

		Set<ByteArrayWrapper> unique = new LinkedHashSet<>(allKeys.size());
		allKeys.forEach(key -> unique.add(new ByteArrayWrapper(key)));

		List<byte[]> uniqueKeys = new ArrayList<>(unique.size());
		unique.forEach(key -> uniqueKeys.add(key.getArray()));

		return uniqueKeys;
	}

	@Override
	public List<?> execute(ValkeyOperationChain criteria, Comparator<?> sort, long offset, int rows, String keyspace) {
		return execute(criteria, sort, offset, rows, keyspace, Object.class);
	}

	@Override
	public long count(ValkeyOperationChain criteria, String keyspace) {

		if (criteria == null || criteria.isEmpty()) {
			return this.getRequiredAdapter().count(keyspace);
		}

		return this.getRequiredAdapter().execute(connection -> {

			long result = 0;

			if (!criteria.getOrSismember().isEmpty()) {
				result += connection.sUnion(keys(keyspace + ":", criteria.getOrSismember())).size();
			}

			if (!criteria.getSismember().isEmpty()) {
				result += connection.sInter(keys(keyspace + ":", criteria.getSismember())).size();
			}

			return result;
		});
	}

	private byte[][] keys(String prefix, Collection<PathAndValue> source) {

		ConversionService conversionService = getRequiredAdapter().getConverter().getConversionService();
		byte[][] keys = new byte[source.size()][];
		int i = 0;
		for (PathAndValue pathAndValue : source) {

			byte[] convertedValue = conversionService.convert(pathAndValue.getFirstValue(), byte[].class);
			byte[] fullPath = conversionService.convert(prefix + pathAndValue.getPath() + ":", byte[].class);

			keys[i] = ByteUtils.concat(fullPath, convertedValue);
			i++;
		}
		return keys;
	}

	private byte[] geoKey(String prefix, NearPath source) {

		String path = GeoIndexedPropertyValue.geoIndexName(source.getPath());
		return getRequiredAdapter().getConverter().getConversionService().convert(prefix + path, byte[].class);

	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class ValkeyCriteriaAccessor implements CriteriaAccessor<ValkeyOperationChain> {

		@Override
		public ValkeyOperationChain resolve(KeyValueQuery<?> query) {
			return (ValkeyOperationChain) query.getCriteria();
		}
	}

	/**
	 * Value object capturing the direct object keys and set of values that need to be looked up from the secondary
	 * indexes.
	 *
	 * @param keys
	 * @param setValueLookup
	 * @since 3.2.4
	 */
	record KeySelector(Collection<byte[]> keys, Set<PathAndValue> setValueLookup) {

		static KeySelector of(ValkeyConverter converter, Set<PathAndValue> pathAndValues, Class<?> domainType) {

			Set<byte[]> keys = new LinkedHashSet<>();
			Set<PathAndValue> remainder = new LinkedHashSet<>();

			for (PathAndValue pathAndValue : pathAndValues) {

				PersistentPropertyPath<ValkeyPersistentProperty> path = converter.getMappingContext()
						.getPersistentPropertyPath(pathAndValue.getPath(), domainType);
				if (path.getLeafProperty().isIdProperty()) {
					byte[] key = converter.getConversionService().convert(pathAndValue.getFirstValue(), byte[].class);
					keys.add(key);
				} else {
					remainder.add(pathAndValue);
				}
			}

			return new KeySelector(keys, remainder);
		}
	}
}
