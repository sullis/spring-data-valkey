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
package org.springframework.data.valkey.core.convert;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.ConfigurableTypeInformationMapper;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link DefaultValkeyTypeMapper}.
 *
 * @author Mark Paluch
 */
class DefaultValkeyTypeMapperUnitTests {

	private GenericConversionService conversionService;
	private ConfigurableTypeInformationMapper configurableTypeInformationMapper;
	private SimpleTypeInformationMapper simpleTypeInformationMapper;
	private DefaultValkeyTypeMapper typeMapper;

	@BeforeEach
	void setUp() {

		conversionService = new GenericConversionService();
		new ValkeyCustomConversions().registerConvertersIn(conversionService);

		configurableTypeInformationMapper = new ConfigurableTypeInformationMapper(singletonMap(String.class, "1"));
		simpleTypeInformationMapper = new SimpleTypeInformationMapper();

		typeMapper = new DefaultValkeyTypeMapper(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY);
	}

	@Test // DATAREDIS-543
	void defaultInstanceWritesClasses() {
		writesTypeToField(new Bucket(), String.class, String.class.getName());
	}

	@Test // DATAREDIS-543
	void defaultInstanceReadsClasses() {

		Bucket bucket = Bucket
				.newBucketFromStringMap(singletonMap(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, String.class.getName()));
		readsTypeFromField(bucket, String.class);
	}

	@Test // DATAREDIS-543
	void writesMapKeyForType() {

		typeMapper = new DefaultValkeyTypeMapper(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY,
				Collections.singletonList(configurableTypeInformationMapper));

		writesTypeToField(new Bucket(), String.class, "1");
		writesTypeToField(new Bucket(), Object.class, null);
	}

	@Test // DATAREDIS-543
	void writesClassNamesForUnmappedValuesIfConfigured() {

		typeMapper = new DefaultValkeyTypeMapper(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY,
				Arrays.asList(configurableTypeInformationMapper, simpleTypeInformationMapper));

		writesTypeToField(new Bucket(), String.class, "1");
		writesTypeToField(new Bucket(), Object.class, Object.class.getName());
	}

	@Test // DATAREDIS-543
	void readsTypeForMapKey() {

		typeMapper = new DefaultValkeyTypeMapper(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY,
				Collections.singletonList(configurableTypeInformationMapper));

		readsTypeFromField(Bucket.newBucketFromStringMap(singletonMap(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, "1")),
				String.class);
		readsTypeFromField(Bucket.newBucketFromStringMap(singletonMap(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, "unmapped")),
				null);
	}

	@Test // DATAREDIS-543
	void readsTypeLoadingClassesForUnmappedTypesIfConfigured() {

		typeMapper = new DefaultValkeyTypeMapper(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY,
				Arrays.asList(configurableTypeInformationMapper, simpleTypeInformationMapper));

		readsTypeFromField(new Bucket(singletonMap(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, "1".getBytes())), String.class);
		readsTypeFromField(
				new Bucket(singletonMap(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, Object.class.getName().getBytes())),
				Object.class);
	}

	@Test // DATAREDIS-543
	void addsFullyQualifiedClassNameUnderDefaultKeyByDefault() {
		writesTypeToField(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, new Bucket(), String.class);
	}

	@Test // DATAREDIS-543
	void writesTypeToCustomFieldIfConfigured() {
		typeMapper = new DefaultValkeyTypeMapper("_custom");
		writesTypeToField("_custom", new Bucket(), String.class);
	}

	@Test // DATAREDIS-543
	void doesNotWriteTypeInformationInCaseKeyIsSetToNull() {
		typeMapper = new DefaultValkeyTypeMapper(null);
		writesTypeToField(null, new Bucket(), String.class);
	}

	@Test // DATAREDIS-543
	void readsTypeFromDefaultKeyByDefault() {
		readsTypeFromField(
				Bucket.newBucketFromStringMap(singletonMap(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, String.class.getName())),
				String.class);
	}

	@Test // DATAREDIS-543
	void readsTypeFromCustomFieldConfigured() {

		typeMapper = new DefaultValkeyTypeMapper("_custom");
		readsTypeFromField(Bucket.newBucketFromStringMap(singletonMap("_custom", String.class.getName())), String.class);
	}

	@Test // DATAREDIS-543
	void returnsListForBasicDBLists() {
		readsTypeFromField(new Bucket(), null);
	}

	@Test // DATAREDIS-543
	void returnsNullIfNoTypeInfoInBucket() {

		readsTypeFromField(new Bucket(), null);
		readsTypeFromField(Bucket.newBucketFromStringMap(singletonMap(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, "")), null);
	}

	@Test // DATAREDIS-543
	void returnsNullIfClassCannotBeLoaded() {

		readsTypeFromField(Bucket.newBucketFromStringMap(singletonMap(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, "fooBar")),
				null);
	}

	@Test // DATAREDIS-543
	void returnsNullIfTypeKeySetToNull() {

		typeMapper = new DefaultValkeyTypeMapper(null);
		readsTypeFromField(
				Bucket.newBucketFromStringMap(singletonMap(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY, String.class.getName())),
				null);
	}

	@Test // DATAREDIS-543
	void returnsCorrectTypeKey() {

		assertThat(typeMapper.isTypeKey(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY)).isTrue();

		typeMapper = new DefaultValkeyTypeMapper("_custom");
		assertThat(typeMapper.isTypeKey("_custom")).isTrue();
		assertThat(typeMapper.isTypeKey(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY)).isFalse();

		typeMapper = new DefaultValkeyTypeMapper(null);
		assertThat(typeMapper.isTypeKey("_custom")).isFalse();
		assertThat(typeMapper.isTypeKey(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY)).isFalse();
	}

	private void readsTypeFromField(Bucket bucket, @Nullable Class<?> type) {

		TypeInformation<?> typeInfo = typeMapper.readType(bucket.getPath());

		if (type != null) {
			assertThat(typeInfo).isNotNull();
			assertThat(typeInfo.getType()).isAssignableFrom(type);
		} else {
			assertThat(typeInfo).isNull();
		}
	}

	private void writesTypeToField(@Nullable String field, Bucket bucket, Class<?> type) {

		typeMapper.writeType(type, bucket.getPath());

		if (field == null) {
			assertThat(bucket.keySet()).isEmpty();
		} else {
			assertThat(bucket.asMap()).containsKey(field);
			assertThat(bucket.get(field)).isEqualTo(type.getName().getBytes());
		}
	}

	private void writesTypeToField(Bucket bucket, Class<?> type, @Nullable Object value) {

		typeMapper.writeType(type, bucket.getPath());

		if (value == null) {
			assertThat(bucket.keySet()).isEmpty();
		} else {

			byte[] expected = value instanceof Class javaClass ? javaClass.getName().getBytes() : value.toString().getBytes();

			assertThat(bucket.asMap()).containsKey(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY);
			assertThat(bucket.get(DefaultValkeyTypeMapper.DEFAULT_TYPE_KEY)).isEqualTo(expected);
		}
	}
}
