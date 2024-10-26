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
package org.springframework.data.valkey.serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Simple {@link java.lang.String} to {@literal byte[]} (and back) serializer. Converts {@link java.lang.String Strings}
 * into bytes and vice-versa using the specified charset (by default {@literal UTF-8}).
 * <p>
 * Useful when the interaction with the Valkey happens mainly through Strings.
 * <p>
 * Does not perform any {@literal null} conversion since empty strings are valid keys/values.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class StringValkeySerializer implements ValkeySerializer<String> {

	private final Charset charset;

	/**
	 * {@link StringValkeySerializer} to use 7 bit ASCII, a.k.a. ISO646-US, a.k.a. the Basic Latin block of the Unicode
	 * character set.
	 *
	 * @see StandardCharsets#US_ASCII
	 * @since 2.1
	 */
	public static final StringValkeySerializer US_ASCII = new StringValkeySerializer(StandardCharsets.US_ASCII);

	/**
	 * {@link StringValkeySerializer} to use ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1.
	 *
	 * @see StandardCharsets#ISO_8859_1
	 * @since 2.1
	 */
	public static final StringValkeySerializer ISO_8859_1 = new StringValkeySerializer(StandardCharsets.ISO_8859_1);

	/**
	 * {@link StringValkeySerializer} to use 8 bit UCS Transformation Format.
	 *
	 * @see StandardCharsets#UTF_8
	 * @since 2.1
	 */
	public static final StringValkeySerializer UTF_8 = new StringValkeySerializer(StandardCharsets.UTF_8);

	/**
	 * Creates a new {@link StringValkeySerializer} using {@link StandardCharsets#UTF_8 UTF-8}.
	 */
	public StringValkeySerializer() {
		this(StandardCharsets.UTF_8);
	}

	/**
	 * Creates a new {@link StringValkeySerializer} using the given {@link Charset} to encode and decode strings.
	 *
	 * @param charset must not be {@literal null}.
	 */
	public StringValkeySerializer(Charset charset) {

		Assert.notNull(charset, "Charset must not be null");
		this.charset = charset;
	}

	@Override
	public byte[] serialize(@Nullable String value) {
		return (value == null ? null : value.getBytes(charset));
	}

	@Override
	public String deserialize(@Nullable byte[] bytes) {
		return (bytes == null ? null : new String(bytes, charset));
	}

	@Override
	public Class<?> getTargetType() {
		return String.class;
	}
}