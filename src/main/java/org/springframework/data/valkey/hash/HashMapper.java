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
package org.springframework.data.valkey.hash;

import java.util.Map;

/**
 * Core mapping contract between Java types and Valkey hashes/maps. It's up to the implementation to support nested
 * objects.
 *
 * @param <T> Object type
 * @param <K> Valkey Hash field type
 * @param <V> Valkey Hash value type
 * @author Costin Leau
 * @author Mark Paluch
 */
public interface HashMapper<T, K, V> {

	/**
	 * Convert an {@code object} to a map that can be used with Valkey hashes.
	 *
	 * @param object
	 * @return
	 */
	Map<K, V> toHash(T object);

	/**
	 * Convert a {@code hash} (map) to an object.
	 *
	 * @param hash
	 * @return
	 */
	T fromHash(Map<K, V> hash);
}