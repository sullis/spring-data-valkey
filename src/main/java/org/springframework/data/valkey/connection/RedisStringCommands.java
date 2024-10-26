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
package org.springframework.data.valkey.connection;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.domain.Range;
import org.springframework.data.valkey.core.types.Expiration;
import org.springframework.lang.Nullable;

/**
 * String/Value-specific commands supported by Valkey.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface ValkeyStringCommands {

	enum BitOperation {
		AND, OR, XOR, NOT;
	}

	/**
	 * Get the value of {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/get">Valkey Documentation: GET</a>
	 */
	@Nullable
	byte[] get(byte[] key);

	/**
	 * Return the value at {@code key} and delete the key.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getdel">Valkey Documentation: GETDEL</a>
	 * @since 2.6
	 */
	@Nullable
	byte[] getDel(byte[] key);

	/**
	 * Return the value at {@code key} and expire the key by applying {@link Expiration}.
	 * <p>
	 * Use {@link Expiration#seconds(long)} for {@code EX}. <br />
	 * Use {@link Expiration#milliseconds(long)} for {@code PX}. <br />
	 * Use {@link Expiration#unixTimestamp(long, TimeUnit)} for {@code EXAT | PXAT}. <br />
	 *
	 * @param key must not be {@literal null}.
	 * @param expiration must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Valkey Documentation: GETEX</a>
	 * @since 2.6
	 */
	@Nullable
	byte[] getEx(byte[] key, Expiration expiration);

	/**
	 * Set {@code value} of {@code key} and return its old value.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} if key did not exist before or when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getset">Valkey Documentation: GETSET</a>
	 */
	@Nullable
	byte[] getSet(byte[] key, byte[] value);

	/**
	 * Get multiple {@code keys}. Values are in the order of the requested keys Absent field values are represented using
	 * {@literal null} in the resulting {@link List}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/mget">Valkey Documentation: MGET</a>
	 */
	@Nullable
	List<byte[]> mGet(byte[]... keys);

	/**
	 * Set {@code value} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/set">Valkey Documentation: SET</a>
	 */
	@Nullable
	Boolean set(byte[] key, byte[] value);

	/**
	 * Set {@code value} for {@code key} applying timeouts from {@code expiration} if set and inserting/updating values
	 * depending on {@code option}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration must not be {@literal null}. Use {@link Expiration#persistent()} to not set any ttl or
	 *          {@link Expiration#keepTtl()} to keep the existing expiration.
	 * @param option must not be {@literal null}. Use {@link SetOption#upsert()} to add non existing.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.7
	 * @see <a href="https://redis.io/commands/set">Valkey Documentation: SET</a>
	 */
	@Nullable
	Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option);

	/**
	 * Set {@code value} for {@code key}, only if {@code key} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/setnx">Valkey Documentation: SETNX</a>
	 */
	@Nullable
	Boolean setNX(byte[] key, byte[] value);

	/**
	 * Set the {@code value} and expiration in {@code seconds} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/setex">Valkey Documentation: SETEX</a>
	 */
	@Nullable
	Boolean setEx(byte[] key, long seconds, byte[] value);

	/**
	 * Set the {@code value} and expiration in {@code milliseconds} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param milliseconds
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/psetex">Valkey Documentation: PSETEX</a>
	 */
	@Nullable
	Boolean pSetEx(byte[] key, long milliseconds, byte[] value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 *
	 * @param tuple must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/mset">Valkey Documentation: MSET</a>
	 */
	@Nullable
	Boolean mSet(Map<byte[], byte[]> tuple);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 *
	 * @param tuple must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/msetnx">Valkey Documentation: MSETNX</a>
	 */
	@Nullable
	Boolean mSetNX(Map<byte[], byte[]> tuple);

	/**
	 * Increment an integer value stored as string value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incr">Valkey Documentation: INCR</a>
	 */
	@Nullable
	Long incr(byte[] key);

	/**
	 * Increment an integer value stored of {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrby">Valkey Documentation: INCRBY</a>
	 */
	@Nullable
	Long incrBy(byte[] key, long value);

	/**
	 * Increment a floating point number value of {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrbyfloat">Valkey Documentation: INCRBYFLOAT</a>
	 */
	@Nullable
	Double incrBy(byte[] key, double value);

	/**
	 * Decrement an integer value stored as string value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/decr">Valkey Documentation: DECR</a>
	 */
	@Nullable
	Long decr(byte[] key);

	/**
	 * Decrement an integer value stored as string value of {@code key} by {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/decrby">Valkey Documentation: DECRBY</a>
	 */
	@Nullable
	Long decrBy(byte[] key, long value);

	/**
	 * Append a {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/append">Valkey Documentation: APPEND</a>
	 */
	@Nullable
	Long append(byte[] key, byte[] value);

	/**
	 * Get a substring of value of {@code key} between {@code start} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getrange">Valkey Documentation: GETRANGE</a>
	 */
	@Nullable
	byte[] getRange(byte[] key, long start, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 * @see <a href="https://redis.io/commands/setrange">Valkey Documentation: SETRANGE</a>
	 */
	void setRange(byte[] key, byte[] value, long offset);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getbit">Valkey Documentation: GETBIT</a>
	 */
	@Nullable
	Boolean getBit(byte[] key, long offset);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @return the original bit value stored at {@code offset} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/setbit">Valkey Documentation: SETBIT</a>
	 */
	@Nullable
	Boolean setBit(byte[] key, long offset, boolean value);

	/**
	 * Count the number of set bits (population counting) in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/bitcount">Valkey Documentation: BITCOUNT</a>
	 */
	@Nullable
	Long bitCount(byte[] key);

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code start} and
	 * {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/bitcount">Valkey Documentation: BITCOUNT</a>
	 */
	@Nullable
	Long bitCount(byte[] key, long start, long end);

	/**
	 * Get / Manipulate specific integer fields of varying bit widths and arbitrary non (necessary) aligned offset stored
	 * at a given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param subCommands must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 */
	@Nullable
	List<Long> bitField(byte[] key, BitFieldSubCommands subCommands);

	/**
	 * Perform bitwise operations between strings.
	 *
	 * @param op must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/bitop">Valkey Documentation: BITOP</a>
	 */
	@Nullable
	Long bitOp(BitOperation op, byte[] destination, byte[]... keys);

	/**
	 * Return the position of the first bit set to given {@code bit} in a string.
	 *
	 * @param key the key holding the actual String.
	 * @param bit the bit value to look for.
	 * @return {@literal null} when used in pipeline / transaction. The position of the first bit set to 1 or 0 according
	 *         to the request.
	 * @see <a href="https://redis.io/commands/bitpos">Valkey Documentation: BITPOS</a>
	 * @since 2.1
	 */
	@Nullable
	default Long bitPos(byte[] key, boolean bit) {
		return bitPos(key, bit, Range.unbounded());
	}

	/**
	 * Return the position of the first bit set to given {@code bit} in a string. {@link Range} start and end can contain
	 * negative values in order to index <strong>bytes</strong> starting from the end of the string, where {@literal -1}
	 * is the last byte, {@literal -2} is the penultimate.
	 *
	 * @param key the key holding the actual String.
	 * @param bit the bit value to look for.
	 * @param range must not be {@literal null}. Use {@link Range#unbounded()} to not limit search.
	 * @return {@literal null} when used in pipeline / transaction. The position of the first bit set to 1 or 0 according
	 *         to the request.
	 * @see <a href="https://redis.io/commands/bitpos">Valkey Documentation: BITPOS</a>
	 * @since 2.1
	 */
	@Nullable
	Long bitPos(byte[] key, boolean bit, Range<Long> range);

	/**
	 * Get the length of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/strlen">Valkey Documentation: STRLEN</a>
	 */
	@Nullable
	Long strLen(byte[] key);

	/**
	 * {@code SET} command arguments for {@code NX}, {@code XX}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum SetOption {

		/**
		 * Do not set any additional command argument.
		 */
		UPSERT,

		/**
		 * {@code NX}
		 */
		SET_IF_ABSENT,

		/**
		 * {@code XX}
		 */
		SET_IF_PRESENT;

		/**
		 * Do not set any additional command argument.
		 */
		public static SetOption upsert() {
			return UPSERT;
		}

		/**
		 * {@code XX}
		 */
		public static SetOption ifPresent() {
			return SET_IF_PRESENT;
		}

		/**
		 * {@code NX}
		 */
		public static SetOption ifAbsent() {
			return SET_IF_ABSENT;
		}
	}

}
