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
package org.springframework.data.valkey.connection.lettuce;

import io.lettuce.core.BitFieldArgs;
import io.lettuce.core.api.async.ValkeyStringAsyncCommands;

import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.valkey.connection.BitFieldSubCommands;
import org.springframework.data.valkey.connection.ValkeyStringCommands;
import org.springframework.data.valkey.connection.convert.Converters;
import org.springframework.data.valkey.core.types.Expiration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ValkeyStringCommands} implementation for {@literal Lettuce}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author dengliming
 * @author John Blum
 * @since 2.0
 */
class LettuceStringCommands implements ValkeyStringCommands {

	private final LettuceConnection connection;

	LettuceStringCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public byte[] get(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::get, key);
	}

	@Nullable
	@Override
	public byte[] getDel(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::getdel, key);
	}

	@Nullable
	@Override
	public byte[] getEx(byte[] key, Expiration expiration) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(expiration, "Expiration must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::getex, key, LettuceConverters.toGetExArgs(expiration));
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::getset, key, value);
	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().fromMany(ValkeyStringAsyncCommands::mget, keys)
				.toList(source -> source.getValueOrElse(null));
	}

	@Override
	public Boolean set(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(ValkeyStringAsyncCommands::set, key, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(expiration, "Expiration must not be null");
		Assert.notNull(option, "Option must not be null");

		return connection.invoke()
				.from(ValkeyStringAsyncCommands::set, key, value, LettuceConverters.toSetArgs(expiration, option))
				.orElse(LettuceConverters.stringToBooleanConverter(), false);
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::setnx, key, value);
	}

	@Override
	public Boolean setEx(byte[] key, long seconds, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(ValkeyStringAsyncCommands::setex, key, seconds, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(ValkeyStringAsyncCommands::psetex, key, milliseconds, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean mSet(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		return connection.invoke().from(ValkeyStringAsyncCommands::mset, tuples).get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::msetnx, tuples);
	}

	@Override
	public Long incr(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::incr, key);
	}

	@Override
	public Long incrBy(byte[] key, long value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::incrby, key, value);
	}

	@Override
	public Double incrBy(byte[] key, double value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::incrbyfloat, key, value);
	}

	@Override
	public Long decr(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::decr, key);
	}

	@Override
	public Long decrBy(byte[] key, long value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::decrby, key, value);
	}

	@Override
	public Long append(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::append, key, value);
	}

	@Override
	public byte[] getRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::getrange, key, start, end);
	}

	@Override
	public void setRange(byte[] key, byte[] value, long offset) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		connection.invokeStatus().just(ValkeyStringAsyncCommands::setrange, key, offset, value);
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(ValkeyStringAsyncCommands::getbit, key, offset)
				.get(LettuceConverters.longToBoolean());
	}

	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(ValkeyStringAsyncCommands::setbit, key, offset, LettuceConverters.toInt(value))
				.get(LettuceConverters.longToBoolean());
	}

	@Override
	public Long bitCount(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::bitcount, key);
	}

	@Override
	public Long bitCount(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::bitcount, key, start, end);
	}

	@Override
	public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(subCommands, "Command must not be null");

		BitFieldArgs args = LettuceConverters.toBitFieldArgs(subCommands);

		return connection.invoke().just(ValkeyStringAsyncCommands::bitfield, key, args);
	}

	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {

		Assert.notNull(op, "BitOperation must not be null");
		Assert.notNull(destination, "Destination key must not be null");

		if (op == BitOperation.NOT && keys.length > 1) {
			throw new IllegalArgumentException("Bitop NOT should only be performed against one key");
		}

		return connection.invoke().just(it ->
			switch (op) {
				case AND -> it.bitopAnd(destination, keys);
				case OR -> it.bitopOr(destination, keys);
				case XOR -> it.bitopXor(destination, keys);
				case NOT -> {
					if (keys.length != 1) {
						throw new IllegalArgumentException("Bitop NOT should only be performed against one key");
					}
					yield it.bitopNot(destination, keys[0]);
				}
      		});
	}

	@Nullable
	@Override
	public Long bitPos(byte[] key, boolean bit, Range<Long> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null Use Range.unbounded() instead");

		if (range.getLowerBound().isBounded()) {

			if (range.getUpperBound().isBounded()) {
				return connection.invoke().just(ValkeyStringAsyncCommands::bitpos, key, bit, getLowerValue(range),
						getUpperValue(range));
			}

			return connection.invoke().just(ValkeyStringAsyncCommands::bitpos, key, bit, getLowerValue(range));
		}

		return connection.invoke().just(ValkeyStringAsyncCommands::bitpos, key, bit);
	}

	@Override
	public Long strLen(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyStringAsyncCommands::strlen, key);
	}

	private static <T extends Comparable<T>> T getUpperValue(Range<T> range) {

		return range.getUpperBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain upper bound value"));
	}

	private static <T extends Comparable<T>> T getLowerValue(Range<T> range) {

		return range.getLowerBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain lower bound value"));
	}
}
