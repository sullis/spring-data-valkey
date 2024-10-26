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

import io.lettuce.core.KeyValue;
import io.lettuce.core.LPosArgs;
import io.lettuce.core.api.async.ValkeyListAsyncCommands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.valkey.connection.ValkeyListCommands;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author dengliming
 * @since 2.0
 */
class LettuceListCommands implements ValkeyListCommands {

	private final LettuceConnection connection;

	LettuceListCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long rPush(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::rpush, key, values);
	}

	@Override
	public List<Long> lPos(byte[] key, byte[] element, @Nullable Integer rank, @Nullable Integer count) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(element, "Element must not be null");

		LPosArgs args = new LPosArgs();
		if (rank != null) {
			args.rank(rank);
		}

		if (count != null) {
			return connection.invoke().just(ValkeyListAsyncCommands::lpos, key, element, count, args);
		}

		return connection.invoke().from(ValkeyListAsyncCommands::lpos, key, element, args)
				.getOrElse(Collections::singletonList, Collections::emptyList);
	}

	@Override
	public Long lPush(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(ValkeyListAsyncCommands::lpush, key, values);
	}

	@Override
	public Long rPushX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::rpushx, key, value);
	}

	@Override
	public Long lPushX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::lpushx, key, value);
	}

	@Override
	public Long lLen(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::llen, key);
	}

	@Override
	public List<byte[]> lRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::lrange, key, start, end);
	}

	@Override
	public void lTrim(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		connection.invokeStatus().just(ValkeyListAsyncCommands::ltrim, key, start, end);
	}

	@Override
	public byte[] lIndex(byte[] key, long index) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::lindex, key, index);
	}

	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::linsert, key, LettuceConverters.toBoolean(where), pivot,
				value);
	}

	@Override
	public byte[] lMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(from, "From direction must not be null");
		Assert.notNull(to, "To direction must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::lmove, sourceKey, destinationKey,
				LettuceConverters.toLmoveArgs(from, to));

	}

	@Override
	public byte[] bLMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to, double timeout) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(from, "From direction must not be null");
		Assert.notNull(to, "To direction must not be null");

		return connection.invoke(connection.getAsyncDedicatedConnection()).just(ValkeyListAsyncCommands::blmove, sourceKey,
				destinationKey, LettuceConverters.toLmoveArgs(from, to), timeout);
	}

	@Override
	public void lSet(byte[] key, long index, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		connection.invokeStatus().just(ValkeyListAsyncCommands::lset, key, index, value);
	}

	@Override
	public Long lRem(byte[] key, long count, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::lrem, key, count, value);
	}

	@Override
	public byte[] lPop(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::lpop, key);
	}

	@Override
	public List<byte[]> lPop(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::lpop, key, count);
	}

	@Override
	public byte[] rPop(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::rpop, key);
	}

	@Override
	public List<byte[]> rPop(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::rpop, key, count);
	}

	@Override
	public List<byte[]> bLPop(int timeout, byte[]... keys) {

		Assert.notNull(keys, "Key must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke(connection.getAsyncDedicatedConnection())
				.from(ValkeyListAsyncCommands::blpop, timeout, keys).get(LettuceListCommands::toBytesList);
	}

	@Override
	public List<byte[]> bRPop(int timeout, byte[]... keys) {

		Assert.notNull(keys, "Key must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke(connection.getAsyncDedicatedConnection())
				.from(ValkeyListAsyncCommands::brpop, timeout, keys).get(LettuceListCommands::toBytesList);
	}

	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(dstKey, "Destination key must not be null");

		return connection.invoke().just(ValkeyListAsyncCommands::rpoplpush, srcKey, dstKey);
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(dstKey, "Destination key must not be null");

		return connection.invoke(connection.getAsyncDedicatedConnection()).just(ValkeyListAsyncCommands::brpoplpush, timeout,
				srcKey, dstKey);
	}

	private static List<byte[]> toBytesList(KeyValue<byte[], byte[]> source) {

		List<byte[]> list = new ArrayList<>(2);
		list.add(source.getKey());
		list.add(source.getValue());

		return list;
	}
}