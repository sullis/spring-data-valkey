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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ValueScanCursor;
import io.lettuce.core.api.async.ValkeySetAsyncCommands;
import io.lettuce.core.cluster.api.sync.ValkeyClusterCommands;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ValkeySetCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.Cursor.CursorId;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceSetCommands implements ValkeySetCommands {

	private final LettuceConnection connection;

	LettuceSetCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long sAdd(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(ValkeySetAsyncCommands::sadd, key, values);
	}

	@Override
	public Long sCard(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeySetAsyncCommands::scard, key);
	}

	@Override
	public Set<byte[]> sDiff(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(ValkeySetAsyncCommands::sdiff, keys);
	}

	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		return connection.invoke().just(ValkeySetAsyncCommands::sdiffstore, destKey, keys);
	}

	@Override
	public Set<byte[]> sInter(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(ValkeySetAsyncCommands::sinter, keys);
	}

	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		return connection.invoke().just(ValkeySetAsyncCommands::sinterstore, destKey, keys);
	}

	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(ValkeySetAsyncCommands::sismember, key, value);
	}

	@Override
	public List<Boolean> sMIsMember(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(ValkeySetAsyncCommands::smismember, key, values);
	}

	@Override
	public Set<byte[]> sMembers(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeySetAsyncCommands::smembers, key);
	}

	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(ValkeySetAsyncCommands::smove, srcKey, destKey, value);
	}

	@Override
	public byte[] sPop(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeySetAsyncCommands::spop, key);
	}

	@Override
	public List<byte[]> sPop(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(ValkeySetAsyncCommands::spop, key, count).get(ArrayList::new);
	}

	@Override
	public byte[] sRandMember(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeySetAsyncCommands::srandmember, key);
	}

	@Override
	public List<byte[]> sRandMember(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(ValkeySetAsyncCommands::srandmember, key, count);
	}

	@Override
	public Long sRem(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(ValkeySetAsyncCommands::srem, key, values);
	}

	@Override
	public Set<byte[]> sUnion(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(ValkeySetAsyncCommands::sunion, keys);
	}

	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		return connection.invoke().just(ValkeySetAsyncCommands::sunionstore, destKey, keys);
	}

	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		return sScan(key, CursorId.initial(), options);
	}

	/**
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 * @since 1.4
	 */
	public Cursor<byte[]> sScan(byte[] key, CursorId cursorId, ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new KeyBoundCursor<byte[]>(key, cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(byte[] key, CursorId cursorId, ScanOptions options) {

				if (connection.isQueueing() || connection.isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'SSCAN' cannot be called in pipeline / transaction mode");
				}

				io.lettuce.core.ScanCursor scanCursor = connection.getScanCursor(cursorId);
				ScanArgs scanArgs = LettuceConverters.toScanArgs(options);

				ValueScanCursor<byte[]> valueScanCursor = connection.invoke().just(ValkeySetAsyncCommands::sscan, key,
						scanCursor, scanArgs);
				String nextCursorId = valueScanCursor.getCursor();

				List<byte[]> values = connection.failsafeReadScanValues(valueScanCursor.getValues(), null);
				return new ScanIteration<>(CursorId.of(nextCursorId), values);
			}

			protected void doClose() {
				LettuceSetCommands.this.connection.close();
			}

		}.open();
	}

	public ValkeyClusterCommands<byte[], byte[]> getCommands() {
		return connection.getConnection();
	}

}
