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
package org.springframework.data.redis.support.collections;

import static org.springframework.data.redis.connection.ValkeyListCommands.*;

import java.time.Duration;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.ValkeyOperations;
import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Valkey extension for the {@link List} contract. Supports {@link List}, {@link Queue} and {@link Deque} contracts as
 * well as their equivalent blocking siblings {@link BlockingDeque} and {@link BlockingDeque}.
 *
 * @param <E> the type of elements in this collection.
 * @author Costin Leau
 * @author Mark Paluch
 * @author John Blum
 */
public interface ValkeyList<E> extends ValkeyCollection<E>, List<E>, BlockingDeque<E> {

	/**
	 * Constructs a new, uncapped {@link ValkeyList} instance.
	 *
	 * @param key Valkey key of this list.
	 * @param operations {@link ValkeyOperations} for the value type of this list.
	 * @since 2.6
	 */
	static <E> ValkeyList<E> create(String key, ValkeyOperations<String, E> operations) {
		return new DefaultValkeyList<>(key, operations);
	}

	/**
	 * Factory method used to construct a new {@link ValkeyList} from a Valkey list reference by the given {@link String
	 * key}.
	 *
	 * @param key Valkey key of this list.
	 * @param operations {@link ValkeyOperations} for the value type of this list.
	 * @param maxSize {@link Integer} used to constrain the size of the list.
	 * @since 2.6
	 */
	static <E> ValkeyList<E> create(String key, ValkeyOperations<String, E> operations, int maxSize) {
		return new DefaultValkeyList<>(key, operations, maxSize);
	}

	/**
	 * Constructs a new, uncapped {@link DefaultValkeyList} instance.
	 *
	 * @param boundOps {@link BoundListOperations} for the value type of this list.
	 * @since 2.6
	 */
	static <E> ValkeyList<E> create(BoundListOperations<String, E> boundOps) {
		return new DefaultValkeyList<>(boundOps);
	}

	/**
	 * Constructs a new {@link DefaultValkeyList}.
	 *
	 * @param boundOps {@link BoundListOperations} for the value type of this list.
	 * @param maxSize {@link Integer} constraining the size of the list.
	 * @since 2.6
	 */
	static <E> ValkeyList<E> create(BoundListOperations<String, E> boundOps, int maxSize) {
		return new DefaultValkeyList<>(boundOps, maxSize);
	}

	/**
	 * Atomically returns and removes the first element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link ValkeyList destination}.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	E moveFirstTo(ValkeyList<E> destination, Direction destinationPosition);

	/**
	 * Atomically returns and removes the first element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link ValkeyList destination}.
	 * <p>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	E moveFirstTo(ValkeyList<E> destination, Direction destinationPosition, long timeout, TimeUnit unit);

	/**
	 * Atomically returns and removes the first element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link ValkeyList destination}.
	 * <p>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @param timeout must not be {@literal null} or negative.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	default E moveFirstTo(ValkeyList<E> destination, Direction destinationPosition, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return moveFirstTo(destination, destinationPosition,
				TimeoutUtils.toMillis(timeout.toMillis(), TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
	}

	/**
	 * Atomically returns and removes the last element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link ValkeyList destination}.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	E moveLastTo(ValkeyList<E> destination, Direction destinationPosition);

	/**
	 * Atomically returns and removes the last element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link ValkeyList destination}.
	 * <p>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	E moveLastTo(ValkeyList<E> destination, Direction destinationPosition, long timeout, TimeUnit unit);

	/**
	 * Atomically returns and removes the last element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link ValkeyList destination}.
	 * <p>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @param timeout must not be {@literal null} or negative.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	default E moveLastTo(ValkeyList<E> destination, Direction destinationPosition, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return moveLastTo(destination, destinationPosition,
				TimeoutUtils.toMillis(timeout.toMillis(), TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
	}

	/**
	 * Get elements between {@code start} and {@code end} from list at the bound key.
	 *
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lrange">Valkey Documentation: LRANGE</a>
	 */
	List<E> range(long start, long end);

	/**
	 * Trim list at the bound key to elements between {@code start} and {@code end}.
	 *
	 * @param start
	 * @param end
	 * @see <a href="https://redis.io/commands/ltrim">Valkey Documentation: LTRIM</a>
	 */
	ValkeyList<E> trim(int start, int end);

	/**
	 * Trim list at the bound key to elements between {@code start} and {@code end}.
	 *
	 * @param start
	 * @param end
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/ltrim">Valkey Documentation: LTRIM</a>
	 */
	ValkeyList<E> trim(long start, long end);

	/**
	 * {@inheritDoc}
	 * <p>
	 * This method is forward-compatible with Java 21 {@literal SequencedCollections}.
	 *
	 * @param element element to be added to the head of the collection.
	 */
	@Override
	default void addFirst(E element) {
		add(0, element);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * This method is forward-compatible with Java 21 {@literal SequencedCollections}.
	 *
	 * @param element element to be added to be added the end of the collection.
	 */
	default void addLast(E element) {
		add(element);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * This method is forward-compatible with Java 21 {@literal SequencedCollections}.
	 *
	 * @return the head of this {@link Deque}.
	 */
	@Nullable
	default E getFirst() {
		return peekFirst();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * This method is forward-compatible with Java 21 {@literal SequencedCollections}.
	 *
	 * @return the tail of this {@link Deque}.
	 */
	@Nullable
	default E getLast() {
		return peekLast();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * This method is forward-compatible with Java 21 {@literal SequencedCollections}.
	 *
	 * @return the head of this {@link Deque}.
	 */
	@Nullable
	default E removeFirst() {
		return pollFirst();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * This method is forward-compatible with Java 21 {@literal SequencedCollections}.
	 *
	 * @return the tail of this {@link Deque}.
	 */
	@Nullable
	default E removeLast() {
		return pollLast();
	}

	/**
	 * Returns a reverse-ordered view of this collection.
	 * <p>
	 * The encounter order of elements returned by the view is the inverse of the encounter order of the elements
	 * stored in this collection. The reverse ordering affects all order-sensitive operations, including any operations
	 * on further views of the returned view. If the collection implementation permits modifications to this view,
	 * the modifications "write-through" to the underlying collection. Changes to the underlying collection might
	 * or might not be visible in this reversed view, depending upon the implementation.
	 * <p>
	 * This method is forward-compatible with Java 21 {@literal SequencedCollections}.
	 *
	 * @return a reverse-ordered view of this collection.
	 */
	default ValkeyList<E> reversed() {
		return new ReversedValkeyListView<>(this);
	}
}
