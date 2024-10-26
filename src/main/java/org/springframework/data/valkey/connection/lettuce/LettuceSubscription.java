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
package org.springframework.data.valkey.connection.lettuce;

import io.lettuce.core.pubsub.StatefulValkeyPubSubConnection;
import io.lettuce.core.pubsub.api.async.ValkeyPubSubAsyncCommands;
import io.lettuce.core.pubsub.api.sync.ValkeyPubSubCommands;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.data.valkey.connection.MessageListener;
import org.springframework.data.valkey.connection.SubscriptionListener;
import org.springframework.data.valkey.connection.util.AbstractSubscription;

/**
 * Message subscription on top of Lettuce.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Sarah Abbey
 * @author Murtuza Boxwala
 * @author Jens Deppe
 */
public class LettuceSubscription extends AbstractSubscription {

	private final StatefulValkeyPubSubConnection<byte[], byte[]> connection;
	private final LettuceMessageListener listener;
	private final LettuceConnectionProvider connectionProvider;
	private final ValkeyPubSubCommands<byte[], byte[]> pubsub;
	private final ValkeyPubSubAsyncCommands<byte[], byte[]> pubSubAsync;

	/**
	 * Creates a new {@link LettuceSubscription} given {@link MessageListener}, {@link StatefulValkeyPubSubConnection}, and
	 * {@link LettuceConnectionProvider}.
	 *
	 * @param listener the listener to notify, must not be {@literal null}.
	 * @param pubsubConnection must not be {@literal null}.
	 * @param connectionProvider must not be {@literal null}.
	 */
	protected LettuceSubscription(MessageListener listener,
			StatefulValkeyPubSubConnection<byte[], byte[]> pubsubConnection, LettuceConnectionProvider connectionProvider) {

		super(listener);

		this.connection = pubsubConnection;
		this.listener = new LettuceMessageListener(listener,
				listener instanceof SubscriptionListener subscriptionListener ? subscriptionListener
						: SubscriptionListener.NO_OP_SUBSCRIPTION_LISTENER);
		this.connectionProvider = connectionProvider;
		this.pubsub = connection.sync();
		this.pubSubAsync = connection.async();

		this.connection.addListener(this.listener);
	}

	protected StatefulValkeyPubSubConnection<byte[], byte[]> getNativeConnection() {
		return connection;
	}

	@Override
	protected void doClose() {

		List<CompletableFuture<?>> futures = new ArrayList<>();

		if (!getChannels().isEmpty()) {
			futures.add(pubSubAsync.unsubscribe().toCompletableFuture());
		}

		if (!getPatterns().isEmpty()) {
			futures.add(pubSubAsync.punsubscribe().toCompletableFuture());
		}

		if (!futures.isEmpty()) {

			// this is to ensure completion of the futures and result processing. Since we're unsubscribing first, we expect
			// that we receive pub/sub confirmations before the PING response.
			futures.add(pubSubAsync.ping().toCompletableFuture());

			CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((v, t) -> {
				connection.removeListener(listener);
			});
		} else {
			connection.removeListener(listener);
		}

		connectionProvider.release(connection);
	}

	@Override
	protected void doPsubscribe(byte[]... patterns) {
		pubsub.psubscribe(patterns);
	}

	@Override
	protected void doPUnsubscribe(boolean all, byte[]... patterns) {

		if (all) {
			pubsub.punsubscribe();
		} else {
			pubsub.punsubscribe(patterns);
		}
	}

	@Override
	protected void doSubscribe(byte[]... channels) {
		pubsub.subscribe(channels);
	}

	@Override
	protected void doUnsubscribe(boolean all, byte[]... channels) {

		if (all) {
			pubsub.unsubscribe();
		} else {
			pubsub.unsubscribe(channels);
		}
	}
}