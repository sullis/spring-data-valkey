/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.valkey.connection.lettuce.observability;

import io.lettuce.core.protocol.ValkeyCommand;
import io.lettuce.core.tracing.Tracing.Endpoint;
import io.micrometer.common.KeyValues;

import java.net.InetSocketAddress;
import java.util.Locale;

import org.springframework.data.valkey.connection.lettuce.observability.ValkeyObservation.HighCardinalityCommandKeyNames;
import org.springframework.data.valkey.connection.lettuce.observability.ValkeyObservation.LowCardinalityCommandKeyNames;

/**
 * Default {@link LettuceObservationConvention} implementation.
 *
 * @author Mark Paluch
 * @since 3.0
 * @deprecated since 3.4 for removal with the next major revision. Use Lettuce's Micrometer integration through
 *             {@link io.lettuce.core.tracing.MicrometerTracing}.
 */
@Deprecated(since = "3.4", forRemoval = true)
record DefaultLettuceObservationConvention(
		boolean includeCommandArgsInSpanTags) implements LettuceObservationConvention {

	@Override
	public KeyValues getLowCardinalityKeyValues(LettuceObservationContext context) {

		Endpoint ep = context.getRequiredEndpoint();
		KeyValues keyValues = KeyValues.of(LowCardinalityCommandKeyNames.DATABASE_SYSTEM.withValue("valkey"), //
				LowCardinalityCommandKeyNames.VALKEY_COMMAND.withValue(context.getRequiredCommand().getType().name()));

		if (ep instanceof SocketAddressEndpoint endpoint) {

			if (endpoint.socketAddress() instanceof InetSocketAddress inet) {
				keyValues = keyValues
						.and(KeyValues.of(LowCardinalityCommandKeyNames.NET_SOCK_PEER_ADDR.withValue(inet.getHostString()),
								LowCardinalityCommandKeyNames.NET_SOCK_PEER_PORT.withValue("" + inet.getPort()),
								LowCardinalityCommandKeyNames.NET_TRANSPORT.withValue("IP.TCP")));
			} else {
				keyValues = keyValues
						.and(KeyValues.of(LowCardinalityCommandKeyNames.NET_PEER_NAME.withValue(endpoint.toString()),
								LowCardinalityCommandKeyNames.NET_TRANSPORT.withValue("Unix")));
			}
		}

		return keyValues;
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(LettuceObservationContext context) {

		ValkeyCommand<?, ?, ?> command = context.getRequiredCommand();

		if (includeCommandArgsInSpanTags) {

			if (command.getArgs() != null) {
				return KeyValues.of(HighCardinalityCommandKeyNames.STATEMENT
						.withValue(command.getType().name() + " " + command.getArgs().toCommandString()));
			}
		}

		return KeyValues.empty();
	}

	@Override
	public String getContextualName(LettuceObservationContext context) {
		return context.getRequiredCommand().getType().name().toLowerCase(Locale.ROOT);
	}
}
