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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.ValkeyCredentials;
import io.lettuce.core.ValkeyCredentialsProvider;
import reactor.core.publisher.Mono;

import org.springframework.data.redis.connection.ValkeyConfiguration;
import org.springframework.data.redis.connection.ValkeySentinelConfiguration;
import org.springframework.lang.Nullable;

/**
 * Factory interface to create {@link ValkeyCredentialsProvider} from a {@link ValkeyConfiguration}. Credentials can be
 * associated with {@link ValkeyCredentials#hasUsername() username} and/or {@link ValkeyCredentials#hasPassword()
 * password}.
 * <p>
 * Credentials are based off the given {@link ValkeyConfiguration} objects. Changing the credentials in the actual object
 * affects the constructed {@link ValkeyCredentials} object. Credentials are requested by the Lettuce client after
 * connecting to the host. Therefore, credential retrieval is subject to complete within the configured connection
 * creation timeout to avoid connection failures.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public interface ValkeyCredentialsProviderFactory {

	/**
	 * Create a {@link ValkeyCredentialsProvider} for data node authentication given {@link ValkeyConfiguration}.
	 *
	 * @param redisConfiguration the {@link ValkeyConfiguration} object.
	 * @return a {@link ValkeyCredentialsProvider} that emits {@link ValkeyCredentials} for data node authentication.
	 */
	@Nullable
	default ValkeyCredentialsProvider createCredentialsProvider(ValkeyConfiguration redisConfiguration) {

		if (redisConfiguration instanceof ValkeyConfiguration.WithAuthentication
				&& ((ValkeyConfiguration.WithAuthentication) redisConfiguration).getPassword().isPresent()) {

			return ValkeyCredentialsProvider.from(() -> {

				ValkeyConfiguration.WithAuthentication withAuthentication = (ValkeyConfiguration.WithAuthentication) redisConfiguration;

				return ValkeyCredentials.just(withAuthentication.getUsername(), withAuthentication.getPassword().get());
			});
		}

		return () -> Mono.just(AbsentValkeyCredentials.ANONYMOUS);
	}

	/**
	 * Create a {@link ValkeyCredentialsProvider} for Sentinel node authentication given
	 * {@link ValkeySentinelConfiguration}.
	 *
	 * @param redisConfiguration the {@link ValkeySentinelConfiguration} object.
	 * @return a {@link ValkeyCredentialsProvider} that emits {@link ValkeyCredentials} for sentinel authentication.
	 */
	default ValkeyCredentialsProvider createSentinelCredentialsProvider(ValkeySentinelConfiguration redisConfiguration) {

		if (redisConfiguration.getSentinelPassword().isPresent()) {

			return ValkeyCredentialsProvider.from(() -> ValkeyCredentials.just(redisConfiguration.getSentinelUsername(),
					redisConfiguration.getSentinelPassword().get()));
		}

		return () -> Mono.just(AbsentValkeyCredentials.ANONYMOUS);
	}

	/**
	 * Default anonymous {@link ValkeyCredentials} without username/password.
	 */
	enum AbsentValkeyCredentials implements ValkeyCredentials {

		ANONYMOUS;

		@Override
		@Nullable
		public String getUsername() {
			return null;
		}

		@Override
		public boolean hasUsername() {
			return false;
		}

		@Override
		@Nullable
		public char[] getPassword() {
			return null;
		}

		@Override
		public boolean hasPassword() {
			return false;
		}
	}
}
