/*
 * Copyright 2018-2024 the original author or authors.
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

import io.lettuce.core.AbstractValkeyClient;

/**
 * Extension to {@link LettuceConnectionProvider} for providers exposing {@link io.lettuce.core.ValkeyClient}.
 *
 * @author Mark Paluch
 * @since 2.0.5
 */
interface ValkeyClientProvider {

	/**
	 * Returns the underlying Valkey Client.
	 *
	 * @return the {@link AbstractValkeyClient}. Never {@literal null}.
	 */
	AbstractValkeyClient getValkeyClient();
}
