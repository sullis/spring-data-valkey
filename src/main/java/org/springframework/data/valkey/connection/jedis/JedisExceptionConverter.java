/*
 * Copyright 2013-2024 the original author or authors.
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
package org.springframework.data.valkey.connection.jedis;

import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisRedirectionException;

import java.io.IOException;
import java.net.UnknownHostException;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.valkey.ClusterRedirectException;
import org.springframework.data.valkey.ValkeyConnectionFailureException;
import org.springframework.data.valkey.TooManyClusterRedirectionsException;

/**
 * Converts Exceptions thrown from Jedis to {@link DataAccessException}s
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Guy Korland
 * @author Mark Paluch
 */
public class JedisExceptionConverter implements Converter<Exception, DataAccessException> {

	static final JedisExceptionConverter INSTANCE = new JedisExceptionConverter();

	public DataAccessException convert(Exception ex) {

		if (ex instanceof DataAccessException dae) {
			return dae;
		}

		if (ex instanceof JedisClusterOperationException && "No more cluster attempts left".equals(ex.getMessage())) {
			return new TooManyClusterRedirectionsException(ex.getMessage(), ex);
		}

		if (ex instanceof JedisRedirectionException rex) {

			return new ClusterRedirectException(rex.getSlot(), rex.getTargetNode().getHost(), rex.getTargetNode().getPort(),
					ex);
		}

		if (ex instanceof JedisConnectionException) {
			return new ValkeyConnectionFailureException(ex.getMessage(), ex);
		}

		if (ex instanceof JedisException || ex instanceof UnsupportedOperationException) {
			return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
		}

		if (ex instanceof UnknownHostException) {
			return new ValkeyConnectionFailureException("Unknown host " + ex.getMessage(), ex);
		}

		if (ex instanceof IOException) {
			return new ValkeyConnectionFailureException("Could not connect to Valkey server", ex);
		}

		return null;
	}
}