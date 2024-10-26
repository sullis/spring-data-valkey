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
package org.springframework.data.redis.core.script;

import java.util.List;

import org.springframework.data.redis.ValkeySystemException;
import org.springframework.data.redis.connection.ValkeyConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.ValkeyCallback;
import org.springframework.data.redis.core.ValkeyTemplate;
import org.springframework.data.redis.serializer.ValkeySerializer;

/**
 * Default implementation of {@link ScriptExecutor}. Optimizes performance by attempting to execute script first using
 * evalsha, then falling back to eval if Valkey has not yet cached the script. Evalsha is not attempted if the script is
 * executed in a pipeline or transaction.
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @param <K> The type of keys that may be passed during script execution
 */
public class DefaultScriptExecutor<K> implements ScriptExecutor<K> {

	private final ValkeyTemplate<K, ?> template;

	/**
	 * @param template The {@link ValkeyTemplate} to use
	 */
	public DefaultScriptExecutor(ValkeyTemplate<K, ?> template) {
		this.template = template;
	}

	@SuppressWarnings("unchecked")
	public <T> T execute(final ValkeyScript<T> script, final List<K> keys, final Object... args) {
		// use the Template's value serializer for args and result
		return execute(script, template.getValueSerializer(), (ValkeySerializer<T>) template.getValueSerializer(), keys,
				args);
	}

	public <T> T execute(final ValkeyScript<T> script, final ValkeySerializer<?> argsSerializer,
			final ValkeySerializer<T> resultSerializer, final List<K> keys, final Object... args) {
		return template.execute((ValkeyCallback<T>) connection -> {
			final ReturnType returnType = ReturnType.fromJavaType(script.getResultType());
			final byte[][] keysAndArgs = keysAndArgs(argsSerializer, keys, args);
			final int keySize = keys != null ? keys.size() : 0;
			if (connection.isPipelined() || connection.isQueueing()) {
				// We could script load first and then do evalsha to ensure sha is present,
				// but this adds a sha1 to exec/closePipeline results. Instead, just eval
				connection.eval(scriptBytes(script), returnType, keySize, keysAndArgs);
				return null;
			}
			return eval(connection, script, returnType, keySize, keysAndArgs, resultSerializer);
		});
	}

	protected <T> T eval(ValkeyConnection connection, ValkeyScript<T> script, ReturnType returnType, int numKeys,
			byte[][] keysAndArgs, ValkeySerializer<T> resultSerializer) {

		Object result;
		try {
			result = connection.evalSha(script.getSha1(), returnType, numKeys, keysAndArgs);
		} catch (Exception ex) {

			if (!ScriptUtils.exceptionContainsNoScriptError(ex)) {
				throw ex instanceof RuntimeException runtimeException ? runtimeException
						: new ValkeySystemException(ex.getMessage(), ex);
			}

			result = connection.eval(scriptBytes(script), returnType, numKeys, keysAndArgs);
		}

		if (script.getResultType() == null) {
			return null;
		}

		return deserializeResult(resultSerializer, result);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected byte[][] keysAndArgs(ValkeySerializer argsSerializer, List<K> keys, Object[] args) {
		final int keySize = keys != null ? keys.size() : 0;
		byte[][] keysAndArgs = new byte[args.length + keySize][];
		int i = 0;
		if (keys != null) {
			for (K key : keys) {
				if (keySerializer() == null && key instanceof byte[] keyBytes) {
					keysAndArgs[i++] = keyBytes;
				} else {
					keysAndArgs[i++] = keySerializer().serialize(key);
				}
			}
		}
		for (Object arg : args) {
			if (argsSerializer == null && arg instanceof byte[] argBytes) {
				keysAndArgs[i++] = argBytes;
			} else {
				keysAndArgs[i++] = argsSerializer.serialize(arg);
			}
		}
		return keysAndArgs;
	}

	protected byte[] scriptBytes(ValkeyScript<?> script) {
		return template.getStringSerializer().serialize(script.getScriptAsString());
	}

	protected <T> T deserializeResult(ValkeySerializer<T> resultSerializer, Object result) {
		return ScriptUtils.deserializeResult(resultSerializer, result);
	}

	@SuppressWarnings("rawtypes")
	protected ValkeySerializer keySerializer() {
		return template.getKeySerializer();
	}
}
