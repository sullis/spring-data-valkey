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
package org.springframework.data.valkey.connection;

import org.springframework.lang.Nullable;

/**
 * Interface defining the commands supported by Valkey.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface ValkeyCommands extends ValkeyKeyCommands, ValkeyStringCommands, ValkeyListCommands, ValkeySetCommands,
		ValkeyZSetCommands, ValkeyHashCommands, ValkeyTxCommands, ValkeyPubSubCommands, ValkeyConnectionCommands,
		ValkeyServerCommands, ValkeyStreamCommands, ValkeyScriptingCommands, ValkeyGeoCommands, ValkeyHyperLogLogCommands {

	/**
	 * {@literal Native} or {@literal raw} execution of the given Valkey command along with the given arguments.
	 * <p>
	 * The command is executed as is, with as little interpretation as possible - it is up to the caller to take care
	 * of any processing of arguments or the result.
	 *
	 * @param command Valkey {@link String command} to execute; must not be {@literal null}.
	 * @param args optional array of command arguments; may be empty;
	 * @return the execution result; may be {@literal null}.
	 */
	@Nullable
	Object execute(String command, byte[]... args);

}
