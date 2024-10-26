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
package org.springframework.data.valkey.core.script;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.core.io.ClassPathResource;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.scripting.support.StaticScriptSource;

/**
 * Test of {@link DefaultValkeyScript}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class DefaultValkeyScriptTests {

	@Test
	void testGetSha1() {

		StaticScriptSource script = new StaticScriptSource("return KEYS[1]");
		DefaultValkeyScript<String> redisScript = new DefaultValkeyScript<>();
		redisScript.setScriptSource(script);
		redisScript.setResultType(String.class);
		String sha1 = valkeyScript.getSha1();
		// Ensure multiple calls return same sha
		assertThat(redisScript.getSha1()).isEqualTo(sha1);
		script.setScript("return KEYS[2]");
		// Sha should now be different as script text has changed
		assertThat(sha1).isNotEqualTo(redisScript.getSha1());
	}

	@Test
	void testGetScriptAsString() {

		DefaultValkeyScript<String> redisScript = new DefaultValkeyScript<>();
		redisScript.setScriptText("return ARGS[1]");
		redisScript.setResultType(String.class);
		assertThat(redisScript.getScriptAsString()).isEqualTo("return ARGS[1]");
	}

	@Test // DATAREDIS-1030
	void testGetScriptAsStringFromResource() {

		ValkeyScript<String> redisScript = ValkeyScript
				.of(new ClassPathResource("org/springframework/data/valkey/core/script/cas.lua"));
		assertThat(redisScript.getScriptAsString()).startsWith("local current = valkey.call('GET', KEYS[1])");
	}

	@Test
	void testGetScriptAsStringError() {

		DefaultValkeyScript<Long> redisScript = new DefaultValkeyScript<>();
		redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("nonexistent")));
		redisScript.setResultType(Long.class);

		assertThatExceptionOfType(ScriptingException.class).isThrownBy(redisScript::getScriptAsString);
	}

	@Test
	void initializeWithNoScript() throws Exception {

		DefaultValkeyScript<Long> redisScript = new DefaultValkeyScript<>();
		assertThatIllegalStateException().isThrownBy(redisScript::afterPropertiesSet);
	}
}
