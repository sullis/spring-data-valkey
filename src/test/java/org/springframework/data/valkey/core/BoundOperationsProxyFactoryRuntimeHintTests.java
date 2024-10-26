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
package org.springframework.data.valkey.core;

import org.junit.jupiter.api.Test;
import org.springframework.aop.scope.ScopedObject;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.test.agent.EnabledIfRuntimeHintsAgent;
import org.springframework.aot.test.agent.RuntimeHintsInvocations;
import org.springframework.aot.test.agent.RuntimeHintsRecorder;
import org.springframework.data.valkey.aot.ValkeyRuntimeHints;
import org.springframework.data.valkey.connection.DataType;
import org.springframework.data.valkey.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.valkey.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.valkey.test.extension.ValkeyStanalone;

/**
 * @author Christoph Strobl
 */
@EnabledIfRuntimeHintsAgent
class BoundOperationsProxyFactoryRuntimeHintTests {

	@Test // GH-2395
	void boundOpsRuntimeHints() {

		LettuceConnectionFactory connectionFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);
		ValkeyTemplate template = new ValkeyTemplate<>();
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		BoundOperationsProxyFactory factory = new BoundOperationsProxyFactory();
		BoundListOperations listOp = factory.createProxy(BoundListOperations.class, "key", DataType.LIST, template,
				ValkeyOperations::opsForList);

		RuntimeHintsInvocations invocations = RuntimeHintsRecorder.record(() -> {
			listOp.trim(0, 10);
		});

		RuntimeHints hints = ValkeyRuntimeHints.redisHints(it -> {
			// hints that should come from another module
			it.reflection().registerType(ScopedObject.class,
					hint -> hint.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS));
		});

		invocations.assertThat().match(hints);
	}
}
