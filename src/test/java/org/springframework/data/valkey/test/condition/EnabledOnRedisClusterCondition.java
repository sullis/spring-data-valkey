/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.valkey.test.condition;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.*;

import java.util.Optional;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;
import org.springframework.data.valkey.SettingsUtils;

/**
 * {@link ExecutionCondition} for {@link EnabledOnValkeyClusterCondition @EnabledOnValkeyClusterAvailable}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see EnabledOnValkeyClusterCondition
 */
class EnabledOnValkeyClusterCondition implements ExecutionCondition {

	private static final ConditionEvaluationResult ENABLED_BY_DEFAULT = enabled(
			"@EnabledOnClusterAvailable is not present");

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

		Optional<EnabledOnValkeyClusterAvailable> optional = AnnotationUtils.findAnnotation(context.getElement(),
				EnabledOnValkeyClusterAvailable.class);

		if (!optional.isPresent()) {
			return ENABLED_BY_DEFAULT;
		}

		if (ValkeyDetector.isClusterAvailable()) {
			return enabled("Connection successful to Valkey Cluster at %s:%d".formatted(SettingsUtils.getHost(),
					SettingsUtils.getClusterPort()));
		}

		return disabled("Cannot connect to Valkey Cluster at %s:%d".formatted(SettingsUtils.getHost(),
				SettingsUtils.getClusterPort()));
	}

}
