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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.function.Try;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.springframework.data.valkey.connection.ValkeyConnectionFactory;

/**
 * {@link ExecutionCondition} for {@link EnabledOnValkeyDriverCondition @EnabledOnValkeyDriver}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see EnabledOnValkeyDriver
 */
class EnabledOnValkeyDriverCondition implements ExecutionCondition {

	private static final ConditionEvaluationResult ENABLED_BY_DEFAULT = enabled("@EnabledOnValkeyDriver is not present");

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

		Optional<EnabledOnValkeyDriver> optional = AnnotationUtils.findAnnotation(context.getElement(),
				EnabledOnValkeyDriver.class);

		if (!optional.isPresent()) {
			return ENABLED_BY_DEFAULT;
		}

		EnabledOnValkeyDriver annotation = optional.get();
		Class<?> testClass = context.getRequiredTestClass();

		List<Field> annotatedFields = AnnotationUtils.findAnnotatedFields(testClass,
				EnabledOnValkeyDriver.DriverQualifier.class, it -> ValkeyConnectionFactory.class.isAssignableFrom(it.getType()));

		if (annotatedFields.isEmpty()) {
			throw new IllegalStateException(
					"@EnabledOnValkeyDriver requires a field of type \"ValkeyConnectionFactory\" annotated with @DriverQualifier");
		}

		if (context.getTestInstance().isEmpty()) {
			return ENABLED_BY_DEFAULT;
		}

		for (Field field : annotatedFields) {

			Try<Object> fieldValue = ReflectionUtils.tryToReadFieldValue(field, context.getRequiredTestInstance());

			ValkeyConnectionFactory value = (ValkeyConnectionFactory) fieldValue
					.getOrThrow(e -> new IllegalStateException("Cannot read field " + field, e));

			boolean foundMatch = false;
			for (ValkeyDriver redisDriver : annotation.value()) {
				if (redisDriver.matches(value)) {
					foundMatch = true;
				}
			}

			if (!foundMatch) {
				return disabled("Driver %s not supported; Supported driver(s): %s"
						.formatted(formatUnsupportedDriver(value), Arrays.toString(annotation.value())));
			}
		}

		return enabled("Found enabled driver(s): %s".formatted(Arrays.toString(annotation.value())));

	}

	private static String formatUnsupportedDriver(ValkeyConnectionFactory value) {

		for (ValkeyDriver redisDriver : ValkeyDriver.values()) {
			if (redisDriver.matches(value)) {
				return redisDriver.toString();
			}
		}

		return value.toString();
	}
}