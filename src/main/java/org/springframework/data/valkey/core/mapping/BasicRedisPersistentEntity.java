/*
 * Copyright 2015-2024 the original author or authors.
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
package org.springframework.data.valkey.core.mapping;

import org.springframework.data.annotation.Id;
import org.springframework.data.keyvalue.core.mapping.BasicKeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeySpaceResolver;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.valkey.core.TimeToLive;
import org.springframework.data.valkey.core.TimeToLiveAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ValkeyPersistentEntity} implementation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T>
 */
public class BasicValkeyPersistentEntity<T> extends BasicKeyValuePersistentEntity<T, ValkeyPersistentProperty>
		implements ValkeyPersistentEntity<T> {

	private final TimeToLiveAccessor timeToLiveAccessor;

	/**
	 * Creates new {@link BasicValkeyPersistentEntity}.
	 *
	 * @param information must not be {@literal null}.
	 * @param keySpaceResolver can be {@literal null}.
	 * @param timeToLiveAccessor can be {@literal null}.
	 */
	public BasicValkeyPersistentEntity(TypeInformation<T> information, @Nullable KeySpaceResolver keySpaceResolver,
			TimeToLiveAccessor timeToLiveAccessor) {
		super(information, keySpaceResolver);

		Assert.notNull(timeToLiveAccessor, "TimeToLiveAccessor must not be null");
		this.timeToLiveAccessor = timeToLiveAccessor;
	}

	@Override
	public TimeToLiveAccessor getTimeToLiveAccessor() {
		return this.timeToLiveAccessor;
	}

	@Override
	@Nullable
	public ValkeyPersistentProperty getExplicitTimeToLiveProperty() {
		return this.getPersistentProperty(TimeToLive.class);
	}

	@Override
	@Nullable
	protected ValkeyPersistentProperty returnPropertyIfBetterIdPropertyCandidateOrNull(ValkeyPersistentProperty property) {

		Assert.notNull(property, "Property must not be null");

		if (!property.isIdProperty()) {
			return null;
		}

		ValkeyPersistentProperty currentIdProperty = getIdProperty();
		boolean currentIdPropertyIsSet = currentIdProperty != null;

		if (!currentIdPropertyIsSet) {
			return property;
		}

		boolean currentIdPropertyIsExplicit = currentIdProperty.isAnnotationPresent(Id.class);
		boolean newIdPropertyIsExplicit = property.isAnnotationPresent(Id.class);

		if (currentIdPropertyIsExplicit && newIdPropertyIsExplicit) {
			throw new MappingException(("Attempt to add explicit id property %s but already have a property %s"
					+ " registered as explicit id; Check your mapping configuration")
					.formatted(property.getField(), currentIdProperty.getField()));
		}

		if (!currentIdPropertyIsExplicit && !newIdPropertyIsExplicit) {
			throw new MappingException(("Attempt to add id property %s but already have a property %s registered as id;"
					+ " Check your mapping configuration").formatted(property.getField(), currentIdProperty.getField()));
		}

		if (newIdPropertyIsExplicit) {
			return property;
		}

		return null;
	}
}
