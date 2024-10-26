/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.valkey.config;

import static org.assertj.core.api.Assertions.*;

import jakarta.annotation.Resource;

import org.junit.jupiter.api.Test;

import org.springframework.data.valkey.core.ClusterOperations;
import org.springframework.data.valkey.core.GeoOperations;
import org.springframework.data.valkey.core.HashOperations;
import org.springframework.data.valkey.core.HyperLogLogOperations;
import org.springframework.data.valkey.core.SetOperations;
import org.springframework.data.valkey.core.StreamOperations;
import org.springframework.data.valkey.core.ValueOperations;
import org.springframework.data.valkey.core.ZSetOperations;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests to obtain various template resources through {@code @Resource} injection.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig(locations = "namespace.xml")
class PropertyEditorSupportIntegrationTests {

	@Resource(name = "valkeyTemplate") ClusterOperations<String, String> cluster;
	@Resource(name = "valkeyTemplate") GeoOperations<String, String> geo;
	@Resource(name = "valkeyTemplate") HashOperations<String, String, String> hash;
	@Resource(name = "valkeyTemplate") HyperLogLogOperations<String, String> hll;
	@Resource(name = "valkeyTemplate") SetOperations<String, String> set;
	@Resource(name = "valkeyTemplate") StreamOperations<String, String, String> stream;
	@Resource(name = "valkeyTemplate") ValueOperations<String, String> value;
	@Resource(name = "valkeyTemplate") ZSetOperations<String, String> zSet;

	@Test // GH-2828, GH-2825
	void shouldInjectResources() {

		assertThat(cluster).isNotNull();
		assertThat(geo).isNotNull();
		assertThat(hash).isNotNull();
		assertThat(hll).isNotNull();
		assertThat(set).isNotNull();
		assertThat(stream).isNotNull();
		assertThat(value).isNotNull();
		assertThat(zSet).isNotNull();
	}
}
