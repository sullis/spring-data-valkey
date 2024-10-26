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
package org.springframework.data.redis.listener;

import static org.springframework.data.redis.connection.ClusterTestVariables.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.data.redis.connection.ValkeyClusterConfiguration;
import org.springframework.data.redis.connection.ValkeyClusterNode;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.test.condition.ValkeyDetector;
import org.springframework.data.redis.test.extension.ValkeyStanalone;

/**
 * Parameters for testing implementations of {@link ReactiveValkeyMessageListenerContainer}
 *
 * @author Mark Paluch
 */
class ReactiveOperationsTestParams {

	public static Collection<Object[]> testParams() {

		LettuceConnectionFactory lettuceConnectionFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class, false);
		LettuceConnectionFactory poolingConnectionFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class, true);

		List<Object[]> list = Arrays.asList(new Object[][] { //
				{ lettuceConnectionFactory, "Standalone" }, //
				{ poolingConnectionFactory, "Pooled" }, //
		});

		if (clusterAvailable()) {

			ValkeyClusterConfiguration clusterConfiguration = new ValkeyClusterConfiguration();
			clusterConfiguration.addClusterNode(new ValkeyClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT));

			LettuceConnectionFactory lettuceClusterConnectionFactory = LettuceConnectionFactoryExtension
					.getConnectionFactory(ValkeyStanalone.class);

			list = new ArrayList<>(list);
			list.add(new Object[] { lettuceClusterConnectionFactory, "Cluster" });
		}

		return list;
	}

	private static boolean clusterAvailable() {
		return ValkeyDetector.isClusterAvailable();
	}

}
