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
package org.springframework.data.valkey.listener;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.valkey.connection.DefaultMessage;
import org.springframework.data.valkey.connection.Message;
import org.springframework.data.valkey.core.ValkeyKeyExpiredEvent;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class KeyExpirationEventMessageListenerUnitTests {

	private static final String MESSAGE_CHANNEL = "channel";
	private static final String MESSAGE_BODY = "body";
	private static final Message MESSAGE = new DefaultMessage(MESSAGE_CHANNEL.getBytes(), MESSAGE_BODY.getBytes());

	@Mock ValkeyMessageListenerContainer containerMock;
	@Mock ApplicationEventPublisher publisherMock;
	KeyExpirationEventMessageListener listener;

	@BeforeEach
	void setUp() {

		listener = new KeyExpirationEventMessageListener(containerMock);
		listener.setApplicationEventPublisher(publisherMock);
	}

	@Test // DATAREDIS-425
	void handleMessageShouldPublishKeyExpiredEvent() {

		listener.onMessage(MESSAGE, "*".getBytes());

		ArgumentCaptor<ApplicationEvent> captor = ArgumentCaptor.forClass(ApplicationEvent.class);

		verify(publisherMock, times(1)).publishEvent(captor.capture());
		assertThat(captor.getValue()).isInstanceOf(ValkeyKeyExpiredEvent.class);
		assertThat((byte[]) captor.getValue().getSource()).isEqualTo(MESSAGE_BODY.getBytes());
	}

	@Test // DATAREDIS-425, DATAREDIS-692
	void handleMessageShouldNotRespondToEmptyMessage() {

		listener.onMessage(new DefaultMessage(new byte[] {}, new byte[] {}), "*".getBytes());

		verifyNoInteractions(publisherMock);
	}
}
