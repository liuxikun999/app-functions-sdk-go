//
// Copyright (c) 2023 Intel Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package transforms

import (
	"fmt"
	"strings"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/liuxikun999/app-functions-sdk-go/v3/internal/common"

	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/interfaces"
	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/secure"
)

// MQTTExternalClient ...
type MQTTExternalClient struct {
	lock              sync.Mutex
	client            MQTT.Client
	mqttConfig        MQTTExternalConfig
	opts              *MQTT.ClientOptions
	subscriptionMutex *sync.Mutex
}

// MQTTExternalConfig ...
type MQTTExternalConfig struct {
	// BrokerAddress should be set to the complete broker address i.e. mqtts://mosquitto:8883/mybroker
	BrokerAddress string
	// ClientId to connect with the broker with.
	ClientId string
	// The name of the secret in secret provider to retrieve your secrets
	SecretName string
	// AutoReconnect indicated whether or not to retry connection if disconnected
	AutoReconnect bool
	// KeepAlive is the interval duration between client sending keepalive ping to broker
	KeepAlive string
	// ConnectTimeout is the duration for timing out on connecting to the broker
	ConnectTimeout string
	// Topic that you wish to publish to
	Topic           string
	SubscribeTopics string
	// QoS for MQTT Connection
	QoS byte
	// Retain setting for MQTT Connection
	Retain bool
	// SkipCertVerify
	SkipCertVerify bool
	// AuthMode indicates what to use when connecting to the broker. Options are "none", "cacert" , "usernamepassword", "clientcert".
	// If a CA Cert exists in the SecretName then it will be used for all modes except "none".
	AuthMode string
	// Will contains the Last Will configuration for the MQTT Client
	Will common.WillConfig
}

// NewMQTTExternalClient ...
func NewMQTTExternalClient(mqttConfig MQTTExternalConfig) *MQTTExternalClient {
	opts := MQTT.NewClientOptions()

	opts.AddBroker(mqttConfig.BrokerAddress)
	opts.SetClientID(mqttConfig.ClientId)
	opts.SetAutoReconnect(mqttConfig.AutoReconnect)

	//avoid casing issues
	mqttConfig.AuthMode = strings.ToLower(mqttConfig.AuthMode)
	sender := &MQTTExternalClient{
		client:     nil,
		mqttConfig: mqttConfig,
		opts:       opts,
	}

	return sender
}

func (sender *MQTTExternalClient) InitializeMQTTExternalClient(ctx interfaces.AppFunctionContext, messageHandler MQTT.MessageHandler) error {
	sender.lock.Lock()
	defer sender.lock.Unlock()

	if sender.client != nil {
		return nil
	}

	ctx.LoggingClient().Info("Initializing MQTTExternalClient")

	config := sender.mqttConfig
	mqttFactory := secure.NewMqttFactory(ctx.SecretProvider(), ctx.LoggingClient(), config.AuthMode, config.SecretName, config.SkipCertVerify)

	if len(sender.mqttConfig.KeepAlive) > 0 {
		keepAlive, err := time.ParseDuration(sender.mqttConfig.KeepAlive)
		if err != nil {
			return fmt.Errorf("in pipeline '%s', unable to parse KeepAlive value of '%s': %s", ctx.PipelineId(), sender.mqttConfig.KeepAlive, err.Error())
		}

		sender.opts.SetKeepAlive(keepAlive)
	}

	if len(sender.mqttConfig.ConnectTimeout) > 0 {
		timeout, err := time.ParseDuration(sender.mqttConfig.ConnectTimeout)
		if err != nil {
			return fmt.Errorf("in pipeline '%s', unable to parse ConnectTimeout value of '%s': %s", ctx.PipelineId(), sender.mqttConfig.ConnectTimeout, err.Error())
		}

		sender.opts.SetConnectTimeout(timeout)
	}

	if config.Will.Enabled {
		sender.opts.SetWill(config.Will.Topic, config.Will.Payload, config.Will.Qos, config.Will.Retained)
		ctx.LoggingClient().Infof("Last Will options set for MQTT Export: %+v", config.Will)
	}

	client, err := mqttFactory.Create(sender.opts)
	if err != nil {
		return fmt.Errorf("in pipeline '%s', unable to create MQTT Client: %s", ctx.PipelineId(), err.Error())
	}

	sender.client = client

	// 订阅Topic
	subscribeTopics := strings.TrimSpace(config.SubscribeTopics)
	topics := strings.Split(subscribeTopics, ",")
	if len(topics) > 0 {
		if err := sender.client.Subscribe(subscribeTopics, config.QoS, messageHandler); err != nil {
			return fmt.Errorf("failed to subscribe to topic(s) '%s': %s", subscribeTopics, err.Error())
		}
		sender.subscriptionMutex.Lock()
		defer sender.subscriptionMutex.Unlock()

		for _, topic := range topics {
			if err := sender.client.Subscribe(topic, config.QoS, messageHandler); err != nil {
				return fmt.Errorf("failed to subscribe to topic(s) '%s': %s", topic, err.Error())
			}
		}
	}
	return nil
}

func (sender *MQTTExternalClient) connectToExternalBroker(ctx interfaces.AppFunctionContext) error {
	sender.lock.Lock()
	defer sender.lock.Unlock()

	// If other thread made the connection while this one was waiting for the lock
	// then skip trying to connect
	if sender.client.IsConnected() {
		return nil
	}

	ctx.LoggingClient().Info("Connecting to mqtt server for export")
	if token := sender.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("could not connect to mqtt external server  Error: %s", token.Error().Error())
	}
	ctx.LoggingClient().Infof("Connected to mqtt server for export in pipeline '%s'", ctx.PipelineId())
	return nil
}

func (sender *MQTTExternalClient) MQTTExternalPublish(topic string, payload string) (bool, interface{}) {
	if sender.client == nil || !sender.client.IsConnected() || !sender.client.IsConnectionOpen() {
		return false, fmt.Errorf("connection to mqtt external server not open")
	}
	if err := sender.client.Publish(topic, sender.mqttConfig.QoS, sender.mqttConfig.Retain, payload); err != nil {
		return false, fmt.Errorf("could not Publish to mqtt external server, topic: %s, payload: %s,  Error: %s", topic, payload, err.Error())
	}
	return true, nil
}
