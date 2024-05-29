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
	"encoding/json"
	"fmt"
	bootstrapInterfaces "github.com/edgexfoundry/go-mod-bootstrap/v3/bootstrap/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/clients/logger"
	coreCommon "github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/util"
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
	Username   string
	Password   string
	// AutoReconnect indicated whether or not to retry connection if disconnected
	AutoReconnect bool
	Enable        bool
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
	opts.SetUsername(mqttConfig.Username)
	opts.SetPassword(mqttConfig.Password)

	//avoid casing issues
	mqttConfig.AuthMode = strings.ToLower(mqttConfig.AuthMode)
	sender := &MQTTExternalClient{
		client:            nil,
		mqttConfig:        mqttConfig,
		opts:              opts,
		subscriptionMutex: new(sync.Mutex),
	}

	return sender
}

func (sender *MQTTExternalClient) InitializeMQTTExternalClient(ctx interfaces.ApplicationService) error {
	return sender.initializeMqttExternalClientPrivate(ctx.SecretProvider(), ctx.LoggingClient())
}

func (sender *MQTTExternalClient) initializeMqttExternalClientPrivate(secretProvider bootstrapInterfaces.SecretProvider, loggingClient logger.LoggingClient) error {
	sender.lock.Lock()
	defer sender.lock.Unlock()

	if sender.client != nil {
		return nil
	}

	loggingClient.Info("Initializing MQTTExternalClient")

	config := sender.mqttConfig
	mqttFactory := secure.NewMqttFactory(secretProvider, loggingClient, config.AuthMode, config.SecretName, config.SkipCertVerify)

	if len(sender.mqttConfig.KeepAlive) > 0 {
		keepAlive, err := time.ParseDuration(sender.mqttConfig.KeepAlive)
		if err != nil {
			return fmt.Errorf("unable to parse KeepAlive value of '%s': %s", sender.mqttConfig.KeepAlive, err.Error())
		}

		sender.opts.SetKeepAlive(keepAlive)
	}

	if len(sender.mqttConfig.ConnectTimeout) > 0 {
		timeout, err := time.ParseDuration(sender.mqttConfig.ConnectTimeout)
		if err != nil {
			return fmt.Errorf("unable to parse ConnectTimeout value of '%s': %s", sender.mqttConfig.ConnectTimeout, err.Error())
		}

		sender.opts.SetConnectTimeout(timeout)
	}

	if config.Will.Enabled {
		sender.opts.SetWill(config.Will.Topic, config.Will.Payload, config.Will.Qos, config.Will.Retained)
		loggingClient.Infof("Last Will options set for MQTT Export: %+v", config.Will)
	}

	client, err := mqttFactory.Create(sender.opts)
	if err != nil {
		return fmt.Errorf("unable to create MQTT Client: %s", err.Error())
	}

	sender.client = client
	// Avoid reconnecting if already connected.
	if sender.client.IsConnected() {
		return nil
	}

	optionsReader := sender.client.OptionsReader()

	return getTokenError(
		sender.client.Connect(),
		optionsReader.ConnectTimeout(),
		"Connect",
		"Unable to connect")
}

func (sender *MQTTExternalClient) Subscribe(topics []types.TopicChannel, messageErrors chan error) error {
	sender.subscriptionMutex.Lock()
	defer sender.subscriptionMutex.Unlock()
	// 订阅Topic
	optionsReader := sender.client.OptionsReader()
	if len(topics) > 0 {
		for _, topic := range topics {
			handler := newMessageHandler(topic.Messages, messageErrors)
			qos := optionsReader.WillQos()

			token := sender.client.Subscribe(topic.Topic, qos, handler)
			err := getTokenError(token, optionsReader.ConnectTimeout(), "Subscribe", "Failed to create external subscription")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getTokenError(token MQTT.Token, timeout time.Duration, operation string, defaultTimeoutMessage string) error {
	hasTimedOut := !token.WaitTimeout(timeout)

	if hasTimedOut && token.Error() != nil {
		return fmt.Errorf(operation, token.Error().Error())
	}

	if hasTimedOut && token.Error() == nil {
		return fmt.Errorf(operation, defaultTimeoutMessage)
	}

	if token.Error() != nil {
		return fmt.Errorf(operation, token.Error().Error())
	}

	return nil
}

func newMessageHandler(
	messageChannel chan<- types.MessageEnvelope,
	errorChannel chan<- error) MQTT.MessageHandler {
	return func(client MQTT.Client, message MQTT.Message) {
		var messageEnvelope types.MessageEnvelope
		payload := message.Payload()
		err := json.Unmarshal(payload, &messageEnvelope)
		if err != nil {
			errorChannel <- err
			return
		}
		messageEnvelope.ReceivedTopic = message.Topic()
		messageChannel <- messageEnvelope
	}
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

func (sender *MQTTExternalClient) MQTTExternalSend(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
	if data == nil {
		// We didn't receive a result
		return false, fmt.Errorf("function MQTTSend in pipeline '%s': No Data Received", ctx.PipelineId())
	}

	exportData, err := util.CoerceType(data)
	if err != nil {
		return false, err
	}
	// if we haven't initialized the client yet OR the cache has been invalidated (due to new/updated secrets) we need to (re)initialize the client
	if sender.client == nil {
		err := sender.initializeMqttExternalClientPrivate(ctx.SecretProvider(), ctx.LoggingClient())
		if err != nil {
			return false, err
		}
	}

	publishTopic := sender.mqttConfig.Topic

	if !sender.client.IsConnected() {
		err := sender.connectToExternalBroker(ctx)
		if err != nil {
			return false, err
		}
	}

	if !sender.client.IsConnectionOpen() {
		return false, fmt.Errorf("in pipeline '%s', 连接外部MQTT服务器失败", ctx.PipelineId())
	}

	token := sender.client.Publish(publishTopic, sender.mqttConfig.QoS, sender.mqttConfig.Retain, exportData)
	token.Wait()
	if token.Error() != nil {
		return false, token.Error()
	}

	// capture the size for metrics
	exportDataBytes := len(exportData)

	ctx.LoggingClient().Debugf("Sent %d bytes of data to MQTT External Broker in pipeline '%s'", exportDataBytes, ctx.PipelineId())
	ctx.LoggingClient().Tracef("Data exported", "Transport", "ExternalMQTT", "pipeline", ctx.PipelineId(), coreCommon.CorrelationHeader, ctx.CorrelationID())

	return true, nil
}
