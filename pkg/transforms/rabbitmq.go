//
// Copyright (c) 2021 Intel Corporation
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
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/liuxikun999/app-functions-sdk-go/v3/internal"
	gometrics "github.com/rcrowley/go-metrics"
	rabbitMq "github.com/streadway/amqp"

	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/interfaces"
	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/util"
)

// RabbitMQSecretSender ...
type RabbitMQSecretSender struct {
	lock                 sync.Mutex
	connection           *rabbitMq.Connection
	channel              *rabbitMq.Channel
	mqttConfig           RabbitMQSecretConfig
	persistOnError       bool
	opts                 *rabbitMq.Config
	secretsLastRetrieved time.Time
	topicFormatter       StringValuesFormatter
	mqttSizeMetrics      gometrics.Histogram
}

// RabbitMQSecretConfig ...
type RabbitMQSecretConfig struct {
	UserName string
	Password string
	Host     string
	Port     int
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
	Topic string
	// ExchangeName that you wish to Declare to
	ExchangeName string
	ExchangeType string
	RoutingKey   string
	Mandatory    bool
	Immediate    bool
	// QoS for MQTT Connection
	QoS byte
	// Retain setting for MQTT Connection
	Retain bool
	// SkipCertVerify
	SkipCertVerify bool
	// AuthMode indicates what to use when connecting to the broker. Options are "none", "cacert" , "usernamepassword", "clientcert".
	// If a CA Cert exists in the SecretName then it will be used for all modes except "none".
	AuthMode string
}

// NewMQTTSecretSender ...
func NewRabbitMQSecretSender(mqttConfig RabbitMQSecretConfig, persistOnError bool) *RabbitMQSecretSender {
	opts := &rabbitMq.Config{}
	//avoid casing issues
	mqttConfig.AuthMode = strings.ToLower(mqttConfig.AuthMode)
	sender := &RabbitMQSecretSender{
		connection:     nil,
		channel:        nil,
		mqttConfig:     mqttConfig,
		persistOnError: persistOnError,
		opts:           opts,
	}

	return sender
}

// NewRabbitMQSecretSenderWithTopicFormatter allows passing a function to build a final publish topic
// from the combination of the configured topic and the input parameters passed to MQTTSend
func NewRabbitMQSecretSenderWithTopicFormatter(mqttConfig RabbitMQSecretConfig, persistOnError bool, topicFormatter StringValuesFormatter) *RabbitMQSecretSender {
	sender := NewRabbitMQSecretSender(mqttConfig, persistOnError)
	sender.topicFormatter = topicFormatter
	return sender
}

// 建立连接
func (sender *RabbitMQSecretSender) createRabbitMqConnect(ctx interfaces.AppFunctionContext, exportData []byte) error {
	sender.lock.Lock()
	defer sender.lock.Unlock()

	// If other thread made the connection while this one was waiting for the lock
	// then skip trying to connect
	if sender.connection != nil && !sender.connection.IsClosed() {
		return nil
	}

	ctx.LoggingClient().Info("Connecting to rabbitmq server for export")
	// 对密码进行URL编码
	encodedPassword := url.QueryEscape(sender.mqttConfig.Password)
	// 构建RabbitMQ连接URL
	rabbitMQURL := fmt.Sprintf("amqp://%s:%s@%s:%d", sender.mqttConfig.UserName, encodedPassword, sender.mqttConfig.Host, sender.mqttConfig.Port)
	ctx.LoggingClient().Info("Connecting to : %s", rabbitMQURL)
	connection, err := rabbitMq.Dial(rabbitMQURL)
	if connection == nil || connection.IsClosed() || err != nil {
		sender.setRetryData(ctx, exportData)
		subMessage := "dropping event"
		if sender.persistOnError {
			subMessage = "persisting Event for later retry"
		}
		return fmt.Errorf("in pipeline '%s', could not connect to rabbtMq server for export, %s. Error: %s", ctx.PipelineId(), subMessage, err.Error())
	}
	sender.connection = connection

	ctx.LoggingClient().Infof("Connected to rabbtMq server for export in pipeline '%s'", ctx.PipelineId())
	return nil
}

// 建立通道
func (sender *RabbitMQSecretSender) openRabbitMqChannel(ctx interfaces.AppFunctionContext, exportData []byte) error {
	sender.lock.Lock()
	defer sender.lock.Unlock()
	if sender.channel != nil {
		return nil
	}
	ch, err := sender.connection.Channel()
	if err != nil {
		return fmt.Errorf("in pipeline '%s', cannot create channel: %v", ctx.PipelineId(), err)
	}
	sender.channel = ch

	if err = ch.ExchangeDeclare(
		sender.mqttConfig.ExchangeName, // name of the exchange
		sender.mqttConfig.ExchangeType, // type
		true,                           // durable
		false,                          // delete when complete
		false,                          // internal
		false,                          // noWait
		nil,                            // arguments
	); err != nil {
		return fmt.Errorf("in pipeline '%s', cannot declare exchange: %v", ctx.PipelineId(), err)
	}
	ctx.LoggingClient().Infof("Create Channel to rabbtMq server for export in pipeline '%s'", ctx.PipelineId())
	return nil
}

func (sender *RabbitMQSecretSender) RabbitMQSend(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
	if data == nil {
		// We didn't receive a result
		return false, fmt.Errorf("function RabbitMQSend in pipeline '%s': No Data Received", ctx.PipelineId())
	}

	exportData, err := util.CoerceType(data)
	if err != nil {
		return false, err
	}
	if sender.connection == nil {
		err := sender.createRabbitMqConnect(ctx, exportData)
		if err != nil {
			return false, err
		}
	}

	if sender.connection.IsClosed() {
		sender.setRetryData(ctx, exportData)
		subMessage := "dropping event"
		if sender.persistOnError {
			subMessage = "persisting Event for later retry"
		}
		return false, fmt.Errorf("in pipeline '%s', connection to mqtt server for export not open, %s", ctx.PipelineId(), subMessage)
	}

	if sender.channel == nil {
		err := sender.openRabbitMqChannel(ctx, exportData)
		if err != nil {
			return false, err
		}
	}

	publishTopic, err := sender.topicFormatter.invoke(sender.mqttConfig.Topic, ctx, data)
	if err != nil {
		return false, fmt.Errorf("in pipeline '%s', RabbitMQ topic formatting failed: %s", ctx.PipelineId(), err.Error())
	}

	publishErr := sender.channel.Publish(sender.mqttConfig.ExchangeName, sender.mqttConfig.RoutingKey, sender.mqttConfig.Mandatory, sender.mqttConfig.Immediate, rabbitMq.Publishing{
		Body: exportData,
	})
	if publishErr != nil {
		sender.setRetryData(ctx, exportData)
		return false, publishErr
	}
	// capture the size for metrics
	exportDataBytes := len(exportData)
	if sender.mqttSizeMetrics == nil {
		var err error
		tag := fmt.Sprintf("%s:%s@%s:%d/%s", sender.mqttConfig.UserName, sender.mqttConfig.Password, sender.mqttConfig.Host, sender.mqttConfig.Port, publishTopic)
		metricName := fmt.Sprintf("%s-%s", internal.MqttExportSizeName, tag)
		ctx.LoggingClient().Debugf("Initializing metric %s.", metricName)
		sender.mqttSizeMetrics = gometrics.NewHistogram(gometrics.NewUniformSample(internal.MetricsReservoirSize))
		metricsManger := ctx.MetricsManager()
		if metricsManger != nil {
			err = metricsManger.Register(metricName, sender.mqttSizeMetrics, map[string]string{"address/topic": tag})
		} else {
			err = errors.New("metrics manager not available")
		}

		if err != nil {
			ctx.LoggingClient().Errorf("Unable to register metric %s. Collection will continue, but metric will not be reported: %s", internal.MqttExportSizeName, err.Error())
		}

	}
	sender.mqttSizeMetrics.Update(int64(exportDataBytes))
	ctx.LoggingClient().Debugf("Sent %d bytes of data to MQTT Broker in pipeline '%s'", exportDataBytes, ctx.PipelineId())
	ctx.LoggingClient().Tracef("Data exported", "Transport", "MQTT", "pipeline", ctx.PipelineId(), common.CorrelationHeader, ctx.CorrelationID())

	return true, nil
}

func (sender *RabbitMQSecretSender) setRetryData(ctx interfaces.AppFunctionContext, exportData []byte) {
	if sender.persistOnError {
		ctx.SetRetryData(exportData)
	}
}
