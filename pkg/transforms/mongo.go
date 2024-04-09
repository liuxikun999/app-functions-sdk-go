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
	"context"
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/dtos"
	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/interfaces"
	gometrics "github.com/rcrowley/go-metrics"
	mongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"sync"
	"time"
)

// MongoSecretClient ...
type MongoSecretClient struct {
	lock                 sync.Mutex
	client               *mongo.Client
	DB                   *mongo.Database
	mongoConfig          MongoSecretConfig
	persistOnError       bool
	secretsLastRetrieved time.Time
	topicFormatter       StringValuesFormatter
	mqttSizeMetrics      gometrics.Histogram
}

// MongoSecretConfig ...
type MongoSecretConfig struct {
	UserName     string
	Password     string
	DatabaseName string
	Host         string
	Port         int
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

// NewMongoSecretClient ...
func NewMongoSecretClient(mongoConfig MongoSecretConfig) *MongoSecretClient {
	//avoid casing issues
	mongoConfig.AuthMode = strings.ToLower(mongoConfig.AuthMode)
	sender := &MongoSecretClient{
		client:      nil,
		mongoConfig: mongoConfig,
	}
	return sender
}

// 建立连接
func (sender *MongoSecretClient) createMongoClient(ctx interfaces.AppFunctionContext) error {
	sender.lock.Lock()
	defer sender.lock.Unlock()
	uri := "mongodb://" + sender.mongoConfig.UserName + ":" + sender.mongoConfig.Password + "@" + sender.mongoConfig.Host + ":" + string(sender.mongoConfig.Port)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("in pipeline '%s', could not connect to Mongo server for export. Error: %s", ctx.PipelineId(), err.Error())
	}
	sender.client = client
	sender.DB = client.Database(sender.mongoConfig.DatabaseName)
	ctx.LoggingClient().Infof("Connected to Mongo server for export in pipeline '%s'", ctx.PipelineId())
	return nil
}

// DisconnectMongoSecretClient ...
func (sender *MongoSecretClient) DisconnectMongoSecretClient(mongoConfig MongoSecretConfig) error {
	if sender.client == nil {
		return fmt.Errorf("mongo数据库连接为空")
	}
	if err := sender.client.Disconnect(context.TODO()); err != nil {
		return fmt.Errorf("mongo数据库连接断开失败")
	}
	return nil
}

// EventBackup 备份事件
func (sender *MongoSecretClient) EventBackup(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
	if data == nil {
		// We didn't receive a result
		return false, fmt.Errorf("function EventBackup in pipeline '%s': No Data Received", ctx.PipelineId())
	}
	event, ok := data.(dtos.Event)
	if !ok {
		return false, fmt.Errorf("function ConvertEventToCloud in pipeline '%s', type received is not an Event", ctx.PipelineId())
	}
	if sender.DB == nil {
		err := sender.createMongoClient(ctx)
		if err != nil {
			return false, err
		}
	}
	coll := sender.DB.Collection("event")
	_, err := coll.InsertOne(context.TODO(), event)
	if err != nil {
		return false, fmt.Errorf("插入数据失败, pipeline '%s', Error: %s", ctx.PipelineId(), err.Error())
	}
	ctx.LoggingClient().Tracef("event inserted", "Transport", "Mongo", "pipeline", ctx.PipelineId(), common.CorrelationHeader, ctx.CorrelationID())
	return true, nil
}
