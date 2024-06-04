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
	"database/sql"
	_ "database/sql"
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	_ "github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	_ "github.com/edgexfoundry/go-mod-core-contracts/v3/dtos"
	_ "github.com/go-sql-driver/mysql"
	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/interfaces"
	gometrics "github.com/rcrowley/go-metrics"
	"strings"
	"sync"
	"time"
)

// MysqlSecretClient ...
type MysqlSecretClient struct {
	lock                 sync.Mutex
	client               *sql.DB
	config               MysqlSecretConfig
	persistOnError       bool
	secretsLastRetrieved time.Time
	topicFormatter       StringValuesFormatter
	mqttSizeMetrics      gometrics.Histogram
}

// MysqlSecretConfig ...
type MysqlSecretConfig struct {
	Enable       bool
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
	// QoS for MQTT Connection
	QoS byte
	// Retain setting for MQTT Connection
	Retain bool
	// AuthMode indicates what to use when connecting to the broker. Options are "none", "cacert" , "usernamepassword", "clientcert".
	// If a CA Cert exists in the SecretName then it will be used for all modes except "none".
	AuthMode string
}

type ExecuteSQLEvent struct {
	events []ExecuteSQLParams
}

type ExecuteSQLParams struct {
	sql    string
	params []interface{}
}

// NewMysqlSecretClient ...
func NewMysqlSecretClient(mysqlConfig MysqlSecretConfig) *MysqlSecretClient {
	//avoid casing issues
	mysqlConfig.AuthMode = strings.ToLower(mysqlConfig.AuthMode)
	sender := &MysqlSecretClient{
		client: nil,
		config: mysqlConfig,
	}
	return sender
}

// InitializeMysqlSecretClient 初始化客户端连接
func (sender *MysqlSecretClient) InitializeMysqlSecretClient() error {
	if sender.client != nil {
		return nil
	}
	err := sender.createMysqlClient()
	if err != nil {
		return err
	}
	return nil
}

// 建立连接
func (sender *MysqlSecretClient) createMysqlClient() error {
	sender.lock.Lock()
	defer sender.lock.Unlock()
	if sender.client != nil {
		return nil
	}
	uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", sender.config.UserName, sender.config.Password, sender.config.Host, sender.config.Port, sender.config.DatabaseName)
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return fmt.Errorf("could not connect to Mysql server for export. Error: %s", err.Error())
	}
	sender.client = db
	fmt.Println("Connected to Mysql server")
	return nil
}

// DisconnectMysqlSecretClient ...
func (sender *MysqlSecretClient) DisconnectMysqlSecretClient() error {
	if sender.client == nil {
		return fmt.Errorf("mysql数据库连接为空")
	}
	if err := sender.client.Close(); err != nil {
		return fmt.Errorf("mysql数据库连接断开失败")
	}
	sender.client = nil
	return nil
}

// executeSql 执行sql
func (sender *MysqlSecretClient) executeSql(sql string, params []interface{}) (bool, error) {
	stmt, err := sender.client.Prepare(sql)
	defer stmt.Close()
	if err != nil {
		return false, fmt.Errorf("prepare sql failed, Error: %s", err.Error())
	}
	_, err = stmt.Exec(params...)
	if err != nil {
		return false, fmt.Errorf("执行sql失败, Error: %s", err.Error())
	}
	return true, nil
}

// EventExportToMysql 备份事件到mysql
func (sender *MysqlSecretClient) EventExportToMysql(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
	if data == nil {
		// We didn't receive a result
		return false, fmt.Errorf("function EventExportToMysql in pipeline '%s': No Data Received", ctx.PipelineId())
	}
	event, ok := data.(ExecuteSQLEvent)
	if !ok {
		return false, fmt.Errorf("function ConvertEventToCloud in pipeline '%s', type received is not an Event", ctx.PipelineId())
	}
	if sender.client == nil {
		err := sender.createMysqlClient()
		if err != nil {
			return false, err
		}
	}
	for _, eventItem := range event.events {
		result, err := sender.executeSql(eventItem.sql, eventItem.params)
		if err != nil {
			return false, err
		} else if !result {
			return false, fmt.Errorf
		}
	}
	ctx.LoggingClient().Tracef("event inserted", "Transport", "Mysql", "pipeline", ctx.PipelineId(), common.CorrelationHeader, ctx.CorrelationID())
	return true, nil
}
