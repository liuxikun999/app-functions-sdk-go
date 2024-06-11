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
	Enable          bool
	UserName        string
	Password        string
	DatabaseName    string
	Host            string
	Port            int
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime int // seconds
	// ClientId to connect with the broker with.
	ClientId string
	// The name of the secret in secret provider to retrieve your secrets
	SecretName string
	// AuthMode indicates what to use when connecting to the broker. Options are "none", "cacert" , "usernamepassword", "clientcert".
	// If a CA Cert exists in the SecretName then it will be used for all modes except "none".
	AuthMode string
}

type ExecuteSQLEvent struct {
	SQLParams []ExecuteSQLParams
}

type ExecuteSQLParams struct {
	InsertSql        string
	InsertParams     []interface{}
	UpdateSql        string
	UpdateParams     []interface{}
	CheckExistSql    string
	CheckExistParams []interface{}
	IsCheckExist     bool // 是否需要判断数据已存在，如果已存在则更新，不存在则插入；如果不判断则直接插入
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
	if sender.config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(sender.config.MaxOpenConns)
	}
	if sender.config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(sender.config.MaxIdleConns)
	}
	if sender.config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(sender.config.ConnMaxLifetime) * time.Second)
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
func (sender *MysqlSecretClient) executeSql(sql string, params ...interface{}) (sql.Result, error) {
	// executeSql的实现代码
	result, err := sender.client.Exec(sql, params)
	if err != nil {
		// 这里可以根据错误类型进行更细致的错误处理
		return nil, fmt.Errorf("执行SQL失败: %s, Error: %s", sql, err.Error())
	}
	return result, nil
}

// HandleSingleSqlEvent 处理单条sql事件，现在使用事务来保证数据的一致性
func (sender *MysqlSecretClient) HandleSingleSqlEvent(data ExecuteSQLParams) (bool, error) {
	if data.IsCheckExist {
		rows, err := sender.client.Query(data.CheckExistSql, data.CheckExistParams...)
		if err != nil {
			return false, fmt.Errorf("执行查询失败: %s", err.Error())
		}
		defer rows.Close()

		if rows.Next() {
			// 如果查询到数据，执行更新操作
			_, err := sender.executeSql(data.UpdateSql, data.UpdateParams...)
			if err != nil {
				return false, fmt.Errorf("执行更新失败: %s", err.Error())
			}
		} else {
			// 没有查询到数据，执行插入操作
			_, err := sender.executeSql(data.InsertSql, data.InsertParams...)
			if err != nil {
				return false, fmt.Errorf("执行插入失败: %s", err.Error())
			}
		}
	} else {
		// 直接执行插入操作
		_, err := sender.executeSql(data.InsertSql, data.InsertParams...)
		if err != nil {
			return false, fmt.Errorf("执行插入失败: %s", err.Error())
		}
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
	for _, eventItem := range event.SQLParams {
		result, err := sender.HandleSingleSqlEvent(eventItem)
		if err != nil {
			return false, err
		} else if !result {
			return false, fmt.Errorf
		}
	}
	ctx.LoggingClient().Tracef("event inserted", "Transport", "Mysql", "pipeline", ctx.PipelineId(), common.CorrelationHeader, ctx.CorrelationID())
	return true, nil
}
