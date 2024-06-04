package transforms

import (
	"fmt"
	"testing"
)

func TestMysql_Connect(t *testing.T) {
	mysqlConfig := MysqlSecretConfig{
		Enable:       true,
		UserName:     "root",
		Password:     "yisquare2022",
		DatabaseName: "eladmin",
		Host:         "192.168.0.178",
		Port:         13306,
		ClientId:     "test",
		SecretName:   "mysql",
	}

	sender := NewMysqlSecretClient(mysqlConfig)

	sender.createMysqlClient()

	stmt, error := sender.client.Prepare("UPDATE sys_log set description=? where log_id=?")
	if error != nil {
		fmt.Println(error)
	}
	defer stmt.Close()
	defer sender.DisconnectMysqlSecretClient()

	_, err := stmt.Exec("设备数据同步2", 3537)
	if err != nil {
		fmt.Println(err)
	}

	rows, err := sender.client.Query("SELECT log_id,description,log_type FROM sys_log where log_id=?", 3537)
	defer rows.Close()
	for rows.Next() {
		var logId uint32
		var description string
		var logType string
		err = rows.Scan(&logId, &description, &logType)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(logId, description, logType)
	}
}
