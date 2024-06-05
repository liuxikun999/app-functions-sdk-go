package transforms

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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

	currentTimeMillis := time.Now().UnixNano() / int64(time.Millisecond)
	// 将当前时间格式化成 年月日时分秒的格式
	currentTime := time.Unix(currentTimeMillis/1000, 0).Format("2006-01-02 15:04:05")
	DeviceName := "Test_Device_Name"
	ResourceName := "Test_Resource_Name"
	srcNo := "20240605"
	Value := 101
	sqlParams := ExecuteSQLParams{
		InsertSql:        "INSERT INTO `t_edge_device_attr_extension` (`dev_key`, `attr_key`, `src_no`, `value`, `value_time`, `change_time`, `modify_time`) VALUES (?, ?, ?, ?, ?, ?, ?)",
		InsertParams:     []interface{}{DeviceName, ResourceName, srcNo, Value, currentTime, currentTime, currentTime},
		UpdateSql:        "UPDATE `t_edge_device_attr_extension` SET `value`=?,`value_time`=? WHERE dev_key=? and attr_key=? and src_no=?",
		UpdateParams:     []interface{}{Value, currentTime, DeviceName, ResourceName, srcNo},
		CheckExistSql:    "select * from `t_edge_device_attr_extension` where dev_key=? and attr_key=? and src_no=?",
		CheckExistParams: []interface{}{DeviceName, ResourceName, srcNo},
		IsCheckExist:     true,
	}
	result, err := sender.HandleSingleSqlEvent(sqlParams)
	if err != nil {
		fmt.Println(err)
	}
	assert.Equal(t, nil, err)
	assert.Equal(t, true, result)

	_, err = stmt.Exec("设备数据同步2", 3537)
	if err != nil {
		fmt.Println(err)
	}

	rows, err := sender.client.Query("SELECT dev_key,attr_key,src_no FROM t_edge_device_attr_extension where src_no=?", "20240605")
	defer rows.Close()
	for rows.Next() {
		var rowDevKey string
		var rowAttrKey string
		var rowSrcNo string
		err = rows.Scan(&rowDevKey, &rowAttrKey, &rowSrcNo)
		if err != nil {
			fmt.Println(err)
		}
		assert.Equal(t, nil, err)
		assert.Equal(t, DeviceName, rowDevKey)
		assert.Equal(t, ResourceName, rowAttrKey)
		assert.Equal(t, srcNo, rowSrcNo)
	}

}
