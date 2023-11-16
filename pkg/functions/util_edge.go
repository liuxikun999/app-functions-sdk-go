package functions

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/interfaces"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func NewUtilEdge(serviceUrls string, serviceNames string, deviceName string, productKey string) UtilEdge {

	return UtilEdge{
		serviceUrls:  serviceUrls,
		serviceNames: serviceNames,
		deviceName:   deviceName,
		productKey:   productKey,
	}
}

type UtilEdge struct {
	serviceUrls  string
	serviceNames string
	deviceName   string
	productKey   string
}

type CollectGatewayEvent struct {
	Payload interface{}
}

type GatewayInfo struct {
	DeviceName string     `json:"deviceName"`
	ProductKey string     `json:"productKey"`
	Status     []ItemInfo `json:"status"`
	Payload    interface{}
}

type ItemInfo struct {
	Code  string      `json:"code"`
	Value interface{} `json:"value"`
	Time  int64       `json:"time"`
}

type PingResponse struct {
	ApiVersion  string `json:"apiVersion"`
	Timestamp   string `json:"timestamp"`
	ServiceName string `json:"serviceName"`
}

func (s *UtilEdge) CollectGatewayInfo(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
	lc := ctx.LoggingClient()

	var statusArray []ItemInfo
	var event *CollectGatewayEvent
	if data != nil {
		e, ok := data.(*CollectGatewayEvent)
		if !ok {
			return false, fmt.Errorf("function LogEventDetails in pipeline '%s', type received is not an Event", ctx.PipelineId())
		}
		event = e
	}
	fmt.Println("event:", event)

	// cpu使用率
	cpuUsedPercent, _ := cpu.Percent(time.Second, false)
	fmt.Println(cpuUsedPercent)
	cpuUsedPercentItem := &ItemInfo{
		Code:  "cpu_rate",
		Value: strconv.FormatFloat(cpuUsedPercent[0], 'f', 2, 64),
		Time:  time.Now().UnixMilli(),
	}
	//获取物理内存信息
	memoryInfo, _ := mem.VirtualMemory()
	fmt.Println("memoryInfo:", memoryInfo)
	// 内存使用率
	memoryUsedPercentItem := &ItemInfo{
		Code:  "memory_rate",
		Value: strconv.FormatFloat(100*(1-float64(memoryInfo.Available)/float64(memoryInfo.Total)), 'f', 2, 64),
		Time:  time.Now().UnixMilli(),
	}
	// 内存总大小
	memoryTotalItem := &ItemInfo{
		Code:  "memory_total",
		Value: memoryInfo.Total,
		Time:  time.Now().UnixMilli(),
	}
	// 内存总大小
	memoryAvailableItem := &ItemInfo{
		Code:  "memory_available",
		Value: memoryInfo.Available,
		Time:  time.Now().UnixMilli(),
	}

	// 硬盘使用率
	//mountpoint := "/"
	//可以通过psutil获取磁盘分区、磁盘使用率和磁盘IO信息
	diskPartitions, _ := disk.Partitions(false) //所有分区
	fmt.Println("d1:", diskPartitions)
	diskUsage, _ := disk.Usage(diskPartitions[0].Mountpoint) //指定某路径的硬盘使用情况
	//fmt.Println("硬盘路径：", diskUsage.Path, "总大小：", diskUsage.Total, "已使用：", diskUsage.Used, "使用率：", diskUsage.UsedPercent)
	fmt.Printf("硬盘路径：%s 总大小：%d 已使用：%d  使用率：%f \n", diskUsage.Path, diskUsage.Total, diskUsage.Used, diskUsage.UsedPercent)
	// 硬盘使用率
	diskUsedPercentItem := &ItemInfo{
		Code:  "disk_rate",
		Value: strconv.FormatFloat(diskUsage.UsedPercent, 'f', 2, 64),
		Time:  time.Now().UnixMilli(),
	}
	// 硬盘总大小
	diskTotalPercentItem := &ItemInfo{
		Code:  "disk_total",
		Value: diskUsage.Total,
		Time:  time.Now().UnixMilli(),
	}
	statusArray = append(statusArray, *cpuUsedPercentItem, *memoryTotalItem, *memoryAvailableItem, *memoryUsedPercentItem, *diskUsedPercentItem, *diskTotalPercentItem)

	serviceUrls := strings.Split(s.serviceUrls, ",")
	serviceNames := strings.Split(s.serviceNames, ",")
	for _, serviceUrl := range serviceUrls {
		pingResponse, err := s.doGet(serviceUrl)
		if err != nil {
			lc.Infof("%s ", err.Error())
			continue
		}
		// 监控服务状态上报
		serviceStatusItem := &ItemInfo{
			Code:  pingResponse.ServiceName,
			Value: "ok",
			Time:  time.Now().UnixMilli(),
		}
		statusArray = append(statusArray, *serviceStatusItem)
		serviceNames = removeElementInPlace(serviceNames, pingResponse.ServiceName)
	}

	for _, serviceName := range serviceNames {
		// 服务未上线上报
		serviceStatusItem := &ItemInfo{
			Code:  serviceName,
			Value: "failed",
			Time:  time.Now().UnixMilli(),
		}
		statusArray = append(statusArray, *serviceStatusItem)
	}

	gatewayInfo := &GatewayInfo{
		DeviceName: s.deviceName,
		ProductKey: s.productKey,
		Status:     statusArray,
		Payload:    event.Payload,
	}
	return true, gatewayInfo
}

func (s *UtilEdge) doGet(url string) (PingResponse, error) {
	var res PingResponse
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		fmt.Errorf("ping service failed : %s", err.Error())
	}
	req.Header.Set("Content-Type", common.ContentTypeJSON)
	response, err := client.Do(req)
	if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		if err == nil {
			err = fmt.Errorf("ping service failed with %d HTTP status code", response.StatusCode)
		} else {
			err = fmt.Errorf("ping service failed : %s", err.Error())
		}
		return res, errors.New("ping service request failed")
	}
	defer func() { _ = response.Body.Close() }()
	responseData, errReadingBody := io.ReadAll(response.Body)
	if errReadingBody != nil {
		// do something
		return res, errReadingBody
	}
	err = json.Unmarshal(responseData, &res)
	if err != nil {
		// do something
		return res, err
	}
	return res, nil
}

func removeElementInPlace(slice []string, element string) []string {
	for i := 0; i < len(slice); i++ {
		if slice[i] == element {
			// 将要移除的元素和切片最后一个元素交换位置
			slice[i] = slice[len(slice)-1]
			// 切片缩短一个元素
			slice = slice[:len(slice)-1]
			// 由于当前位置的元素已被交换，需要重新检查当前位置
			i--
		}
	}
	return slice
}
