package triggers

import (
	"context"
	"fmt"
	"github.com/edgexfoundry/go-mod-bootstrap/v3/bootstrap"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/go-jose/go-jose/v3/json"
	"github.com/google/uuid"
	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/interfaces"
	"sync"
	"time"
)

type TimerTriggerPayload struct {
	Message string
}

type TimerTrigger struct {
	TC            interfaces.TriggerConfig
	Interval      int
	ReceivedTopic string
}

// Initialize initializes the Trigger for logging
func (trigger *TimerTrigger) Initialize(_ *sync.WaitGroup, ctx context.Context, background <-chan interfaces.BackgroundMessage) (bootstrap.Deferred, error) {

	interval := trigger.Interval

	correlationId := uuid.New().String()

	message := "Timer Ticker"
	payload := TimerTriggerPayload{
		Message: message,
	}
	jsonData, jsonErr := json.Marshal(payload)
	if jsonErr != nil {
		return nil, fmt.Errorf("failed to Marshal TimerTriggerPayload %w", jsonErr)
	}
	envelope := types.MessageEnvelope{
		CorrelationID: correlationId,
		Payload:       jsonData,
		ReceivedTopic: trigger.ReceivedTopic,
		ContentType:   common.ContentTypeJSON,
	}
	// default 1 second
	if interval == 0 {
		interval = 1000
	}

	trigger.TC.Logger.Infof("Initializing Timer Trigger, interval: %d", interval)

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	go func() {
		defer ticker.Stop() // 确保定时器在goroutine退出时被停止
		for {
			select {
			case <-ticker.C:
				trigger.TC.MessageReceived(nil, envelope, trigger.responseHandler)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
	return nil, nil
}

func (trigger *TimerTrigger) responseHandler(ctx interfaces.AppFunctionContext, pipeline *interfaces.FunctionPipeline) error {
	return nil
}
