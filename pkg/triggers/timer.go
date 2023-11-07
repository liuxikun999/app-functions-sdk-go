package triggers

import (
	"context"
	"errors"
	"github.com/edgexfoundry/go-mod-bootstrap/v3/bootstrap"
	"github.com/edgexfoundry/go-mod-bootstrap/v3/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v3/di"
	"github.com/edgexfoundry/go-mod-messaging/v3/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/google/uuid"
	"github.com/liuxikun999/app-functions-sdk-go/v3/internal/appfunction"
	"github.com/liuxikun999/app-functions-sdk-go/v3/internal/trigger"
	"github.com/liuxikun999/app-functions-sdk-go/v3/pkg/interfaces"
	"strings"
	"sync"
	"time"
)

// Trigger implements Trigger to support Triggers
type TimerTrigger struct {
	client            messaging.MessageClient
	timerPublishTopic string
	dic               *di.Container
	serviceBinding    trigger.ServiceBinding
	messageProcessor  trigger.MessageProcessor
}

// Initialize initializes the Trigger for logging
func (trigger *TimerTrigger) Initialize(_ *sync.WaitGroup, ctx context.Context, background <-chan interfaces.BackgroundMessage) (bootstrap.Deferred, error) {
	lc := trigger.serviceBinding.LoggingClient()
	interval := trigger.serviceBinding.Config().Trigger.Interval

	correlationId := uuid.New().String()

	envelope := types.MessageEnvelope{
		CorrelationID: correlationId,
		Payload:       nil,
	}
	appContext := trigger.serviceBinding.BuildContext(envelope)

	// default 1 second
	if interval == 0 {
		interval = 1000
	}

	// 判断是否异步执行
	async := trigger.serviceBinding.Config().Trigger.Async
	if async {
		trigger.client = container.MessagingClientFrom(trigger.dic.Get)
		if trigger.client == nil {
			return nil, errors.New("timer trigger unable to find MessageBus Client. Make sure it is configured properly")
		}
		trigger.timerPublishTopic = strings.TrimSpace(trigger.serviceBinding.Config().Trigger.TimerPublishTopic)
		if len(trigger.timerPublishTopic) <= 0 {
			lc.Infof("TimerPublishTopic topic not set for Timer Trigger. ")
			return nil, errors.New("TimerPublishTopic topic not set for Timer Trigger. ")
		}
	}

	lc.Info("Initializing Timer Trigger, interval:%d", interval)

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defaultPipeline := trigger.serviceBinding.GetDefaultPipeline()

	go func() {
		defer ticker.Stop() // 确保定时器在goroutine退出时被停止
		for {
			select {
			case <-ticker.C:
				if async {
					err := trigger.client.Publish(envelope, trigger.timerPublishTopic)
					if err != nil {
						lc.Errorf("timer trigger Failed to publish background Message to bus, %v", err)
						return
					}
				} else {
					messageError := trigger.serviceBinding.ProcessMessage(appContext.(*appfunction.Context), envelope, defaultPipeline)
					if messageError != nil {
						lc.Errorf("timer trigger Failed to process Message, %v", messageError)
						return
					}
				}

			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
	return nil, nil
}
