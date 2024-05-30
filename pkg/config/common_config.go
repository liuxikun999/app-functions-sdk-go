package config

type MonitorInfo struct {
	ServiceUrls  string
	ServiceNames string
}

type RabbitMQInfo struct {
	UserName       string
	Password       string
	Host           string
	Port           int
	Topic          string
	AuthMode       string
	PersistOnError bool
	ExchangeName   string
	ExchangeType   string
	RoutingKey     string
	ClientId       string
	Mandatory      bool
	Immediate      bool
}

type ThingsBoardMessageEnvelope struct {
	Device        string                 `json:"device"`
	Data          map[string]interface{} `json:"data"`
	ReceivedTopic string                 `json:"receivedTopic"`
}

type ThingsBoardTopicChannel struct {
	// Topic for subscriber to filter on if any
	Topic string
	// Messages is the returned message channel for the subscriber
	Messages chan ThingsBoardMessageEnvelope
}
