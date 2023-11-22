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
