// Package mqtt provides the log driver for forwarding server logs to mqtt broker
package mqtt

import (
	"bytes"
	"fmt"
	mqtt "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/logger"
	"github.com/docker/docker/pkg/jsonlog"
	"github.com/docker/docker/pkg/timeutils"
	"math/rand"
	"net"
	"strconv"
	"time"
)

const (
	Name                   = "mqtt"
	defaultPort            = "1883"
	defaultQos             = 2
	defaultRetained        = false
	defaultTopicPrefix     = "docker-log"
	defaultClientIdPrefix  = "docker-logger-"
	defaultKeepAliveSecond = 60
	defaultWaitTimeout     = 3 * time.Second
)
const hexLetters = "0123456789abcdef"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = hexLetters[rand.Intn(len(hexLetters))]
	}
	return string(b)
}

type mqttLogger struct {
	c           *mqtt.Client
	buf         *bytes.Buffer
	topicPrefix string
	qos         int
	retained    bool
}

func init() {
	if err := logger.RegisterLogDriver(Name, New); err != nil {
		logrus.Fatal(err)
	}
	if err := logger.RegisterLogOptValidator(Name, ValidateLogOpt); err != nil {
		logrus.Fatal(err)
	}
}
func New(ctx logger.Context) (logger.Logger, error) {

	addr, topicPrefix, qos, retained, err := parseConfig(ctx)
	if err != nil {
		return nil, err
	}

	opts := mqtt.NewClientOptions().AddBroker(addr)
	opts.SetClientID(defaultClientIdPrefix + randStringBytes(16))
	opts.SetKeepAlive(defaultKeepAliveSecond * time.Second)

	c := mqtt.NewClient(opts)
	token := c.Connect()
	// return true if not timeout, return false if timeout
	ok := token.WaitTimeout(defaultWaitTimeout)
	if !ok {
		err = token.Error()
		if err != nil {
			return nil, err
		} else {
			return nil, fmt.Errorf("Mqtt connection timeout")
		}
	}

	return &mqttLogger{
		c:           c,
		buf:         bytes.NewBuffer(nil),
		topicPrefix: topicPrefix,
		qos:         qos,
		retained:    retained,
	}, nil
}
func (m *mqttLogger) Name() string {
	return "mqtt"
}
func (m *mqttLogger) Log(msg *logger.Message) error {

	timestamp, err := timeutils.FastMarshalJSON(msg.Timestamp)
	if err != nil {
		return err
	}
	err = (&jsonlog.JSONLogs{Log: append(msg.Line, '\n'), Stream: msg.Source, Created: timestamp}).MarshalJSONBuf(m.buf)
	if err != nil {
		return err
	}

	topic := fmt.Sprintf("%s/%s", m.topicPrefix, msg.ContainerID)
	token := m.c.Publish(topic, byte(m.qos), m.retained, m.buf.Bytes())
	return token.Error()
}
func (m *mqttLogger) Close() error {
	m.c.Disconnect(250)
	return nil
}

func parseConfig(ctx logger.Context) (string, string, int, bool, error) {

	config := ctx.Config

	host, ok := config["mqtt-host"]
	if !ok {
		return "", "", 0, false, fmt.Errorf("missing mqtt host")
	}

	port, ok := config["mqtt-port"]
	if !ok {
		port = defaultPort
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return "", "", 0, false, fmt.Errorf("incorrect host or port")
	}
	addr := fmt.Sprintf("tcp://%s", tcpAddr.String())

	topicPrefix, ok := config["mqtt-topic-prefix"]
	if !ok {
		topicPrefix = defaultTopicPrefix
	}

	qos := defaultQos
	if qosString, ok := config["mqtt-qos"]; ok {
		//FIXME find a better way to write this
		qosInt64, err := strconv.ParseInt(qosString, 10, 64)
		if err != nil {
			return "", "", 0, false, fmt.Errorf("missing mqtt host")
		}
		qos = int(qosInt64)
	}

	retained := defaultRetained // false here
	if _, ok := config["mqtt-retained"]; ok {
		retained = true
	}

	return addr, topicPrefix, qos, retained, nil
}

func ValidateLogOpt(cfg map[string]string) error {
	for key, value := range cfg {
		switch key {
		case "mqtt-host":
		case "mqtt-port":
		case "mqtt-topic-prefix":
		case "mqtt-qos":
			if value != "0" && value != "1" && value != "2" {
				return fmt.Errorf("incorrect qos level '%s' for mqtt log driver", value)
			}
		case "mqtt-retained":
		default:
			return fmt.Errorf("unknown log opt '%s' for mqtt log driver", key)
		}
	}
	return nil
}
