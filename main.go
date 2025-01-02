package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cjburchell/profiluxmqtt/update"

	appSettings "github.com/cjburchell/profiluxmqtt/settings"
	"github.com/cjburchell/tools-go/env"

	"github.com/cjburchell/profiluxmqtt/data/repo"

	"github.com/cjburchell/settings-go"
	logger "github.com/cjburchell/uatu-go"
	mqtt "github.com/eclipse/paho.mqtt.golang"

	profiluxmqtt "github.com/cjburchell/profiluxmqtt/mqtt"
)

func main() {
	set := settings.Get(env.Get("ConfigFile", ""))
	log := logger.Create(logger.Settings{
		MinLogLevel:  logger.GetLogLevel(set.Get("MinLogLevel", logger.INFO.Text)),
		ServiceName:  "Profilux MQTT",
		LogToConsole: true,
		UseHTTP:      false,
		UsePubSub:    false,
	})
	log.Printf("Starting Up!")
	appConfig := appSettings.Get(set)

	controllerRepo := repo.NewController()

	mqttOptions := mqtt.NewClientOptions().AddBroker(fmt.Sprintf("mqtt://%s:%d", appConfig.MqttHost, appConfig.MqttPort)).SetClientID(appConfig.ClientID)

	mqttOptions.Username = appConfig.MqttUser
	mqttOptions.Password = appConfig.MqttPassword
	mqttOptions.SetOrderMatters(false)       // Allow out of order messages (use this option unless in order delivery is essential)
	mqttOptions.ConnectTimeout = time.Second // Minimal delays on connect
	mqttOptions.WriteTimeout = time.Second   // Minimal delays on writes
	mqttOptions.KeepAlive = 10               // Keepalive every 10 seconds so we quickly detect network outages
	mqttOptions.PingTimeout = time.Second    // local broker so response should be quick
	mqttOptions.ConnectRetry = true
	mqttOptions.AutoReconnect = true

	mqttOptions.OnConnectionLost = func(cl mqtt.Client, err error) {
		log.Warn("connection lost")
	}
	mqttOptions.OnConnect = func(mqtt.Client) {
		log.Print("connection established")
	}
	mqttOptions.OnReconnecting = func(mqtt.Client, *mqtt.ClientOptions) {
		log.Print("attempting to reconnect")
	}

	mqttClient := mqtt.NewClient(mqttOptions)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	log.Print("Connection is up")

	defer mqttClient.Disconnect(100)

	var profiluxMqtt profiluxmqtt.ProfiluxMqtt

	profiluxmqtt.RegisterCommands(&profiluxMqtt, mqttClient, controllerRepo, log, appConfig)

	log.Debugf("Getting Data from Controller")
	for {
		var err = update.All(controllerRepo, log, appConfig.Connection)
		if err == nil {
			break
		}

		log.Error(err, "Unable to do first update")
		log.Printf("RefreshSettings Sleeping for %s", logRate.String())
		<-time.After(logRate)
		continue
	}

	profiluxMqtt.UpdateMQTT(controllerRepo, mqttClient, log, false)
	profiluxMqtt.UpdateHomeAssistant(controllerRepo, mqttClient, log, false)

	RunUpdate(controllerRepo, mqttClient, log, appConfig, &profiluxMqtt)
}

const logRate = time.Second * 1
const logAllRate = time.Minute * 1

func RunUpdate(controller repo.Controller, mqttClient mqtt.Client, log logger.ILog, config appSettings.Config, profiluxMqtt *profiluxmqtt.ProfiluxMqtt) {
	c := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	for {
		select {
		case <-c:
			profiluxMqtt.PublishMQTT(mqttClient, log, "status", "offline", true)
			log.Debug("Exit Application")
			return
		case <-time.After(logAllRate):
			log.Debug("Updating Config")
			var err = update.All(controller, log, config.Connection)
			if err != nil {
				log.Errorf(err, "Unable to update")
			} else {
				profiluxMqtt.UpdateMQTT(controller, mqttClient, log, true)
				profiluxMqtt.UpdateHomeAssistant(controller, mqttClient, log, true)
			}
		case <-time.After(logRate):
			log.Debug("Updating State")
			var err = update.State(controller, log, config.Connection)
			if err != nil {
				log.Errorf(err, "Unable to update state")
			} else {
				profiluxMqtt.UpdateMQTT(controller, mqttClient, log, false)
			}
		}
	}
}
