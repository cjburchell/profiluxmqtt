package mqtt

import (
	"github.com/cjburchell/profiluxmqtt/commands"
	data "github.com/cjburchell/profiluxmqtt/data/repo"
	appSettings "github.com/cjburchell/profiluxmqtt/settings"
	logger "github.com/cjburchell/uatu-go"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"strconv"
	"strings"
)

func RegisterCommands(commandRefresh chan string, mqttClient mqtt.Client, controllerRepo data.Controller, log logger.ILog, appConfig appSettings.Config) {
	mqttClient.SubscribeMultiple(map[string]byte{
		"profiluxmqtt/+/Maintenance/+/command": 1}, func(_ mqtt.Client, message mqtt.Message) {
		tokens := strings.Split(message.Topic(), "/")
		index, _ := strconv.Atoi(tokens[3])
		log.Printf("Maintenance: %d", index)
		commands.Maintenance(index, string(message.Payload()) == "ON", controllerRepo, log, appConfig.Connection)
		commandRefresh <- "Update"
	})

	mqttClient.SubscribeMultiple(map[string]byte{
		"profiluxmqtt/+/Reminders/+/reset": 1}, func(_ mqtt.Client, message mqtt.Message) {
		tokens := strings.Split(message.Topic(), "/")
		index, _ := strconv.Atoi(tokens[3])
		log.Printf("Reset Reminders: %d", index)
		commands.ResetReminder(index, controllerRepo, log, appConfig.Connection)
		commandRefresh <- "Update"
	})

	mqttClient.SubscribeMultiple(map[string]byte{
		"profiluxmqtt/+/Level/+/clearalarm": 1}, func(_ mqtt.Client, message mqtt.Message) {
		tokens := strings.Split(message.Topic(), "/")
		log.Printf("Clear Alarm: %s", tokens[3])
		commands.ClearLevelAlarm(tokens[3], controllerRepo, log, appConfig.Connection)
		commandRefresh <- "Update"
	})

	mqttClient.SubscribeMultiple(map[string]byte{
		"profiluxmqtt/+/Level/+/startwaterchange": 1}, func(_ mqtt.Client, message mqtt.Message) {
		tokens := strings.Split(message.Topic(), "/")
		log.Printf("Water Change: %s", tokens[3])
		commands.WaterChange(tokens[3], controllerRepo, log, appConfig.Connection)
		commandRefresh <- "Update"
	})

	mqttClient.SubscribeMultiple(map[string]byte{
		"profiluxmqtt/+/Controller/feedpause": 1}, func(_ mqtt.Client, message mqtt.Message) {
		log.Printf("Start Feed Pause")
		commands.FeedPause(true, controllerRepo, log, appConfig.Connection)
		commandRefresh <- "Update"
	})

	mqttClient.SubscribeMultiple(map[string]byte{
		"profiluxmqtt/+/Controller/ManualSockets/command": 1}, func(_ mqtt.Client, message mqtt.Message) {
		log.Printf("Manual Sockets")
		commands.ManualSockets(string(message.Payload()) == "ON", controllerRepo, log, appConfig.Connection)
		commandRefresh <- "Update"
	})

	mqttClient.SubscribeMultiple(map[string]byte{
		"profiluxmqtt/+/SPorts/+/command": 1}, func(_ mqtt.Client, message mqtt.Message) {
		tokens := strings.Split(message.Topic(), "/")
		log.Printf("SPort: %s", tokens[3])
		commands.ManualSockets(true, controllerRepo, log, appConfig.Connection)
		commands.SetSocketState(tokens[3], string(message.Payload()) == "ON", controllerRepo, log, appConfig.Connection)
		commandRefresh <- "Update"
	})
}
