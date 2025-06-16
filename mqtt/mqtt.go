package mqtt

import (
	"encoding/json"
	"fmt"
	"github.com/cjburchell/profiluxmqtt/data/repo"
	"github.com/cjburchell/profiluxmqtt/profilux/types"
	logger "github.com/cjburchell/uatu-go"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"strings"
)

func sanitize(text string) string {

	newText := strings.Replace(text, " ", "_", -1)
	newText = strings.Replace(newText, "/", "_", -1)
	newText = strings.Replace(newText, ".", "_", -1)
	newText = strings.Replace(newText, "&", "_", -1)
	return newText
}

type ProfiluxMqtt struct {
	data map[string]string
}

func (profiMqtt *ProfiluxMqtt) PublishMQTTOld(mqttClient mqtt.Client, log logger.ILog, topic string) {
	fullTopic := fmt.Sprintf("profiluxmqtt/%s", topic)
	if profiMqtt.data == nil {
		return
	} else {
		_, ok := profiMqtt.data[fullTopic]
		if !ok {
			return
		}
	}

	t := mqttClient.Publish(fullTopic, 1, false, profiMqtt.data[fullTopic])
	// Handle the token in a go routine so this loop keeps sending messages regardless of delivery status
	go func() {
		_ = t.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
		if t.Error() != nil {
			log.Warnf("ERROR PUBLISHING profiluxmqtt/%s", fullTopic)
		}
	}()
}

func (profiMqtt *ProfiluxMqtt) PublishMQTT(mqttClient mqtt.Client, log logger.ILog, topic string, payload string, forceUpdate bool) {
	fullTopic := fmt.Sprintf("profiluxmqtt/%s", topic)
	if profiMqtt.data == nil {
		profiMqtt.data = make(map[string]string)
	} else {
		if profiMqtt.data[fullTopic] == payload && !forceUpdate {
			return
		}
	}
	profiMqtt.data[fullTopic] = payload

	t := mqttClient.Publish(fullTopic, 1, false, payload)
	// Handle the token in a go routine so this loop keeps sending messages regardless of delivery status
	go func() {
		_ = t.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
		if t.Error() != nil {
			log.Warnf("ERROR PUBLISHING profiluxmqtt/%s", fullTopic)
		}
	}()
}

func (profiMqtt *ProfiluxMqtt) UpdateMQTT(controllerRepo repo.Controller, mqttClient mqtt.Client, log logger.ILog, forceUpdate bool) {
	info, _ := controllerRepo.GetInfo()
	msg, _ := json.Marshal(info)
	controllerName := fmt.Sprintf("%s_%d%s", sanitize(string(info.Model)), info.DeviceAddress, suffix)
	profiMqtt.PublishMQTT(mqttClient, log, "status", "online", forceUpdate)
	profiMqtt.PublishMQTT(mqttClient, log, controllerName+"/Controller/data", string(msg), forceUpdate)
	profiMqtt.PublishMQTT(mqttClient, log, controllerName+"/Controller/alarm", string(info.Alarm), forceUpdate)
	profiMqtt.PublishMQTT(mqttClient, log, controllerName+"/Controller/mode", string(info.OperationMode), forceUpdate)
	profiMqtt.PublishMQTT(mqttClient, log, controllerName+"/Controller/ManualSockets/state", string(types.GetCurrentStateBool(info.OperationMode == types.OperationModeManualSockets)), forceUpdate)

	for _, p := range info.Maintenance {
		path := fmt.Sprintf("%s/Maintenance/%d", controllerName, p.Index)
		data, _ := json.Marshal(p)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/data", string(data), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/state", strings.ToUpper(string(p.IsActive)), forceUpdate)
	}

	for _, p := range info.Reminders {
		path := fmt.Sprintf("%s/Reminders/%d", controllerName, p.Index)
		data, _ := json.Marshal(p)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/data", string(data), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/state", string(p.IsOverdue), forceUpdate)
	}

	probes, _ := controllerRepo.GetProbes()
	for _, p := range probes {
		path := fmt.Sprintf("%s/Probes/%s", controllerName, p.ID)
		if p.SensorType == types.SensorTypeAirTemperature {
			if p.Value > 35 || p.Value < -30 {
				if forceUpdate {
					profiMqtt.PublishMQTTOld(mqttClient, log, path+"/data")
					profiMqtt.PublishMQTTOld(mqttClient, log, path+"/state")
					profiMqtt.PublishMQTTOld(mqttClient, log, path+"/convertedvalue")
				}
				continue
			}
		}
		data, _ := json.Marshal(p)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/data", string(data), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/setpoint", fmt.Sprintf("%.2f", p.CenterValue), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/min", fmt.Sprintf("%.2f", p.MinRange), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/max", fmt.Sprintf("%.2f", p.MaxRange), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/state", fmt.Sprintf("%.2f", p.Value), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/convertedvalue", fmt.Sprintf("%.2f", p.ConvertedValue), forceUpdate)
	}

	levelSensors, _ := controllerRepo.GetLevelSensors()
	for _, p := range levelSensors {
		path := fmt.Sprintf("%s/Level/%s", controllerName, p.ID)
		data, _ := json.Marshal(p)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/data", string(data), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/state", string(p.Value), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/alarm", string(p.AlarmState), forceUpdate)
		if p.HasTwoInputs {
			profiMqtt.PublishMQTT(mqttClient, log, path+"/state2", string(p.SecondSensor), forceUpdate)
		}

		if p.HasWaterChange {
			profiMqtt.PublishMQTT(mqttClient, log, path+"/waterchange", string(p.WaterMode), forceUpdate)
		}
	}

	sockets, _ := controllerRepo.GetSPorts()
	for _, p := range sockets {
		path := fmt.Sprintf("%s/SPorts/%s", controllerName, p.ID)
		data, _ := json.Marshal(p)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/data", string(data), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/state", string(p.Value), forceUpdate)
	}

	lightPorts, _ := controllerRepo.GetLPorts()
	for _, p := range lightPorts {
		path := fmt.Sprintf("%s/LPorts/%s", controllerName, p.ID)
		data, _ := json.Marshal(p)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/data", string(data), forceUpdate)
		profiMqtt.PublishMQTT(mqttClient, log, path+"/state", fmt.Sprintf("%.2f", p.Value), forceUpdate)
	}
}
