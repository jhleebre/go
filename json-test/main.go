package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// Config asdasd
type Config struct {
	// ConsumerGroup asdasd
	ConsumerGroup struct {
		Brokers  []string `json:"brokers"`
		Topic    string   `json:"topic"`
		Group    string   `json:"group"`
		Assignor string   `json:"assignor"`
		Version  string   `json:"version"`
		Oldest   bool     `json:"oldest"`
	} `json:"consumer_data"`
	// Producer asdads
	Producer struct {
		Brokers []string `json:"brokers"`
		Topic   string   `json:"topic"`
	} `json:"producer_data"`
}

func main() {
	jsonFile, err := os.Open("config.json")
	if err != nil {
		fmt.Println(err)
	}

	defer jsonFile.Close()

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		fmt.Println(err)
	}

	var config Config

	json.Unmarshal(byteValue, &config)

	fmt.Println(config)
}
