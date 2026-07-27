# atest-ext-store-mqtt

## Introduction
`atest-ext-store-mqtt` is an extension for the [linuxsuren/api-testing](https://github.com/linuxsuren/api-testing) project, designed to integrate MQTT as a data source for API testing. This extension allows you to subscribe to MQTT topics and retrieve messages as test data, enabling testing of IoT and messaging-based systems.

## Features
- **MQTT Integration**: Seamlessly connect your API testing environment with MQTT brokers.
- **Topic Subscription**: Subscribe to MQTT topics and retrieve published messages.
- **Data Query**: Use topic filters to query and collect MQTT messages.

## Getting Started

### Prerequisites
- Go 1.23 or higher
- MQTT broker (e.g., Mosquitto, EMQX, HiveMQ)

### Installation
1. Clone the repository:
