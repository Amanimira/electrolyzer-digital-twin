# Use the official Mosquitto image as the base
FROM eclipse-mosquitto:2.0

# Install the Mosquitto client tools for the healthcheck
RUN apt-get update && apt-get install -y mosquitto-clients

# Expose the standard MQTT ports
EXPOSE 1883 9001