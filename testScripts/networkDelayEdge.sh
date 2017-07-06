#!/bin/bash

# network delay configuration for edge brokers

BROKER_ROUTING_DELAY="200ms"
BROKER_CLIENT_DELAY="100ms"

ROUTING_REGION="8.8.8.8/32"
CLIENT_REGION="8.8.4.4/32"

# clean up qdisc

# delete filters
sudo tc filter del dev enp0s3 prio 1

# delete qdisc
sudo tc qdisc del dev enp0s3 root

# add new priority qdisc, with 3 bands and priority mapping for normal service (no priority)
sudo tc qdisc add dev enp0s3 root handle 1: prio bands 3 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0

# place a BROKER_ROUTING_DELAY on 1:1
sudo tc qdisc add dev enp0s3 parent 1:1 handle 10: netem delay $BROKER_ROUTING_DELAY

# place a BROKER_ROUTING_DELAY on 1:2
sudo tc qdisc add dev enp0s3 parent 1:2 handle 20: netem delay $BROKER_CLIENT_DELAY

# add filters at the root qdisc

# foward pkgs for ROUTING_REGION to 1:1
sudo tc filter add dev enp0s3 protocol ip parent 1: prio 1 u32 match ip dst $ROUTING_REGION flowid 1:1

# foward pkgs for CLIENT_REGION to 1:2
sudo tc filter add dev enp0s3 protocol ip parent 1: prio 1 u32 match ip dst $CLIENT_REGION flowid 1:2

# forward other pkgs to 1:3
sudo tc filter add dev enp0s3 protocol ip parent 1: prio 1 flowid 1:3

# print configuration results
sudo tc -s qdisc ls dev enp0s3
