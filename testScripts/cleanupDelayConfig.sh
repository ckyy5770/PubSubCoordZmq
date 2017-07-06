#!/bin/bash

# clean up

# delete filters
sudo tc filter del dev enp0s3 prio 1

# delete qdisc
sudo tc qdisc del dev enp0s3 root

# print clean up results
sudo tc -s qdisc ls dev enp0s3
sudo tc filter show dev enp0s3
