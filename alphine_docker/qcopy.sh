#!/bin/bash
echo "Start"
sudo docker pull <account_id>.dkr.ecr.eu-west-1.amazonaws.com/qualdo:zipfilecheck_1
echo "PUll completed"
sudo docker run <account_id>.dkr.ecr.eu-west-1.amazonaws.com/qualdo:zipfilecheck_1 &
echo "Step1"
sleep 20
container_id=`sudo docker ps -q`
echo "Step2"
echo $container_id
echo "step 3"
sudo docker cp $container_id:/qualdo/check.txt ./check.txt
echo "Final step"
sudo docker kill $container_id
echo "docker kill completed"
