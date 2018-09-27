#!/bin/bash
ifconfig eth1 172.16.253.1 netmask 255.255.255.0
sleep 5
sshpass -p "odroid" scp -o StrictHostKeyChecking=no /opt/monroe/airscopefix.sh root@172.16.253.2:/home/odroid/airscope
odroidname=$(sshpass -p "odroid" ssh -o StrictHostKeyChecking=no -R 8080:localhost:8080 root@172.16.253.2 'cat /etc/hostname')
odroidfile=airscope_node_$odroidname
sshpass -p "odroid" ssh -o StrictHostKeyChecking=no -R 8080:localhost:8080 root@172.16.253.2 'cd /home/odroid/airscope && ./airscope airscope_monroe_restful.conf & sh /home/odroid/airscope/airscopefix.sh' & nodejs /opt/monroe/nodejs_server/server.js &
PID1=$!
python /opt/monroe/nettest.py&
PID2=$!
wait $PID2
echo 'PID2 has ended.'
cp /monroe/results/$odroidfile /monroe/results/limesdrresult.json
chmod +777 /monroe/results/limesdrresult.json
#kill -9 $PID1
echo 'All background processes have exited.'
#sleep 5
echo 'Experiment finished'
