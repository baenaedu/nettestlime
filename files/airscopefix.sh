#/bin/sh

while true
do
echo "Starting Airscope"
cd /home/odroid/airscope
./airscope airscope_monroe_restful.conf
sleep 3
done
