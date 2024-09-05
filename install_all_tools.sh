# Copy custom_producer.py
cp /local/repository/custom_producer.py /home/playground/

# Downloading flink 
curl -0 https://archive.apache.org/dist/flink/flink-1.16.2/flink-1.16.2-bin-scala_2.12.tgz --output /home/playground/zip/flink.tgz
# Unzip flink to playgrounds directory
tar zxf /home/playground/zip/flink.tgz -C /home/playground/

# Copy oshi,jna and jna_platform
cp /local/repository/jna-platform-5.10.0.jar /home/playground/flink-1.16.2/lib
cp /local/repository/jna-5.10.0.jar /home/playground/flink-1.16.2/lib
cp /local/repository/oshi-core-6.1.5.jar /home/playground/flink-1.16.2/lib

# Copy dsp_jobs files
cp /local/repository/dsp_jobs-1.0-SNAPSHOT.jar /home/playground/flink-1.16.2/bin

# Copy roads.geojson file to flink/bin

cp /local/repository/roads.geojson /home/playground/flink-1.16.2/bin 

# Downloading prometheus 
curl -L https://github.com/prometheus/prometheus/releases/download/v2.42.0/prometheus-2.42.0.linux-amd64.tar.gz > /home/playground/zip/prometheus.tar.gz
# Unzip prometheus to playgrounds directory
tar zxf /home/playground/zip/prometheus.tar.gz -C /home/playground/

# Download grafana
curl -L https://dl.grafana.com/enterprise/release/grafana-enterprise-9.3.6.linux-amd64.tar.gz > /home/playground/zip/grafana.tar.gz
# Unzip grafana to playgrounds directory
tar zxf /home/playground/zip/grafana.tar.gz -C /home/playground/

# download zookeeper
# set remote_prefix to the remote path of the playbook
remote_prefix=/home/playground/zip
curl -L https://archive.apache.org/dist/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz > $remote_prefix/zookeeper.tgz
sudo cp -Rp $remote_prefix/apache-zookeeper-3.6.2-bin/. $remote_prefix/zookeeper/
sudo chown -R ryou $remote_prefix/zookeeper
echo 'export PATH=$PATH:$remote_prefix/zookeeper/bin' >> ~/.bashrc
source ~/.bashrc
mkdir /tmp/zookeeper
cp $remote_prefix/zookeeper/conf/zoo_sample.cfg $remote_prefix/zookeeper/conf/zoo.cfg

# Create a service file for Zookeeper
# TODO: check group name
echo "[Unit]
Description=Start Zookeeper

[Service]
Type=forking
ExecStart=${remote_prefix}/zookeeper/bin/zkServer.sh start ${remote_prefix}/zookeeper/conf/zoo.cfg
ExecStop=${remote_prefix}/zookeeper/bin/zkServer.sh stop ${remote_prefix}/zookeeper/conf/zoo.cfg
ExecRestart=${remote_prefix}/zookeeper/bin/zkServer.sh restart ${remote_prefix}/zookeeper/conf/zoo.cfg
User=ryou
Group=MAKI

[Install]
WantedBy=multi-user.target" | sudo tee /etc/systemd/system/zookeeper.service
#start zookeeper
$remote_prefix/zookeeper/bin/zkServer.sh start $remote_prefix/zookeeper/conf/zoo.cfg

# download Storm
curl -L https://archive.apache.org/dist/storm/apache-storm-2.4.0/apache-storm-2.4.0.tar.gz > $remote_prefix
tar zxf $remote_prefix/apache-storm-2.4.0.tar.gz -C $remote_prefix
sudo cp -Rp $remote_prefix/apache-storm-2.4.0/. $remote_prefix/storm
#remove the tar file
rm $remote_prefix/apache-storm-2.4.0.tar.gz
#remove old storm directory
rm -r $remote_prefix/apache-storm-2.4.0
# create tmp directory for storm
mkdir /tmp/storm
#Create entry for master in storm.yaml
# #!/bin/bash

# # Variables
# remote_prefix="/path/to/storm"  # replace with your actual path
# item="master"  # replace with your actual item
# is_openstack="true"  # replace with your actual condition

# # Check if is_openstack is true
# if [ "$is_openstack" = "true" ]; then
#   # Append the line to the file
#   echo -e "storm.zookeeper.servers: \n  - \"$item\"" >> "${remote_prefix}/storm/conf/storm.yaml"
# fi

# add storm to path
echo 'export PATH=$PATH:$remote_prefix/storm/bin' >> ~/.bashrc

# Download kafka
curl -L https://downloads.apache.org/kafka/3.8.0/kafka-3.8.0-src.tgz > /home/playground/zip/kafka.tgz

# Unzip kafka to playgrounds directory
tar zxf /home/playground/zip/kafka.tgz -C /home/playground/

# # removing default kafka property
# rm /home/playground/kafka-3.8.0-src/config/server.properties

# # Copying the server.properties to /home/playground
# cp /local/repository/server.properties /home/playground/kafka_2.12-3.4.1/config

#rm -r /local/repository

sudo rm -r /home/playground/zip