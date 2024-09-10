# download zookeeper
# set remote_prefix to the remote path of the playbook
remote_prefix=/home/playground/zip
sudo curl -L https://archive.apache.org/dist/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz > $remote_prefix/zookeeper.tgz
sudo tar zxf $remote_prefix/zookeeper.tgz -C $remote_prefix
sudo cp -Rp $remote_prefix/apache-zookeeper-3.6.2-bin/. $remote_prefix/zookeeper/
sudo chown -R ryou $remote_prefix/zookeeper
sudo echo 'export PATH=$PATH:$remote_prefix/zookeeper/bin' >> ~/.bashrc
sudo source ~/.bashrc
sudo mkdir /tmp/zookeeper
sudo cp $remote_prefix/zookeeper/conf/zoo_sample.cfg $remote_prefix/zookeeper/conf/zoo.cfg


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
#TODO: this will make zookeeper start on boot on all nodes
# sudo $remote_prefix/zookeeper/bin/zkServer.sh start $remote_prefix/zookeeper/conf/zoo.cfg
# bin/zkServer.sh start /conf/zoo.cfg

# download Storm
sudo curl -L https://dlcdn.apache.org/storm/apache-storm-1.2.4/apache-storm-1.2.4.tar.gz > $remote_prefix/storm.tgz
sudo tar zxf $remote_prefix/storm.tgz -C $remote_prefix
# sudo cp -Rp $remote_prefix/apache-storm-1.2.4/. /usr/local/storm
sudo mv $remote_prefix/apache-storm-2.2.0 /usr/local/storm
#remove the tar file
rm $remote_prefix/apache-storm-1.2.4.tar.gz
#remove old storm directory
rm -r $remote_prefix/apache-storm-1.2.4
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
# echo 'export PATH=$PATH:$remote_prefix/storm/bin' >> ~/.bashrc
# source ~/.bashrc
echo 'export STORM_HOME=/usr/local/storm' >> ~/.bashrc
echo 'export PATH=$PATH:$STORM_HOME/bin' >> ~/.bashrc
source ~/.bashrc
# Download kafka
curl -L https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz > /home/playground/zip/kafka.tgz

# Unzip kafka to playgrounds directory
tar zxf /home/playground/zip/kafka.tgz -C /home/playground/

# # removing default kafka property
# rm /home/playground/kafka-3.8.0-src/config/server.properties

# # Copying the server.properties to /home/playground
# cp /local/repository/server.properties /home/playground/kafka_2.12-3.4.1/config

#rm -r /local/repository

# sudo rm -r /home/playground/zip
