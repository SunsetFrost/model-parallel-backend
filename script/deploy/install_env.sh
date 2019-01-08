#!/bin/sh

# install docker 
sudo apt update
sudo apt install -y curl 

sudo sh get-docker.sh --mirror Aliyun

sudo systemctl enable docker
sudo systemctl start docker

# install ALi Accelerator
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json <<-'EOF'
{
  "registry-mirrors": ["https://51lm2h1x.mirror.aliyuncs.com"]
}
EOF
sudo systemctl daemon-reload
sudo systemctl restart docker

# install weave
sudo cp ./get-weave /usr/local/bin/weave  
sudo chmod a+x /usr/local/bin/weave  
