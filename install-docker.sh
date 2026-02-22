#!/bin/bash
set -euo pipefail

echo "=== 安装 Docker on Ubuntu 24.04 (WSL2) ==="

sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

sudo usermod -aG docker "$USER"

sudo systemctl enable docker
sudo systemctl start docker

echo "=== Docker 安装完成 ==="
docker --version
docker compose version
echo ""
echo "提示: 请运行 'newgrp docker' 或重新打开终端以使用免 sudo 的 docker 命令"
