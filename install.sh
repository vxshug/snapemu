#!/bin/bash

install_dir=$(pwd)/snapemu
db_password=''
web_domain=''
jwt_key=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c 16)

RED='\e[31m'
RESET='\e[0m'
GREEN='\e[32m'
error_log() {
    local input="$1"
    echo -e "${RED}${input}${RESET}"
    exit 1
}
info_log() {
    local input="$1"
    echo -e "${GREEN}${input}${RESET}"
}

if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    echo "Only Ubuntu and Debian are supported"
    exit 1
fi

install_docker_ubuntu_debian() {
  sudo apt-get update
  sudo apt-get install ca-certificates curl
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update
  sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
}

check_docker_installed() {
    if command -v docker &> /dev/null; then
        local version=$(docker --version)
        info_log "docker is installed：${version}"
    else
      ask_to_install_docker
      check_docker_installed
    fi
}

check_docker_running() {
    if sudo systemctl is-active --quiet docker; then
        info_log "Docker is running"
    else
        echo "Docker is not running, trying to start..."
        sudo systemctl start docker
        if sudo systemctl is-active --quiet docker; then
            info_log "Docker has started successfully"
        else
            error_log "Docker startup failed, please check the problem"
            exit 1
        fi
    fi
}

ask_to_install_docker() {
    local answer
    # 提示用户输入
    echo -n -e "Do you want to install docker? (Y/n)"
    read answer
    answer="${answer:-Y}"
    case "$answer" in
        yes|y|Y)
            ;;
        *)
            error_log "exit setup"
            exit 1
            ;;
    esac
    case "$OS" in
        ubuntu|debian)
            install_docker_ubuntu_debian
            ;;
        *)
            error_log "The operating system does not support it"
            exit 1
            ;;
    esac
}

check_install_dir() {
  if [ -d "$install_dir" ]; then
    error_log "The software has been installed in $install_dir"
  else
    info_log "The software will be installed in ${install_dir}"
    mkdir -p $install_dir
  fi
}

generate_docker_compose() {
  cat <<EOF > ${install_dir}/compose.yaml
services:
  traefik:
    image: "traefik:v3.2"
    restart: always
    command:
      - "--api.insecure=true"
      - "--providers.docker"
      - "--entrypoints.web.address=:80"
      - "--entrypoints.web.forwardedheaders.insecure"
      - "--entrypoints.websecure.address=:443"
      - "--entrypoints.websecure.http3"
      - "--certificatesresolvers.myresolver.acme.httpchallenge.entrypoint=web"
      - "--certificatesresolvers.myresolver.acme.email=example@example.com"
      - "--certificatesresolvers.myresolver.acme.storage=/letsencrypt/acme.json"
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - "letsencrypt:/letsencrypt"
      - "/var/run/docker.sock:/var/run/docker.sock:ro"
  db:
    image: 'postgres:16.3-bookworm'
    restart: always
    environment:
      POSTGRES_PASSWORD: ${db_password}
      POSTGRES_DB: snapemu
    volumes:
      - "db-data:/var/lib/postgresql/data"
  redis:
    image: 'redis:7.2.4-bookworm'
    restart: always
  api:
    image: 'registry.cn-hongkong.aliyuncs.com/snap_emu/api:alpine-dev'
    pull_policy: always
    volumes:
      - ./config:/etc/snapemu:ro
      - "assets:/app/assets"
    restart: always
    command: ["./snap_api", "run"]
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.snapemu.rule=Host(\`${web_domain}\`) && PathPrefix(\`/api\`)"
      - "traefik.http.routers.snapemu.service=snapemu"
      - "traefik.http.routers.snapemu.tls=true"
      - "traefik.http.routers.snapemu.tls.certresolver=myresolver"
      - "traefik.http.routers.snapemuhttp.rule=Host(\`${web_domain}\`) && PathPrefix(\`/api\`)"
      - "traefik.http.routers.snapemuhttp.service=snapemu"
      - "traefik.http.services.snapemu.loadbalancer.server.port=8000"
  manager:
    image: 'registry.cn-hongkong.aliyuncs.com/snap_emu/manager:alpine-dev'
    pull_policy: always
    ports:
      - 1700:1700/udp
    volumes:
      - ./config:/etc/snapemu:ro
    restart: always
    command: ["./devices_manager", "run"]
  web:
    image: 'registry.cn-hongkong.aliyuncs.com/snap_emu/web:alpine-dev'
    pull_policy: always
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.web.rule=Host(\`${web_domain}\`)"
      - "traefik.http.routers.web.service=web"
      - "traefik.http.routers.web.tls=true"
      - "traefik.http.routers.web.tls.certresolver=myresolver"
      - "traefik.http.routers.web.rule=Host(\`${web_domain}\`)"
      - "traefik.http.routers.web.service=web"
      - "traefik.http.services.web.loadbalancer.server.port=80"
volumes:
  letsencrypt:
  db-data:
  assets:
EOF
}

generate_config() {
  local config_dir=${install_dir}/config
  if [ -d "$config_dir" ]; then
    error_log "The software has been installed in ${config_dir}"
  else
    mkdir -p $config_dir
  fi
  local api_config=${install_dir}/config/config.yaml
  read -p "Enter the website domain name: " web_domain
  read -p "Please enter the database password: " db_password
  cat <<EOF > ${api_config}
log: INFO
device:
  topic:
    data: LoRa-Push-Data
    event: LoRa-Event
    down: LoRa-Down-Data
  lorawan:
    host: "0.0.0.0"
    port: 1700
api:
  model:
    path: /etc/snapemu/model.yaml
  port: 8000
  host: "0.0.0.0"
  web: "https://platform.snapemu.com"
redis:
  host: redis
  db: 0
db:
  host: db
  password: ${db_password}
  username: postgres
  db: snapemu
jwt_key: ${jwt_key}
EOF
}


start_run() {
  cd $install_dir
  sudo docker compose up -d
}

check_docker_installed
check_docker_running
check_install_dir
generate_config
generate_docker_compose
start_run