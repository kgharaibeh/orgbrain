#!/bin/bash
# OrgBrain EC2 bootstrap — runs as root via EC2 user-data on first boot.
# Clones the repo, builds images locally, starts the full platform.
set -euxo pipefail
exec > /var/log/orgbrain-bootstrap.log 2>&1

REPO="https://github.com/kgharaibeh/orgbrain.git"
INSTALL_DIR="/opt/orgbrain"
DATA_DIR="/data"

# ── System update + packages ───────────────────────────────────────────────────
dnf update -y
dnf install -y docker git htop

# ── Docker Compose v2 ─────────────────────────────────────────────────────────
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
     -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
ln -sf /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

# ── Start + enable Docker ─────────────────────────────────────────────────────
systemctl enable --now docker
usermod -aG docker ec2-user

# ── Data volume setup (/dev/xvdf = 200 GB EBS data volume) ────────────────────
if [ -b /dev/xvdf ] && ! blkid /dev/xvdf &>/dev/null; then
  mkfs.xfs /dev/xvdf
fi
mkdir -p "$DATA_DIR"
if [ -b /dev/xvdf ]; then
  mount /dev/xvdf "$DATA_DIR"
  echo "/dev/xvdf $DATA_DIR xfs defaults,nofail 0 2" >> /etc/fstab
fi
mkdir -p "$DATA_DIR"/{neo4j/data,neo4j/logs,neo4j/plugins,qdrant,timescale,postgres,minio,grafana,vault}

# ── Clone repo ────────────────────────────────────────────────────────────────
git clone "$REPO" "$INSTALL_DIR"

# ── Get public IP ─────────────────────────────────────────────────────────────
PUBLIC_IP=$(curl -sf http://169.254.169.254/latest/meta-data/public-ipv4 || echo "localhost")
export PUBLIC_IP

cat > "$INSTALL_DIR/infra/.env.aws" <<EOF
PUBLIC_IP=${PUBLIC_IP}
EOF

# ── Build custom images ────────────────────────────────────────────────────────
cd "$INSTALL_DIR/infra"
docker compose -f docker-compose.yml -f aws/docker-compose.aws.yml build \
  cp-backend cp-frontend flink-jobmanager

# ── Start platform ─────────────────────────────────────────────────────────────
docker compose -f docker-compose.yml -f aws/docker-compose.aws.yml \
  --env-file .env.aws up -d

# ── Pull Ollama model ──────────────────────────────────────────────────────────
# Give Ollama 60 s to start, then pull the embedding + LLM models
sleep 60
docker exec orgbrain-ollama ollama pull nomic-embed-text  || true
docker exec orgbrain-ollama ollama pull llama3.1:8b       || true

# ── Systemd service for reboot persistence ────────────────────────────────────
cat > /etc/systemd/system/orgbrain.service << 'EOF'
[Unit]
Description=OrgBrain Platform
After=docker.service network-online.target
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/orgbrain/infra
EnvironmentFile=/opt/orgbrain/infra/.env.aws
ExecStart=/usr/local/lib/docker/cli-plugins/docker-compose \
          -f docker-compose.yml -f aws/docker-compose.aws.yml \
          --env-file .env.aws up -d
ExecStop=/usr/local/lib/docker/cli-plugins/docker-compose \
         -f docker-compose.yml -f aws/docker-compose.aws.yml down
Restart=no

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable orgbrain

echo "Bootstrap complete. OrgBrain starting at http://${PUBLIC_IP}:3001"
