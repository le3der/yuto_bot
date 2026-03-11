#!/bin/bash
# ════════════════════════════════════════════
#   Yuto Bot — VPS Setup Script
#   Run as root: bash setup_services.sh
# ════════════════════════════════════════════

echo "🚀 Setting up Yuto Bot services..."

# ── 1. Copy optimized files ──────────────────
cp /root/yuto_bot/v9_optimized.py        /root/yuto_bot/v9.py
cp /root/yuto_bot/telegram_auto_importer_optimized.py /root/yuto_bot/telegram_auto_importer.py
echo "✅ Files copied"

# ── 2. Create systemd service for main bot ──
cat > /etc/systemd/system/yuto-bot.service << 'EOF'
[Unit]
Description=Yuto Scanner Bot
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/root/yuto_bot
EnvironmentFile=/root/yuto_bot/.env
ExecStart=/usr/bin/python3 /root/yuto_bot/v9.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# ── 3. Create systemd service for importer ──
cat > /etc/systemd/system/yuto-importer.service << 'EOF'
[Unit]
Description=Yuto Telegram Auto Importer
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/root/yuto_bot
EnvironmentFile=/root/yuto_bot/.env
ExecStart=/usr/bin/python3 /root/yuto_bot/telegram_auto_importer.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo "✅ Service files created"

# ── 4. Enable and start services ────────────
systemctl daemon-reload
systemctl enable yuto-bot
systemctl enable yuto-importer
systemctl restart yuto-bot
systemctl restart yuto-importer

echo ""
echo "✅ Done! Check status with:"
echo "   systemctl status yuto-bot"
echo "   systemctl status yuto-importer"
echo ""
echo "📋 View live logs:"
echo "   journalctl -u yuto-bot -f"
echo "   journalctl -u yuto-importer -f"
