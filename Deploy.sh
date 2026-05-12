#!/usr/bin/sh

set -vxe

# グループ作成
if ! getent group nwp > /dev/null; then
    sudo groupadd --gid 2000 nwp
fi

# ユーザー作成（既に存在する場合は重要な設定だけ修正）
if ! id nwp > /dev/null 2>&1; then
    sudo useradd --uid 2000 \
             --gid nwp \
             --groups adm \
             --shell /bin/false \
             --create-home \
             --comment "nwp application user" \
             nwp
else
    # 既存ユーザーの修正
    sudo usermod --uid 2000 --gid nwp --shell /bin/false nwp
    sudo usermod -aG adm nwp 2>/dev/null || true   # 既にadmに入っていてもエラーにならない
fi

for u in wismon[34].service
do
  test -f $u
  sudo install -m 0644 $u /etc/systemd/system/$u
done
# decommissioned
rm -f /etc/systemd/system/wismon[12].service

test -f wismon-cfg.json
if test -f /usr/local/etc/wismon-cfg.json
then
  echo 'Found /usr/local/etc/wismon-cfg.json : skip installing'
else
  sudo install -m 0644 wismon-cfg.json /usr/local/etc
fi

for p in wnmsaver.rb wnm-topicstat.rb run-topicstat.sh topicstat-ctab.rb \
  wnm-gtshist.rb wnm-convobs.rb
do
  test -f $p
  sudo install $p /usr/local/bin/
done

test -d /nwp/m0 || sudo install -d -o nwp -g nwp /nwp/m0
cnf=/etc/apache2/conf-available/nwp.conf
test -f $cnf || sudo install -m 0644 apache-nwp.conf $cnf
sudo a2enconf nwp
sudo install -m 0444 readme-datadir.txt /nwp/m0/README.txt

#test -f mdtopic.gdbm || false
#if test ! -f /nwp/m0/mdtopic.gdbm
#then
#  sudo install -m 0444 mdtopic.gdbm /nwp/m0/
#fi

test -d /nwp/m1 || sudo install -d -o nwp -g nwp /nwp/m1
sudo -u nwp touch /nwp/m1/gtshist-jmagc.txt
