#!/usr/bin/sh

set -vxe
for u in wismon[123].service
do
  test -f $u
  sudo install -m 0644 $u /etc/systemd/system/$u
done

test -f wismon-cfg.json
if test -f /usr/local/etc/wismon-cfg.json
then
  echo 'Found /usr/local/etc/wismon-cfg.json : skip installing'
else
  sudo install -m 0644 wismon-cfg.json /usr/local/etc
fi

for p in wnmsaver.rb wnm-topicstat.rb run-topicstat.sh topicstat-ctab.rb \
  wnm-gtshist.rb
do
  test -f $p
  sudo install $p /usr/local/bin/
done

test -d /nwp/m0 || sudo install -d -o nwp -g nwp /nwp/m0
sudo install -m 0444 readme-datadir.txt /nwp/m0/README.txt

test -f mdtopic.gdbm || false
if test ! -f /nwp/m0/mdtopic.gdbm
then
  sudo install -m 0444 mdtopic.gdbm /nwp/m0/
fi

test -d /nwp/m1 || sudo install -d -o nwp -g nwp /nwp/m1
