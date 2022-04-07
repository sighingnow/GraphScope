ps -ef | grep 'vineyardd --socket' | grep -v grep | awk '{print "sudo kill " $2}' | sh 
sudo vineyardd --socket /tmp/vineyard.sock --size 128Gi > v6d.log &
