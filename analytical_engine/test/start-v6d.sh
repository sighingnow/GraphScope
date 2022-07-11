ps -ef | grep 'vineyardd --socket' | grep -v grep | awk '{print "sudo kill " $2}' | sh 
sudo vineyardd --socket /tmp/vineyard.sock --size 128Gi   > v6d.log &
#,http://11.227.236.67:2379
#-etcd_endpoint http://11.227.236.89:2379
