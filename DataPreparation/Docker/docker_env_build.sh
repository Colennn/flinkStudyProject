## mysql
## https://blog.csdn.net/wenqi1992/article/details/138803216?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522172053188216800186583978%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=172053188216800186583978&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~sobaiduend~default-1-138803216-null-null.142^v100^pc_search_result_base3&utm_term=mac%20docker%20mysql&spm=1018.2226.3001.4187

docker pull mysql:8.0
docker run -d \
  --name mysql-server \
  -p 3306:3306 \
  -v ~/mysql/data:/var/lib/mysql8.0 \
  -e MYSQL_ROOT_PASSWORD=123456 \
  mysql:8.0


## kafka zookeeper
## https://blog.csdn.net/weixin_42262463/article/details/135978993?ops_request_misc=&request_id=&biz_id=102&utm_term=mac%20docker%20%E5%AE%89%E8%A3%85%20kafka%20flink&utm_medium=distribute.pc_search_result.none-task-blog-2~all~sobaiduweb~default-1-135978993.142^v100^pc_search_result_base3&spm=1018.2226.3001.4187

docker pull zookeeper:3.6
docker pull cppla/kafka-docker:arm
docker run -d --name zookeeper -p 2181:2181 -v /etc/localtime:/etc/localtime  -e TZ=Asia/Shanghai  zookeeper:3.6
docker run -d --name kafka  -v /etc/localtime:/etc/localtime:ro -p 9092:9092  -e TZ=Asia/Shanghai --link zookeeper:zookeeper --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 --env KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 cppla/kafka-docker:arm

# 进入容器
docker exec -it kafka bash
# 创建topic
kafka-topics.sh --create --topic topic_create_test --partitions 1 --replication-factor 1 --zookeeper zookeeper:2181 
# 创建生产者
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic topic_create_test
#{"id":773320,"name":"杨娟","address":"吉林省杭州市合川梧州路L座 934689","create_time":1653905861,"event_time":1653905561,"price":40343.9750680629,"list_info":["QeWHWDTPqOyjxivCdHFb","mAEgJHRZnojFWbueQiiR","AZWPwVMRyJKCJMXaEJhp","cbeTljzvygYcDrwdLKel","HFiEKQiFkEejdZQftpbE"],"map_info":{"也是":"aHzrLubeXUZLbwURNwmK","搜索":"MXhrcgUVOmNTqDjIHGTD","一些":"reiKveakVvTvNoesUjYE","女人":"ckoKoxJCrxniQIFAUiZu","之间":"DUXVsnqwtDrnxAcwFcZx"}}
# 创建消费者
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic_create_test
# 查看有哪些 topic
kafka-topics.sh --list --zookeeper 192.168.10.123:2181


## flink
## https://blog.csdn.net/weixin_34929338/article/details/136066736?ops_request_misc=&request_id=&biz_id=102&utm_term=docker%20flink&utm_medium=distribute.pc_search_result.none-task-blog-2~all~sobaiduweb~default-4-136066736.142^v100^pc_search_result_base3&spm=1018.2226.3001.4187

docker pull flink
docker network create flink-network
docker run \
  -itd \
  --name=jobmanager \
  --publish 8081:8081 \
  --network flink-network \
  --env FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
  flink jobmanager 
docker run \
  -itd \
  --name=taskmanager \
  --network flink-network \
  --env FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
  flink taskmanager 

## HBase
## https://blog.csdn.net/weixin_41709748/article/details/115718614?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522172083670316800184119925%2522%252C%2522scm%2522%253A%252220140713.130102334.pc%255Fall.%2522%257D&request_id=172083670316800184119925&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~first_rank_ecpm_v1~rank_v31_ecpm-1-115718614-null-null.142^v100^pc_search_result_base3&utm_term=mac%20docker%20hbase%E5%8D%95%E6%9C%BA&spm=1018.2226.3001.4187

docker pull harisekhon/hbase:1.3
docker run -d -h hbase    \
-p 2182:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095    \
-p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301   \
--name hbase  \
harisekhon/hbase:1.3

hbase shell #进入hbase命令行
list #查看表列表

