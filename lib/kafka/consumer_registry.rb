# This class handles the consumers interaction with zookeeper
#
# Directories:
# 1. Consumer id registry:
# /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
# A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
# and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
# A consumer subscribes to event changes of the consumer id registry within its group.
#
# The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
# ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
# whether the creation of a sequential znode has succeeded or not. More details can be found at
# (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
#
# 2. Broker node registry:
# /brokers/[0...N] --> { "host" : "host:port",
#                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
#                                    "topicN": ["partition1" ... "partitionN"] } }
# This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
# node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
# is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
# the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
# A consumer subscribes to event changes of the broker node registry.
#
# 3. Partition owner registry:
# /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
# This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
# within a consumer group. The mapping is reestablished after each rebalancing.
#
# 4. Consumer offset tracking:
# /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
# Each consumer tracks the offset of the latest message consumed for each partition.
#
#/

require 'zk'
require 'json'

module Kafka
  class ConsumerRegistry
    def initialize(connect='localhost:9092')
      @connect = connect
    end
    
    def register(group_id, consumer_id, topic)
      dir = "/consumers/#{group_id}/ids"
      keeper.mkdir_p(dir)
      
      keeper.create(
        "#{dir}/#{consumer_id}", 
        {topic => 1}.to_json, 
        {:ephemeral => true}
      )
    end
    
    def keeper
      @keeper = ZK.new(@connect) if @keeper.nil?
      @keeper
    end
  end
end