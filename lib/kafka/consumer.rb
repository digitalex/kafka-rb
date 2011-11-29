# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
module Kafka
  class Consumer

    include Kafka::IO

    CONSUME_REQUEST_TYPE = Kafka::RequestType::FETCH
    MAX_SIZE = 1048576 # 1 MB
    DEFAULT_POLLING_INTERVAL = 2 # 2 seconds
    DEFAULT_GROUP_ID = "test"

    attr_accessor :topic, :partition, :offset, :max_size, :request_type, :polling, :group_id, :consumer_id

    def initialize(options = {})
      self.topic        = options[:topic]        || "test"
      self.partition    = options[:partition]    || 0
      self.host         = options[:host]         || "localhost"
      self.port         = options[:port]         || 9092
      self.offset       = options[:offset]       || 0
      self.max_size     = options[:max_size]     || MAX_SIZE
      self.request_type = options[:request_type] || CONSUME_REQUEST_TYPE
      self.polling      = options[:polling]      || DEFAULT_POLLING_INTERVAL
      self.group_id     = options[:group_id]     || DEFAULT_GROUP_ID
      self.consumer_id  = options[:consumer_id]  || Consumer.generate_id(self.group_id)
      
      self.connect(self.host, self.port)
    end

    # REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
    def request_size
      2 + 2 + topic.length + 4 + 8 + 4
    end

    def encode_request_size
      [self.request_size].pack("N")
    end

    def encode_request(request_type, topic, partition, offset, max_size)
      request_type = [request_type].pack("n")
      topic        = [topic.length].pack('n') + topic
      partition    = [partition].pack("N")
      offset       = [offset].pack("Q").reverse # DIY 64bit big endian integer
      max_size     = [max_size].pack("N")

      request_type + topic + partition + offset + max_size
    end

    def consume
      self.send_consume_request         # request data
      data = self.read_data_response    # read data response
      self.parse_message_set_from(data) # parse message set
    end

    def loop(&block)
      messages = []
      while(true) do
        messages = self.consume
        block.call(messages) if messages && !messages.empty?
        sleep(self.polling)
      end
    end

    def read_data_response
      data_length = self.socket.read(4).unpack("N").shift # read length
      data = self.socket.read(data_length)                # read message set
      data[2, data.length]                                # we start with a 2 byte offset
    end

    def send_consume_request
      self.write(self.encode_request_size) # write request_size
      self.write(self.encode_request(self.request_type, self.topic, self.partition, self.offset, self.max_size)) # write request
    end

    def parse_message_set_from(data)
      messages = []
      processed = 0
      length = data.length - 4
      while(processed <= length) do
        message_size = data[processed, 4].unpack("N").shift + 4
        message_data = data[processed, message_size]
        break unless message_data.size == message_size
        messages << Kafka::Message.parse_from(message_data)
        processed += message_size
      end
      self.offset += processed
      messages
    end
    
    private
    
    def self.generate_id(group_id)
      "#{group_id}_#{Socket.gethostname}-#{Time.now.usec}"
    end
  end
end
