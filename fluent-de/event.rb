#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

puts 'jinchen ### event.rb'
require 'fluent/engine'

module Fluent
  class EventStream
    include Enumerable

    def repeatable?
      puts 'jinchen ----- event.rb : eventstream : repeatable? start'
      false
      puts 'jinchen ----- event.rb : eventstream : repeatable? end'
    end

    def each(&block)
      puts 'jinchen ----- event.rb : eventstream : each '
      raise NotImplementedError, "DO NOT USE THIS CLASS directly."
    end

    def to_msgpack_stream
      puts 'jinchen ----- event.rb : eventstream : to_msgpack_stream start'
      out = Fluent::Engine.msgpack_factory.packer
      each {|time,record|
        out.write([time,record])
      }
      out.to_s
      puts 'jinchen ----- event.rb : eventstream : to_msgpack_stream end'
    end
  end


  class OneEventStream < EventStream
    def initialize(time, record)
      puts 'jinchen ----- event.rb : one-event-stream : initialize start'
      @time = time
      @record = record
      puts 'jinchen ----- event.rb : one-event-stream : initialize end'
    end

    def dup
      puts 'jinchen ----- event.rb : one-event-stream : dup start'
      OneEventStream.new(@time, @record.dup)
      puts 'jinchen ----- event.rb : one-event-stream : dup end'
    end

    def repeatable?
      puts 'jinchen ----- event.rb : one-event-stream : repeatable? start'
      true
      puts 'jinchen ----- event.rb : one-event-stream : repeatable? end'
    end

    def each(&block)
      puts 'jinchen ----- event.rb : one-event-stream : each start'
      block.call(@time, @record)
      nil
      puts 'jinchen ----- event.rb : one-event-stream : each end'
    end
  end

  # EventStream from entries: Array of [time, record]
  #
  # Use this class for many events data with a tag
  # and its representation is [ [time, record], [time, record], .. ]
  class ArrayEventStream < EventStream
    def initialize(entries)
      puts 'jinchen ----- event.rb : array-event-stream : initialize start'
      @entries = entries
      puts 'jinchen ----- event.rb : array-event-stream : initialize end'
    end

    def dup
      puts 'jinchen ----- event.rb : array-event-stream : dup start'
      entries = @entries.map { |entry| entry.dup } # @entries.map(:dup) doesn't work by ArgumentError
      ArrayEventStream.new(entries)
      puts 'jinchen ----- event.rb : array-event-stream : dup end'
    end

    def repeatable?
      puts 'jinchen ----- event.rb : array-event-stream : repeatable? start'
      true
      puts 'jinchen ----- event.rb : array-event-stream : repeatable? end'
    end

    def empty?
      puts 'jinchen ----- event.rb : array-event-stream : empty? start'
      @entries.empty?
      puts 'jinchen ----- event.rb : array-event-stream : empty? end'
    end

    def each(&block)
      puts 'jinchen ----- event.rb : array-event-stream : each start'
      @entries.each(&block)
      nil
      puts 'jinchen ----- event.rb : array-event-stream : each end'
    end
  end

  # EventStream from entries: numbers of pairs of time and record.
  #
  # This class can handle many events more efficiently than ArrayEventStream
  # because this class generate less objects than ArrayEventStream.
  #
  # Use this class as below, in loop of data-enumeration:
  #  1. initialize blank stream:
  #     streams[tag] ||= MultiEventStream
  #  2. add events
  #     stream[tag].add(time, record)
  class MultiEventStream < EventStream
    def initialize
      puts 'jinchen ----- event.rb : multi-event-stream : initialize start'
      @time_array = []
      @record_array = []
      puts 'jinchen ----- event.rb : multi-event-stream : initialize end'
    end

    def dup
      puts 'jinchen ----- event.rb : multi-event-stream : dup start'
      es = MultiEventStream.new
      @time_array.zip(@record_array).each { |time, record|
        es.add(time, record.dup)
      }
      es
      puts 'jinchen ----- event.rb : multi-event-stream : dup end'
    end

    def add(time, record)
      puts 'jinchen ----- event.rb : multi-event-stream : add start'
      @time_array << time
      @record_array << record
      puts 'jinchen ----- event.rb : multi-event-stream : add end'
    end

    def repeatable?
      true
      puts 'jinchen ----- event.rb : multi-event-stream : repeatable?'
    end

    def empty?
      @time_array.empty?
      puts 'jinchen ----- event.rb : multi-event-stream : empty?'
    end

    def each(&block)
      time_array = @time_array
      record_array = @record_array
      for i in 0..time_array.length-1
        block.call(time_array[i], record_array[i])
      end
      nil
      puts 'jinchen ----- event.rb : multi-event-stream : each'
    end
  end

  class MessagePackEventStream < EventStream
    # Keep cached_unpacker argument for existence plugins
    def initialize(data, cached_unpacker = nil)
      @data = data
      puts 'jinchen ----- event.rb : messagepack-event-stream : initialize'
    end

    def repeatable?
      true
      puts 'jinchen ----- event.rb : messagepack-event-stream : repeatable?'
    end

    def each(&block)
      # TODO format check
      unpacker = Fluent::Engine.msgpack_factory.unpacker
      unpacker.feed_each(@data, &block)
      nil
      puts 'jinchen ----- event.rb : messagepack-event-stream : each'
    end

    def to_msgpack_stream
      @data
      puts 'jinchen ----- event.rb : messagepack-event-stream : to_msgpack_stream'
    end
  end
end

