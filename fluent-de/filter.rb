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

puts 'jinchen ### filter.rb'

require 'fluent/config'
require 'fluent/configurable'
require 'fluent/engine'
require 'fluent/event'
require 'fluent/log'

module Fluent
  class Filter
    include Configurable
    include PluginId
    include PluginLoggerMixin

    attr_accessor :router

    def initialize
      puts 'jinchen ----- filter.rb : initialize '
      super
    end

    def configure(conf)
      puts 'jinchen ----- filter.rb : configure start'
      super

      if label_name = conf['@label']
        label = Engine.root_agent.find_label(label_name)
        @router = label.event_router
      elsif @router.nil?
        @router = Engine.root_agent.event_router
      end
      puts 'jinchen ----- filter.rb : configure end'
    end

    def start
      puts 'jinchen ----- filter.rb : start'
    end

    def shutdown
      puts 'jinchen ----- filter.rb : shutdown'
    end

    def filter(tag, time, record)
      puts 'jinchen ----- filter.rb : filter '
      raise NotImplementedError, "Implement this method in child class"
    end

    def filter_stream(tag, es)
      puts 'jinchen ----- filter.rb : filter_stream start'
      new_es = MultiEventStream.new
      es.each { |time, record|
        begin
          filtered_record = filter(tag, time, record)
          new_es.add(time, filtered_record) if filtered_record
        rescue => e
          router.emit_error_event(tag, time, record, e)
        end
      }
      puts 'jinchen ----- filter.rb : filter_stream end'
      new_es
    end
  end
end
