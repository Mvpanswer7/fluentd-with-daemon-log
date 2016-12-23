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
puts 'jinchen ### agent.rb'
require 'fluent/configurable'
require 'fluent/engine'
require 'fluent/plugin'
require 'fluent/output'

module Fluent
  #
  # Agent is a resource unit who manages emittable plugins
  #
  # Next step: `fluentd/root_agent.rb`
  # Next step: `fluentd/label.rb`
  #
  class Agent
    include Configurable

    def initialize(opts = {})
      puts 'jinchen ------  Agent.rb : initialize start'
      super()

      @context = nil
      @outputs = []
      @filters = []
      @started_outputs = []
      @started_filters = []

      @log = Engine.log
      @event_router = EventRouter.new(NoMatchMatch.new(log), self)
      @error_collector = nil
      puts 'jinchen ------  Agent.rb : initialize end'
    end

    attr_reader :log
    attr_reader :outputs
    attr_reader :filters
    attr_reader :context
    attr_reader :event_router
    attr_reader :error_collector

    def configure(conf)
      puts 'jinchen ------  Agent.rb : configure start'
      super

      # initialize <match> and <filter> elements
      conf.elements.select { |e| e.name == 'filter' || e.name == 'match' }.each { |e|
        pattern = e.arg.empty? ? '**' : e.arg
        type = e['@type'] || e['type']
        if e.name == 'filter'
          add_filter(type, pattern, e)
        else
          add_match(type, pattern, e)
        end
      }
      puts 'jinchen ------  Agent.rb : configure end'
    end

    def start
      puts 'jinchen ------  Agent.rb : start start'
      @outputs.each { |o|
        o.start
        @started_outputs << o
      }

      @filters.each { |f|
        f.start
        @started_filters << f
      }
      puts 'jinchen ------  Agent.rb : start end'
    end

    def shutdown
      puts 'jinchen ------  Agent.rb : shutdown start'
      @started_filters.map { |f|
        Thread.new do
          begin
            log.info "shutting down filter#{@context.nil? ? '' : " in #{@context}"}", type: Plugin.lookup_name_from_class(f.class), plugin_id: f.plugin_id
            f.shutdown
          rescue => e
            log.warn "unexpected error while shutting down filter plugins", plugin: f.class, plugin_id: f.plugin_id, error_class: e.class, error: e
            log.warn_backtrace
          end
        end
      }.each { |t| t.join }

      # Output plugin as filter emits records at shutdown so emit problem still exist.
      # This problem will be resolved after actual filter mechanizm.
      @started_outputs.map { |o|
        Thread.new do
          begin
            log.info "shutting down output#{@context.nil? ? '' : " in #{@context}"}", type: Plugin.lookup_name_from_class(o.class), plugin_id: o.plugin_id
            o.shutdown
          rescue => e
            log.warn "unexpected error while shutting down output plugins", plugin: o.class, plugin_id: o.plugin_id, error_class: e.class, error: e
            log.warn_backtrace
          end
        end
      }.each { |t| t.join }
      puts 'jinchen ------  Agent.rb : shutdown end'
    end

    def flush!
      puts 'jinchen ------  Agent.rb : flush! start'
      flush_recursive(@outputs)
      puts 'jinchen ------  Agent.rb : flush! end'
    end

    def flush_recursive(array)
      puts 'jinchen ------  Agent.rb : flush_recursive start'
      array.each { |o|
        begin
          if o.is_a?(BufferedOutput)
            o.force_flush
          elsif o.is_a?(MultiOutput)
            flush_recursive(o.outputs)
          end
        rescue => e
          log.debug "error while force flushing", error_class: e.class, error: e
          log.debug_backtrace
        end
      }
      puts 'jinchen ------  Agent.rb : flush_recursive end'
    end

    def add_match(type, pattern, conf)
      puts 'jinchen ------  Agent.rb : add_match start'
      log.warn "adding match#{@context.nil? ? '' : " in #{@context}"}", pattern: pattern, type: type

      output = Plugin.new_output(type)
      output.router = @event_router
      output.configure(conf)
      @outputs << output
      @event_router.add_rule(pattern, output)
      puts 'jinchen ------  Agent.rb : add_match end'

      output
    end

    def add_filter(type, pattern, conf)
      puts 'jinchen ------  Agent.rb : add_filter start'
      log.info "adding filter#{@context.nil? ? '' : " in #{@context}"}", pattern: pattern, type: type

      filter = Plugin.new_filter(type)
      filter.router = @event_router
      filter.configure(conf)
      @filters << filter
      @event_router.add_rule(pattern, filter)
      puts 'jinchen ------  Agent.rb : add_filter end'

      filter
    end

    # For handling invalid record
    def emit_error_event(tag, time, record, error)
      puts 'jinchen ------  Agent.rb : emit_error_event start'
      puts 'jinchen ------  Agent.rb : emit_error_event end'
    end

    def handle_emits_error(tag, es, error)
      puts 'jinchen ------  Agent.rb : handle_emits_error start'
      puts 'jinchen ------  Agent.rb : handle_emits_error end'
    end

    class NoMatchMatch
      def initialize(log)
        puts 'jinchen ------  Agent.rb : no-match-event-initialize start'
        @log = log
        @count = 0
        puts 'jinchen ------  Agent.rb : no-match-event-initialize end'
      end

      def emit(tag, es, chain)
        puts 'jinchen ------  Agent.rb : no-match-event-emit start'
        # TODO use time instead of num of records
        c = (@count += 1)
        if c < 512
          if Math.log(c) / Math.log(2) % 1.0 == 0
            @log.warn "no patterns matched", tag: tag
            return
          end
        else
          if c % 512 == 0
            @log.warn "no patterns matched", tag: tag 
            return 
          end 
        end
        puts 'jinchen ------  Agent.rb : no-match-event-emit end'
        @log.on_trace { @log.trace "no patterns matched", tag: tag }
      end

      def start
        puts 'jinchen ------  Agent.rb : no-match-event-start start'
        puts 'jinchen ------  Agent.rb : no-match-event-start end'
      end

      def shutdown
        puts 'jinchen ------  Agent.rb : no-match-event-shutdown start'
        puts 'jinchen ------  Agent.rb : no-match-event-shutdown end'
      end
    end
  end
end
