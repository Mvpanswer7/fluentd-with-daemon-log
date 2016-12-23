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

puts 'jinchen ### engine.rb'
require 'socket'

require 'msgpack'
require 'cool.io'

require 'fluent/config'
require 'fluent/event'
require 'fluent/event_router'
require 'fluent/root_agent'
require 'fluent/time'
require 'fluent/system_config'
require 'fluent/plugin'

module Fluent
  class EngineClass
    class DummyMessagePackFactory
      def packer(*args)
        puts 'jinchen ----- engine.rb : engineclass-DummyMessagePackFactory : packer start'
        MessagePack::Packer.new(*args)
        puts 'jinchen ----- engine.rb : engineclass-DummyMessagePackFactory : packer end'
      end

      def unpacker(*args)
        puts 'jinchen ----- engine.rb : engineclass-DummyMessagePackFactory : unpacker start'
        MessagePack::Unpacker.new(*args)
        puts 'jinchen ----- engine.rb : engineclass-DummyMessagePackFactory : unpacker end'
      end
    end

    def initialize
        puts 'jinchen ----- engine.rb : engineclass : initialize start'
      @root_agent = nil
      @event_router = nil
      @default_loop = nil
      @engine_stopped = false

      @log_emit_thread = nil
      @log_event_loop_stop = false
      @log_event_queue = []

      @suppress_config_dump = false

      @msgpack_factory = DummyMessagePackFactory.new
      puts 'jinchen ----- engine.rb : engineclass : initialize end'
    end

    MATCH_CACHE_SIZE = 1024
    LOG_EMIT_INTERVAL = 0.1

    attr_reader :root_agent
    attr_reader :matches, :sources
    attr_reader :msgpack_factory
    attr_reader :system_config

    def init(system_config)
      puts 'jinchen ----- engine.rb : engineclass : init start'
      @system_config = system_config

      BasicSocket.do_not_reverse_lookup = true
      Plugin.load_plugins
      if defined?(Encoding)
        Encoding.default_internal = 'ASCII-8BIT' if Encoding.respond_to?(:default_internal)
        Encoding.default_external = 'ASCII-8BIT' if Encoding.respond_to?(:default_external)
      end

      suppress_interval(system_config.emit_error_log_interval) unless system_config.emit_error_log_interval.nil?
      @suppress_config_dump = system_config.suppress_config_dump unless system_config.suppress_config_dump.nil?
      @without_source = system_config.without_source unless system_config.without_source.nil?

      @root_agent = RootAgent.new(@system_config)

      puts 'jinchen ----- engine.rb : engineclass : init end'
      self
    end

    def log
      puts 'jinchen ----- engine.rb : engineclass : log start'
      $log
    end

    def suppress_interval(interval_time)
      puts 'jinchen ----- engine.rb : engineclass : suppress_interval start'
      @suppress_emit_error_log_interval = interval_time
      @next_emit_error_log_time = Time.now.to_i
      puts 'jinchen ----- engine.rb : engineclass : suppress_interval end'
    end

    def parse_config(io, fname, basepath = Dir.pwd, v1_config = false)
      puts 'jinchen ----- engine.rb : engineclass : parse_config start'
      if fname =~ /\.rb$/
        require 'fluent/config/dsl'
        Config::DSL::Parser.parse(io, File.join(basepath, fname))
      else
        Config.parse(io, fname, basepath, v1_config)
      end
      puts 'jinchen ----- engine.rb : engineclass : parse_config end'
    end

    def run_configure(conf)
      puts 'jinchen ----- engine.rb : engineclass : run_config start'
      configure(conf)
      conf.check_not_fetched { |key, e|
        parent_name, plugin_name = e.unused_in
        if parent_name
          message = if plugin_name
                      "section <#{e.name}> is not used in <#{parent_name}> of #{plugin_name} plugin"
                    else
                      "section <#{e.name}> is not used in <#{parent_name}>"
                    end
          $log.warn message
          next
        end
        unless e.name == 'system'
          unless @without_source && e.name == 'source'
            $log.warn "parameter '#{key}' in #{e.to_s.strip} is not used."
          end
        end
      }
      puts 'jinchen ----- engine.rb : engineclass : run_config end'
    end

    def configure(conf)
      puts 'jinchen ----- engine.rb : engineclass : configure start'
      # plugins / configuration dumps
      Gem::Specification.find_all.select{|x| x.name =~ /^fluent(d|-(plugin|mixin)-.*)$/}.each do |spec|
        $log.info "gem '#{spec.name}' version '#{spec.version}'"
      end

      @root_agent.configure(conf)
      @event_router = @root_agent.event_router

      unless @suppress_config_dump
        $log.info "using configuration file: #{conf.to_s.rstrip}"
      end
      puts 'jinchen ----- engine.rb : engineclass : configure end'
    end

    def load_plugin_dir(dir)
      puts 'jinchen ----- engine.rb : engineclass :  load_plugin_dir'
      Plugin.load_plugin_dir(dir)
    end

    def emit(tag, time, record)
      puts 'jinchen ----- engine.rb : engineclass : emit'
      unless record.nil?
        emit_stream tag, OneEventStream.new(time, record)
      end
    end

    def emit_array(tag, array)
      puts 'jinchen ----- engine.rb : engineclass : emit_array'
      emit_stream tag, ArrayEventStream.new(array)
    end

    def emit_stream(tag, es)
      puts 'jinchen ----- engine.rb : engineclass : emit_stream'
      @event_router.emit_stream(tag, es)
    end

    def flush!
      puts 'jinchen ----- engine.rb : engineclass : flush!'
      @root_agent.flush!
    end

    def now
      # TODO thread update
      puts 'jinchen ----- engine.rb : engineclass : now'
      Time.now.to_i
    end

    def log_event_loop
      puts 'jinchen ----- engine.rb : engineclass : log_event_loop start'
      $log.disable_events(Thread.current)

      while sleep(LOG_EMIT_INTERVAL)
        break if @log_event_loop_stop
        next if @log_event_queue.empty?

        # NOTE: thead-safe of slice! depends on GVL
        events = @log_event_queue.slice!(0..-1)
        next if events.empty?

        events.each {|tag,time,record|
          begin
            @event_router.emit(tag, time, record)
          rescue => e
            $log.error "failed to emit fluentd's log event", tag: tag, event: record, error_class: e.class, error: e
          end
        }
      end
      puts 'jinchen ----- engine.rb : engineclass : log_event_loop end'
    end

    def run
      puts 'jinchen ----- engine.rb : engineclass : run start'
      begin
        start

        if @event_router.match?($log.tag)
          $log.enable_event
          @log_emit_thread = Thread.new(&method(:log_event_loop))
        end

        unless @engine_stopped
          # for empty loop
          @default_loop = Coolio::Loop.default
          @default_loop.attach Coolio::TimerWatcher.new(1, true)
          # TODO attach async watch for thread pool
          @default_loop.run
        end

        if @engine_stopped and @default_loop
          @default_loop.stop
          @default_loop = nil
        end

        puts 'jinchen ----- engine.rb : engineclass : run end'
      rescue => e
        $log.error "unexpected error", error_class: e.class, error: e
        $log.error_backtrace
      ensure
        $log.info "shutting down fluentd"
        shutdown
        if @log_emit_thread
          @log_event_loop_stop = true
          @log_emit_thread.join
        end
      end
    end

    def stop
        puts 'jinchen ----- engine.rb : engineclass : stop start'
      @engine_stopped = true
      if @default_loop
        @default_loop.stop
        @default_loop = nil
      end
      puts 'jinchen ----- engine.rb : engineclass : stop end'
      nil
    end

    def push_log_event(tag, time, record)
      puts 'jinchen ----- engine.rb : engineclass : push_log_event start'
      return if @log_emit_thread.nil?
      @log_event_queue.push([tag, time, record])
      puts 'jinchen ----- engine.rb : engineclass : push_log_event end'
    end

    private

    def start
      puts 'jinchen ----- engine.rb : engineclass : start start'
      @root_agent.start
      puts 'jinchen ----- engine.rb : engineclass : start end'
    end

    def shutdown
      puts 'jinchen ----- engine.rb : engineclass : shutdown start'
      @root_agent.shutdown
      puts 'jinchen ----- engine.rb : engineclass : shutdown end '
    end
  end

  Engine = EngineClass.new
end
