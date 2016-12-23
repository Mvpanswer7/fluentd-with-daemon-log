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

puts 'jinchen ### event_router.rb'
require 'fluent/match'
require 'fluent/event'

module Fluent
  #
  # EventRouter is responsible to route events to a collector.
  #
  # It has a list of MatchPattern and Collector pairs:
  #
  #  +----------------+     +-----------------+
  #  |  MatchPattern  |     |    Collector    |
  #  +----------------+     +-----------------+
  #  |   access.**  ---------> type forward   |
  #  |     logs.**  ---------> type copy      |
  #  |  archive.**  ---------> type s3        |
  #  +----------------+     +-----------------+
  #
  # EventRouter does:
  #
  # 1) receive an event at `#emit` methods
  # 2) match the event's tag with the MatchPatterns
  # 3) forward the event to the corresponding Collector
  #
  # Collector is either of Output, Filter or other EventRouter.
  #
  class EventRouter
    def initialize(default_collector, emit_error_handler)
      puts 'jinchen ----- event_router.rb : initialize start'
      @match_rules = []
      @match_cache = MatchCache.new
      @default_collector = default_collector
      @emit_error_handler = emit_error_handler
      @chain = NullOutputChain.instance
      puts 'jinchen ----- event_router.rb : initialize end'
    end

    attr_accessor :default_collector
    attr_accessor :emit_error_handler

    class Rule
      def initialize(pattern, collector)
        puts 'jinchen ----- event_router.rb : rule : initialize start'
        patterns = pattern.split(/\s+/).map { |str| MatchPattern.create(str) }
        @pattern = if patterns.length == 1
                     patterns[0]
                   else
                     OrMatchPattern.new(patterns)
                   end
        @pattern_str = pattern
        @collector = collector
        puts 'jinchen ----- event_router.rb : rule : initialize end'
      end

      def match?(tag)
        puts 'jinchen ----- event_router.rb : rule : match?'
        @pattern.match(tag)
      end

      attr_reader :collector
      attr_reader :pattern_str
    end

    # called by Agent to add new match pattern and collector
    def add_rule(pattern, collector)
      puts 'jinchen ----- event_router.rb : rule : add_rule start'
      @match_rules << Rule.new(pattern, collector)
      puts 'jinchen ----- event_router.rb : add_rule end'
    end

    def emit(tag, time, record)
        puts 'jinchen ----- event_router.rb : rule : emit start'
      unless record.nil?
        emit_stream(tag, OneEventStream.new(time, record))
      end
      puts 'jinchen ----- event_router.rb : emit end'
    end

    def emit_array(tag, array)
        puts 'jinchen ----- event_router.rb : rule : emit_array start'
      emit_stream(tag, ArrayEventStream.new(array))
      puts 'jinchen ----- event_router.rb : emit_array end'
    end

    def emit_stream(tag, es)
        puts 'jinchen ----- event_router.rb : rule : emit_stream start'
      match(tag).emit(tag, es, @chain)
      puts 'jinchen ----- event_router.rb : emit_stream end'
    rescue => e
      @emit_error_handler.handle_emits_error(tag, es, e)
    end

    def emit_error_event(tag, time, record, error)
        puts 'jinchen ----- event_router.rb : rule : emit_error_event start'
      @emit_error_handler.emit_error_event(tag, time, record, error)
      puts 'jinchen ----- event_router.rb : emit_error_event end'
    end

    def match?(tag)
      puts 'jinchen ----- event_router.rb : match?'
      !!find(tag)
    end

    def match(tag)
        puts 'jinchen ----- event_router.rb : rule : match start'
      collector = @match_cache.get(tag) {
        c = find(tag) || @default_collector
      }
      collector
      puts 'jinchen ----- event_router.rb : match end'
    end

    class MatchCache
      MATCH_CACHE_SIZE = 1024

      def initialize
        puts 'jinchen ----- event_router.rb : match-cache : initialize start'
        super
        @map = {}
        @keys = []
        puts 'jinchen ----- event_router.rb : matchcache : initialize end'
      end

      def get(key)
        puts 'jinchen ----- event_router.rb : match-cache : get start'
        if collector = @map[key]
          return collector
        end
        collector = @map[key] = yield
        if @keys.size >= MATCH_CACHE_SIZE
          # expire the oldest key
          @map.delete @keys.shift
        end
        @keys << key
        collector
        puts 'jinchen ----- event_router.rb : matchcache : get end'
      end
    end

    private

    class Pipeline
      def initialize
        puts 'jinchen ----- event_router.rb : pipeline: initialize start'
        @filters = []
        @output = nil
        puts 'jinchen ----- event_router.rb : pipeline: initialize end'
      end

      def add_filter(filter)
        puts 'jinchen ----- event_router.rb : pipeline: add_filter start'
        @filters << filter
        puts 'jinchen ----- event_router.rb : pipeline: add_filter end'
      end

      def set_output(output)
        puts 'jinchen ----- event_router.rb : pipeline: set_output start'
        @output = output
        puts 'jinchen ----- event_router.rb : pipeline: set_output end'
      end

      def emit(tag, es, chain)
        puts 'jinchen ----- event_router.rb : pipeline: emit start'
        processed = es
        @filters.each { |filter|
          processed = filter.filter_stream(tag, processed)
        }
        @output.emit(tag, processed, chain)
        puts 'jinchen ----- event_router.rb : pipeline: emit end'
      end
    end

    def find(tag)
        puts 'jinchen ----- event_router.rb : pipeline: find start'
      pipeline = nil
      @match_rules.each_with_index { |rule, i|
        if rule.match?(tag)
          if rule.collector.is_a?(Filter)
            pipeline ||= Pipeline.new
            pipeline.add_filter(rule.collector)
          else
            if pipeline
              pipeline.set_output(rule.collector)
            else
              # Use Output directly when filter is not matched
              pipeline = rule.collector
            end
            return pipeline
          end
        end
      }

      if pipeline
        # filter is matched but no match
        pipeline.set_output(@default_collector)
        pipeline
      else
        nil
      end
      puts 'jinchen ----- event_router.rb : pipeline: find end'
    end
  end
end
