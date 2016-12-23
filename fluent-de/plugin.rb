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

puts 'jinchen ### plugin.rb'
require 'fluent/config/error'

module Fluent
  class PluginClass
    # This class is refactored using Fluent::Registry at v0.14

    def initialize
      puts 'jinchen ----- plugin.rb : initialize'
      @input = {}
      @output = {}
      @filter = {}
      @buffer = {}
    end

    def register_input(type, klass)
      puts 'jinchen ----- plugin.rb : register_input start'
      register_impl('input', @input, type, klass)
      puts 'jinchen ----- plugin.rb : register_input end'
    end

    def register_output(type, klass)
      puts 'jinchen ----- plugin.rb : register_output start'
      register_impl('output', @output, type, klass)
      puts 'jinchen ----- plugin.rb : register_output end'
    end

    def register_filter(type, klass)
      puts 'jinchen ----- plugin.rb : register_filter start'
      register_impl('filter', @filter, type, klass)
      puts 'jinchen ----- plugin.rb : register_filter end'
    end

    def register_buffer(type, klass)
      puts 'jinchen ----- plugin.rb : register_buffer start'
      register_impl('buffer', @buffer, type, klass)
      puts 'jinchen ----- plugin.rb : register_buffer end'
    end

    def register_parser(type, klass)
      puts 'jinchen ----- plugin.rb : register_parser start'
      TextParser.register_template(type, klass)
      puts 'jinchen ----- plugin.rb : register_parser end'
    end

    def register_formatter(type, klass)
      puts 'jinchen ----- plugin.rb : register_formatter start'
      TextFormatter.register_template(type, klass)
      puts 'jinchen ----- plugin.rb : register_formatter end'
    end

    def new_input(type)
      puts 'jinchen ----- plugin.rb : new_input use new_impl'
      new_impl('input', @input, type)
    end

    def new_output(type)
      puts 'jinchen ----- plugin.rb : new_output use new_impl'
      new_impl('output', @output, type)
    end

    def new_filter(type)
      puts 'jinchen ----- plugin.rb : new_filter use new_impl'
      new_impl('filter', @filter, type)
    end

    def new_buffer(type)
      puts 'jinchen ----- plugin.rb : new_buffer use new_impl'
      new_impl('buffer', @buffer, type)
    end

    def new_parser(type)
      puts 'jinchen ----- plugin.rb : new_parser start'
      require 'fluent/parser'
      TextParser.lookup(type)
    end

    def new_formatter(type)
      puts 'jinchen ----- plugin.rb : new_formatter start'
      require 'fluent/formatter'
      TextFormatter.lookup(type)
    end

    def load_plugins
      puts 'jinchen ----- plugin.rb : load_plugins start'
      dir = File.join(File.dirname(__FILE__), "plugin")
      load_plugin_dir(dir)
      puts 'jinchen ----- plugin.rb : load_plugins end'
    end

    def load_plugin_dir(dir)
      puts 'jinchen ----- plugin.rb : load_plugin_dir start'
      dir = File.expand_path(dir)
      Dir.entries(dir).sort.each {|fname|
        if fname =~ /\.rb$/
          require File.join(dir, fname)
        end
      }
      puts 'jinchen ----- plugin.rb : load_plugin_dir end'
      nil
    end

    def load_plugin(type, name)
      puts 'jinchen ----- plugin.rb : load_plugin start'
      try_load_plugin(name, type)
      puts 'jinchen ----- plugin.rb : load_plugin end'
    end

    def lookup_name_from_class(klass_or_str)
      puts 'jinchen ----- plugin.rb : lookup_name_from_class start'
      klass = if klass_or_str.class == String
                eval(klass_or_str) # const_get can't handle A::B
              else
                klass_or_str
              end

      @input.each { |name, plugin|
        return name if plugin == klass
      }
      @output.each { |name, plugin|
        return name if plugin == klass
      }
      @filter.each { |name, plugin|
        return name if plugin == klass
      }

      puts 'jinchen ----- plugin.rb : lookup_name_from_class end'
      nil
    end

    private
    def register_impl(name, map, type, klass)
      puts 'jinchen ----- plugin.rb : register_impl start'
      map[type] = klass
      $log.trace { "registered #{name} plugin '#{type}'" }
      puts 'jinchen ----- plugin.rb : register_impl end'
      nil
    end

    def new_impl(name, map, type)
      puts 'jinchen ----- plugin.rb : new_impl start'
      if klass = map[type]
        return klass.new
      end
      try_load_plugin(name, type)
      if klass = map[type]
        return klass.new
      end
      puts 'jinchen ----- plugin.rb : new_impl end'
      raise ConfigError, "Unknown #{name} plugin '#{type}'. Run 'gem search -rd fluent-plugin' to find plugins"
    end

    def try_load_plugin(name, type)
      puts 'jinchen ----- plugin.rb : try_load_plugin start'
      case name
      when 'input'
        path = "fluent/plugin/in_#{type}"
      when 'output'
        path = "fluent/plugin/out_#{type}"
      when 'filter'
        path = "fluent/plugin/filter_#{type}"
      when 'buffer'
        path = "fluent/plugin/buf_#{type}"
      else
        return
      end

      # prefer LOAD_PATH than gems
      files = $LOAD_PATH.map {|lp|
        lpath = File.join(lp, "#{path}.rb")
        File.exist?(lpath) ? lpath : nil
      }.compact
      unless files.empty?
        # prefer newer version
        require File.expand_path(files.sort.last)
        return
      end

      # search gems
      specs = Gem::Specification.find_all { |spec|
        spec.contains_requirable_file? path
      }

      # prefer newer version
      specs = specs.sort_by { |spec| spec.version }
      if spec = specs.last
        spec.require_paths.each { |lib|
          file = "#{spec.full_gem_path}/#{lib}/#{path}"
          require file
        }
        puts 'jinchen ----- plugin.rb : try_load_plugin end'
      end
    end
  end

  Plugin = PluginClass.new
end
