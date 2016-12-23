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
puts 'jinchen ### config/dsl.rb'
require 'json'

require 'fluent/config'
require 'fluent/config/element'

module Fluent
  module Config
    module DSL
      module Parser
        def self.read(path)
          puts 'jinchen ----- config/dsl.rb : self.read start'
          path = File.expand_path(path)
          data = File.read(path)
          parse(data, path)
          puts 'jinchen ----- config/dsl.rb : self.read end'
        end

        def self.parse(source, source_path="config.rb")
          puts 'jinchen ----- config/dsl.rb : self.parse start'
          Proxy.new('ROOT', nil).eval(source, source_path).to_config_element
          puts 'jinchen ----- config/dsl.rb : self.parse end'
        end
      end

      class Proxy
        def initialize(name, arg)
          puts 'jinchen ----- config/dsl.rb : Proxy : initialize start'
          @element = Element.new(name, arg, self)
          puts 'jinchen ----- config/dsl.rb : Proxy : initialize end'
        end

        def element
          puts 'jinchen ----- config/dsl.rb : Proxy : element start'
          @element
          puts 'jinchen ----- config/dsl.rb : Proxy : element end'
        end

        def eval(source, source_path)
          puts 'jinchen ----- config/dsl.rb : Proxy : eval start'
          @element.instance_eval(source, source_path)
          puts 'jinchen ----- config/dsl.rb : Proxy : eval end'
          self
        end

        def to_config_element
          puts 'jinchen ----- config/dsl.rb : Proxy : to_config_element start'
          @element.instance_eval do
            Config::Element.new(@name, @arg, @attrs, @elements)
          end
          puts 'jinchen ----- config/dsl.rb : Proxy : to_config_element end'
        end

        def add_element(name, arg, block)
          puts 'jinchen ----- config/dsl.rb : Proxy : add_element start'
          ::Kernel.raise ::ArgumentError, "#{name} block must be specified" if block.nil?

          proxy = self.class.new(name.to_s, arg)
          proxy.element.instance_exec(&block)

          @element.instance_eval do
            @elements.push(proxy.to_config_element)
          end

          puts 'jinchen ----- config/dsl.rb : Proxy : add_element end'
          self
        end
      end

      class Element < BasicObject
        def initialize(name, arg, proxy)
          puts 'jinchen ----- config/dsl.rb : Element : initialize start'
          @name     = name
          @arg      = arg || ''
          @attrs    = {}
          @elements = []
          @proxy    = proxy
          puts 'jinchen ----- config/dsl.rb : Element : initialize end'
        end

        def method_missing(name, *args, &block)
          puts 'jinchen ----- config/dsl.rb : Element : method_missing start'
          ::Kernel.raise ::ArgumentError, "Configuration DSL Syntax Error: only one argument allowed" if args.size > 1
          value = args.first

          if block
            proxy = Proxy.new(name.to_s, value)
            proxy.element.instance_exec(&block)
            @elements.push(proxy.to_config_element)
          else
            @attrs[name.to_s] = if value.is_a?(Array) || value.is_a?(Hash)
                                  JSON.dump(value)
                                else
                                  value.to_s
                                end
          end

          puts 'jinchen ----- config/dsl.rb : Element : method_missing end'
          self
        end

        def source(&block)
          puts 'jinchen ----- config/dsl.rb : Element : source start'
          @proxy.add_element('source', nil, block)
          puts 'jinchen ----- config/dsl.rb : Element : source end'
        end

        def match(*args, &block)
          puts 'jinchen ----- config/dsl.rb : Element : match start'
          ::Kernel.raise ::ArgumentError, "#{name} block requires arguments for match pattern" if args.nil? || args.size != 1
          @proxy.add_element('match', args.first, block)
          puts 'jinchen ----- config/dsl.rb : Element : match end'
        end

        def self.const_missing(name)
          puts 'jinchen ----- config/dsl.rb : Element : self.const_missing start'
          return ::Kernel.const_get(name) if ::Kernel.const_defined?(name)

          if name.to_s =~ /^Fluent::Config::DSL::Element::(.*)$/
            name = "#{$1}".to_sym
            return ::Kernel.const_get(name) if ::Kernel.const_defined?(name)
          end
          ::Kernel.eval("#{name}")
          puts 'jinchen ----- config/dsl.rb : Element : self.const_missing end'
        end

        def ruby(&block)
          puts 'jinchen ----- config/dsl.rb : Element : ruby start'
          if block
            @proxy.instance_exec(&block)
          else
            ::Kernel
          end
          puts 'jinchen ----- config/dsl.rb : Element : ruby end'
        end
      end
    end
  end
end
