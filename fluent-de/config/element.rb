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

require 'fluent/config/error'
require 'fluent/config/literal_parser'

module Fluent
  module Config
    class Element < Hash
      def initialize(name, arg, attrs, elements, unused = nil)
        puts 'jinchen ----- config/element.rb : initialize start'
        @name = name
        @arg = arg
        @elements = elements
        super()
        attrs.each { |k, v|
          self[k] = v
        }
        @unused = unused || attrs.keys
        @v1_config = false
        @corresponding_proxies = [] # some plugins use flat parameters, e.g. in_http doesn't provide <format> section for parser.
        @unused_in = false # if this element is not used in plugins, correspoing plugin name and parent element name is set, e.g. [source, plugin class].
        puts 'jinchen ----- config/element.rb : initialize end'
      end

      attr_accessor :name, :arg, :elements, :unused, :v1_config, :corresponding_proxies, :unused_in

      def add_element(name, arg='')
        puts 'jinchen ----- config/element.rb : add_element start  '
        e = Element.new(name, arg, {}, [])
        e.v1_config = @v1_config
        @elements << e
        puts 'jinchen ----- config/element.rb : add_element end'
        e
      end

      def inspect
        puts 'jinchen ----- config/element.rb : inspect start'
        attrs = super
        "name:#{@name}, arg:#{@arg}, " + attrs + ", " + @elements.inspect
        puts 'jinchen ----- config/element.rb : inspect end'
      end

      # This method assumes _o_ is an Element object. Should return false for nil or other object
      def ==(o)
        puts 'jinchen ----- config/element.rb : ==(o) start'
        self.name == o.name && self.arg == o.arg &&
          self.keys.size == o.keys.size &&
          self.keys.reduce(true){|r, k| r && self[k] == o[k] } &&
          self.elements.size == o.elements.size &&
          [self.elements, o.elements].transpose.reduce(true){|r, e| r && e[0] == e[1] }
        puts 'jinchen ----- config/element.rb : ==(o) end'
      end

      def +(o)
        puts 'jinchen ----- config/element.rb : +(o) start'
        e = Element.new(@name.dup, @arg.dup, o.merge(self), @elements + o.elements, (@unused + o.unused).uniq)
        e.v1_config = @v1_config
        puts 'jinchen ----- config/element.rb : +(o) end'
        e
      end

      def each_element(*names, &block)
        puts 'jinchen ----- config/element.rb : each_element start'
        if names.empty?
          @elements.each(&block)
        else
          @elements.each { |e|
            if names.include?(e.name)
              block.yield(e)
            end
          }
        end
        puts 'jinchen ----- config/element.rb : each_element end'
      end

      def has_key?(key)
        puts 'jinchen ----- config/element.rb : has_key? start'
        @unused_in = false # some sections, e.g. <store> in copy, is not defined by config_section so clear unused flag for better warning message in chgeck_not_fetched.
        @unused.delete(key)
        puts 'jinchen ----- config/element.rb : has_key? end'
        super
      end

      def [](key)
        puts 'jinchen ----- config/element.rb : [] start'
        @unused_in = false # ditto
        @unused.delete(key)
        puts 'jinchen ----- config/element.rb : [] end'
        super
      end

      def check_not_fetched(&block)
        puts 'jinchen ----- config/element.rb : check_not_fetched start'
        each_key { |key|
          if @unused.include?(key)
            block.call(key, self)
          end
        }
        @elements.each { |e|
          e.check_not_fetched(&block)
        }
        puts 'jinchen ----- config/element.rb : check_not_fetched end'
      end

      def to_s(nest = 0)
        puts 'jinchen ----- config/element.rb : to_s start'
        indent = "  " * nest
        nindent = "  " * (nest + 1)
        out = ""
        if @arg.empty?
          out << "#{indent}<#{@name}>\n"
        else
          out << "#{indent}<#{@name} #{@arg}>\n"
        end
        each_pair { |k, v|
          if secret_param?(k)
            out << "#{nindent}#{k} xxxxxx\n"
          else
            out << "#{nindent}#{k} #{v}\n"
          end
        }
        @elements.each { |e|
          out << e.to_s(nest + 1)
        }
        out << "#{indent}</#{@name}>\n"
        puts 'jinchen ----- config/element.rb : to_s end'
        out
      end

      def to_masked_element
        puts 'jinchen ----- config/element.rb : to_masked_element start'
        new_elems = @elements.map { |e| e.to_masked_element }
        new_elem = Element.new(@name, @arg, {}, new_elems, @unused)
        each_pair { |k, v|
          new_elem[k] = secret_param?(k) ? 'xxxxxx' : v
        }
        puts 'jinchen ----- config/element.rb : to_masked_element end'
        new_elem
      end

      def secret_param?(key)
        puts 'jinchen ----- config/element.rb : secret_param? start'
        return false if @corresponding_proxies.empty?

        param_key = key.to_sym
        @corresponding_proxies.each { |proxy|
          block, opts = proxy.params[param_key]
          if opts && opts.has_key?(:secret)
            return opts[:secret]
          end
        }

        puts 'jinchen ----- config/element.rb : secret_param? end'
        false
      end

      def self.unescape_parameter(v)
        puts 'jinchen ----- config/element.rb : self.unescape_param start'
        result = ''
        v.each_char { |c| result << LiteralParser.unescape_char(c) }
        puts 'jinchen ----- config/element.rb : self.unescape_param end'
        result
      end
    end
  end
end
