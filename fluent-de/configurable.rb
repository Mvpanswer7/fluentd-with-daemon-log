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

puts 'jinchen ### configurable.rb'
require 'fluent/config/configure_proxy'
require 'fluent/config/section'
require 'fluent/config/error'
require 'fluent/registry'
require 'fluent/plugin'
require 'fluent/mixin'

module Fluent
  module Configurable
    def self.included(mod)
      puts 'jinchen ----- configurable.rb : self.included'
      mod.extend(ClassMethods)
    end

    def initialize
      puts 'jinchen ----- configurable.rb : initialize start'
      # to simulate implicit 'attr_accessor' by config_param / config_section and its value by config_set_default
      proxy = self.class.merged_configure_proxy
      proxy.params.keys.each do |name|
        if proxy.defaults.has_key?(name)
          instance_variable_set("@#{name}".to_sym, proxy.defaults[name])
        end
      end
      proxy.sections.keys.each do |name|
        subproxy = proxy.sections[name]
        if subproxy.multi?
          instance_variable_set("@#{subproxy.param_name}".to_sym, [])
        else
          instance_variable_set("@#{subproxy.param_name}".to_sym, nil)
        end
      end
      puts 'jinchen ----- configurable.rb : initialize end'
    end

    def configure(conf)
      puts 'jinchen ----- configurable.rb : configure start'
      @config = conf

      logger = self.respond_to?(:log) ? log : $log
      proxy = self.class.merged_configure_proxy
      conf.corresponding_proxies << proxy

      # In the nested section, can't get plugin class through proxies so get plugin class here
      plugin_class = Fluent::Plugin.lookup_name_from_class(proxy.name.to_s)
      root = Fluent::Config::SectionGenerator.generate(proxy, conf, logger, plugin_class)
      @config_root_section = root

      root.instance_eval{ @params.keys }.each do |param_name|
        varname = "@#{param_name}".to_sym
        if (! root[param_name].nil?) || instance_variable_get(varname).nil?
          instance_variable_set(varname, root[param_name])
        end
      end

      puts 'jinchen ----- configurable.rb : configure end'
      self
    end

    def config
      puts 'jinchen ----- configurable.rb : config start'
      @masked_config ||= @config.to_masked_element
      puts 'jinchen ----- configurable.rb : config end'
    end

    CONFIG_TYPE_REGISTRY = Registry.new(:config_type, 'fluent/plugin/type_')

    def self.register_type(type, callable = nil, &block)
      puts 'jinchen ----- configurable.rb : register_type start'
      puts type
      callable ||= block
      CONFIG_TYPE_REGISTRY.register(type, callable)
      puts 'jinchen ----- configurable.rb : register_type end'
    end

    def self.lookup_type(type)
      puts 'jinchen ----- configurable.rb : lookup_type start'
      CONFIG_TYPE_REGISTRY.lookup(type)
    end

    module ClassMethods
      def configure_proxy_map
        puts 'jinchen ----- configurable.rb : classmethod : configure_proxy_map start'
        map = {}
        self.define_singleton_method(:configure_proxy_map){ map }
        puts 'jinchen ----- configurable.rb : classmethod : configure_proxy_map end'
        map
      end

      def configure_proxy(mod_name)
        puts 'jinchen ----- configurable.rb : classmethod : configure_proxy start'
        map = configure_proxy_map
        unless map[mod_name]
          proxy = Fluent::Config::ConfigureProxy.new(mod_name, required: true, multi: false)
          map[mod_name] = proxy
        end
        puts 'jinchen ----- configurable.rb : classmethod : configure_proxy end'
        map[mod_name]
      end

      def config_param(name, *args, &block)
        puts 'jinchen ----- configurable.rb : classmethod : config_param start '
        configure_proxy(self.name).config_param(name, *args, &block)
        attr_accessor name
        puts 'jinchen ----- configurable.rb : classmethod : config_param end'
      end

      def config_set_default(name, defval)
        puts 'jinchen ----- configurable.rb : classmethod : config_set_default start '
        configure_proxy(self.name).config_set_default(name, defval)
        puts 'jinchen ----- configurable.rb : classmethod : config_set_default end'
      end

      def config_set_desc(name, desc)
        puts 'jinchen ----- configurable.rb : classmethod : config_set_desc start '
        configure_proxy(self.name).config_set_desc(name, desc)
        puts 'jinchen ----- configurable.rb : classmethod : config_set_desc end'
      end

      def config_section(name, *args, &block)
        puts 'jinchen ----- configurable.rb : classmethod : config_section start '
        configure_proxy(self.name).config_section(name, *args, &block)
        attr_accessor configure_proxy(self.name).sections[name].param_name
        puts 'jinchen ----- configurable.rb : classmethod : config_section end'
      end

      def desc(description)
        puts 'jinchen ----- configurable.rb : classmethod : desc start '
        configure_proxy(self.name).desc(description)
        puts 'jinchen ----- configurable.rb : classmethod : desc end'
      end

      def merged_configure_proxy
        puts 'jinchen ----- configurable.rb : classmethod : merged_configure_proxy start '
        configurables = ancestors.reverse.select{ |a| a.respond_to?(:configure_proxy) }

        # 'a.object_id.to_s' is to support anonymous class
        #   which created in tests to overwrite original behavior temporally
        #
        # p Module.new.name   #=> nil
        # p Class.new.name    #=> nil
        # p AnyGreatClass.dup.name #=> nil
        configurables.map{ |a| a.configure_proxy(a.name || a.object_id.to_s) }.reduce(:merge)
      end

      def dump(level = 0)
        puts 'jinchen ----- configurable.rb : classmethod : dump start '
        configure_proxy_map[self.to_s].dump(level)
        puts 'jinchen ----- configurable.rb : classmethod : dump end'
      end
    end
  end

  # load default types
  require 'fluent/config/types'
end
