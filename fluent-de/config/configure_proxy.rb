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
puts 'jinchen ### config/configure_proxy'

require 'fluent/configurable'

module Fluent
  module Config
    class ConfigureProxy
      attr_accessor :name, :final, :param_name, :required, :multi, :alias, :argument, :params, :defaults, :descriptions, :sections
      # config_param :desc, :string, :default => '....'
      # config_set_default :buffer_type, :memory
      #
      # config_section :default, required: true, multi: false do
      #   config_argument :arg, :string
      #   config_param :required, :bool, default: false
      #   config_param :name, :string
      #   config_param :power, :integer
      # end
      #
      # config_section :child, param_name: 'children', required: false, multi: true, alias: 'node' do
      #   config_param :name, :string
      #   config_param :power, :integer, default: nil
      #   config_section :item do
      #     config_param :name
      #   end
      # end

      def initialize(name, opts = {})
        puts 'jinchen ----- config/config_proxy.rb : initialize start'
        @name = name.to_sym
        @final = opts.fetch(:final, false)

        @param_name = (opts[:param_name] || @name).to_sym
        @required = opts[:required]
        @multi = opts[:multi]
        @alias = opts[:alias]

        @argument = nil # nil: ignore argument
        @params = {}
        @defaults = {}
        @descriptions = {}
        @sections = {}
        puts 'jinchen ----- config/config_proxy.rb : initialize end'
      end

      def required?
        puts 'jinchen ----- config/config_proxy.rb : required? start '
        @required.nil? ? false : @required
        puts 'jinchen ----- config/config_proxy.rb : required? end'
      end

      def multi?
        puts 'jinchen ----- config/config_proxy.rb : multi? start '
        @multi.nil? ? true : @multi
        puts 'jinchen ----- config/config_proxy.rb : multi? end'
      end

      def final?
        puts 'jinchen ----- config/config_proxy.rb : final? start '
        @final
        puts 'jinchen ----- config/config_proxy.rb : final? end'
      end

      def merge(other) # self is base class, other is subclass
        puts 'jinchen ----- config/config_proxy.rb : merge start '
        return merge_for_finalized(other) if self.final?

        options = {
          param_name: other.param_name,
          required: (other.required.nil? ? self.required : other.required),
          multi: (other.multi.nil? ? self.multi : other.multi)
        }
        merged = self.class.new(other.name, options)

        merged.argument = other.argument || self.argument
        merged.params = self.params.merge(other.params)
        merged.defaults = self.defaults.merge(other.defaults)
        merged.sections = {}
        (self.sections.keys + other.sections.keys).uniq.each do |section_key|
          self_section = self.sections[section_key]
          other_section = other.sections[section_key]
          merged_section = if self_section && other_section
                             self_section.merge(other_section)
                           elsif self_section || other_section
                             self_section || other_section
                           else
                             raise "BUG: both of self and other section are nil"
                           end
          merged.sections[section_key] = merged_section
        end

        puts 'jinchen ----- config/config_proxy.rb : merge end'
        merged
      end

      def merge_for_finalized(other)
        puts 'jinchen ----- config/config_proxy.rb : merge_for_finalized start '
        # list what subclass can do for finalized section
        #  * overwrite param_name to escape duplicated name of instance variable
        #  * append params/defaults/sections which are missing in superclass

        options = {
          param_name: other.param_name,
          required: (self.required.nil? ? other.required : self.required),
          multi: (self.multi.nil? ? other.multi : self.multi),
          final: true,
        }
        merged = self.class.new(other.name, options)

        merged.argument = self.argument || other.argument
        merged.params = other.params.merge(self.params)
        merged.defaults = other.defaults.merge(self.defaults)
        merged.sections = {}
        (self.sections.keys + other.sections.keys).uniq.each do |section_key|
          self_section = self.sections[section_key]
          other_section = other.sections[section_key]
          merged_section = if self_section && other_section
                             other_section.merge(self_section)
                           elsif self_section || other_section
                             self_section || other_section
                           else
                             raise "BUG: both of self and other section are nil"
                           end
          merged.sections[section_key] = merged_section
        end

        puts 'jinchen ----- config/config_proxy.rb : merge_for_finalized end'
        merged
      end

      def parameter_configuration(name, *args, &block)
        puts 'jinchen ----- config/config_proxy.rb : parameter_configurartion start '
        name = name.to_sym

        opts = {}
        args.each { |a|
          if a.is_a?(Symbol)
            opts[:type] = a
          elsif a.is_a?(Hash)
            opts.merge!(a)
          else
            raise ArgumentError, "#{self.name}: wrong number of arguments (#{1 + args.length} for #{block ? 2 : 3})"
          end
        }

        type = opts[:type]
        if block && type
          raise ArgumentError, "#{self.name}: both of block and type cannot be specified"
        end

        begin
          type = :string if type.nil?
          block ||= Configurable.lookup_type(type)
        rescue ConfigError
          # override error message
          raise ArgumentError, "#{self.name}: unknown config_argument type `#{type}'"
        end

        if opts.has_key?(:default)
          config_set_default(name, opts[:default])
        end

        if opts.has_key?(:desc)
          config_set_desc(name, opts[:desc])
        end

        puts 'jinchen ----- config/config_proxy.rb : parameter_configurartion end'
        [name, block, opts]
      end

      def config_argument(name, *args, &block)
        puts 'jinchen ----- config/config_proxy.rb : config_argument start '
        if @argument
          raise ArgumentError, "#{self.name}: config_argument called twice"
        end
        name, block, opts = parameter_configuration(name, *args, &block)

        @argument = [name, block, opts]
        puts 'jinchen ----- config/config_proxy.rb : config_argument end'
        name
      end

      def config_param(name, *args, &block)
        puts 'jinchen ----- config/config_proxy.rb : config_param start '
        name, block, opts = parameter_configuration(name, *args, &block)

        if @current_description
          config_set_desc(name, @current_description)
          @current_description = nil
        end

        @sections.delete(name)
        @params[name] = [block, opts]
        puts 'jinchen ----- config/config_proxy.rb : config_param end'
        name
      end

      def config_set_default(name, defval)
        puts 'jinchen ----- config/config_proxy.rb : config_set_default start '
        name = name.to_sym

        if @defaults.has_key?(name)
          raise ArgumentError, "#{self.name}: default value specified twice for #{name}"
        end

        @defaults[name] = defval
        puts 'jinchen ----- config/config_proxy.rb : config_set_default end'
        nil
      end

      def config_set_desc(name, description)
        puts 'jinchen ----- config/config_proxy.rb : config_set desc start '
        name = name.to_sym

        if @descriptions.has_key?(name)
          raise ArgumentError, "#{self.name}: description specified twice for #{name}"
        end

        @descriptions[name] = description
        puts 'jinchen ----- config/config_proxy.rb : config_set desc end'
        nil
      end

      def desc(description)
        puts 'jinchen ----- config/config_proxy.rb : desc start'
        @current_description = description
        puts 'jinchen ----- config/config_proxy.rb : desc end'
      end

      def config_section(name, *args, &block)
        puts 'jinchen ----- config/config_proxy.rb : config_section start '
        unless block_given?
          raise ArgumentError, "#{self.name}: config_section requires block parameter"
        end
        name = name.to_sym

        opts = {}
        unless args.empty? || args.size == 1 && args.first.is_a?(Hash)
          raise ArgumentError, "#{self.name}: unknown config_section arguments: #{args.inspect}"
        end

        sub_proxy = ConfigureProxy.new(name, *args)
        sub_proxy.instance_exec(&block)

        @params.delete(name)
        @sections[name] = sub_proxy

        puts 'jinchen ----- config/config_proxy.rb : config_section end'
        name
      end

      def dump(level = 0)
        puts 'jinchen ----- config/config_proxy.rb : dump start '
        dumped_config = ""
        indent = " " * level
        @params.each do |name, config|
          dumped_config << "#{indent}#{name}: #{config[1][:type]}: <#{@defaults[name].inspect}>"
          dumped_config << " # #{@descriptions[name]}" if @descriptions[name]
          dumped_config << "\n"
        end
        @sections.each do |section_name, sub_proxy|
          dumped_config << "#{indent}#{section_name}\n#{sub_proxy.dump(level + 1)}"
        end
        puts 'jinchen ----- config/config_proxy.rb : dump end'
        dumped_config
      end
    end
  end
end

