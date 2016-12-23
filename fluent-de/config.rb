#
# Fluent
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

puts 'jinchen ### buffer.rb'
require 'fluent/config/error'
require 'fluent/config/element'
require 'fluent/configurable'

module Fluent
  module Config
    def self.parse(str, fname, basepath = Dir.pwd, v1_config = false)
      puts 'jinchen ----- config.rb : self.parse start'
      if fname =~ /\.rb$/
        require 'fluent/config/dsl'
        Config::DSL::Parser.parse(str, File.join(basepath, fname))
      else
        if v1_config
          require 'fluent/config/v1_parser'
          V1Parser.parse(str, fname, basepath, Kernel.binding)
        else
          require 'fluent/config/parser'
          Parser.parse(str, fname, basepath)
        end
      end
    end

    def self.new(name = '')
      puts 'jinchen ----- config.rb : self.new start'
      Element.new(name, '', {}, [])
      puts 'jinchen ----- config.rb : self.new end'
    end
  end

  module PluginId
    def configure(conf)
      puts 'jinchen ----- config.rb : configure start'
      @id = conf['@id'] || conf['id']
      super
      puts 'jinchen ----- config.rb : configure end'
    end

    def plugin_id
      puts 'jinchen ----- config.rb : plugin_id start'
      @id ? @id : "object:#{object_id.to_s(16)}"
      puts 'jinchen ----- config.rb : plugin_id end'
    end
  end
end
