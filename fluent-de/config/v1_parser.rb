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
puts 'jinchen ### v1_parser.rb'
require 'strscan'
require 'uri'

require 'fluent/config/error'
require 'fluent/config/basic_parser'
require 'fluent/config/literal_parser'
require 'fluent/config/element'

module Fluent
  module Config
    class V1Parser < LiteralParser
      ELEMENT_NAME = /[a-zA-Z0-9_]+/

      def self.parse(data, fname, basepath = Dir.pwd, eval_context = nil)
        puts 'jinchen ----- config/vq_parser.rb : self.parse start'
        ss = StringScanner.new(data)
        ps = V1Parser.new(ss, basepath, fname, eval_context)
        ps.parse!
      end

      def initialize(strscan, include_basepath, fname, eval_context)
        puts 'jinchen ----- config/vq_parser.rb : initialize start'
        super(strscan, eval_context)
        @include_basepath = include_basepath
        @fname = fname
        puts 'jinchen ----- config/vq_parser.rb : initialize end'
      end

      def parse!
        puts 'jinchen ----- config/vq_parser.rb : parse! start'
        attrs, elems = parse_element(true, nil)
        root = Element.new('ROOT', '', attrs, elems)
        root.v1_config = true

        spacing
        unless eof?
          parse_error! "expected EOF"
        end

        puts 'jinchen ----- config/vq_parser.rb : parse! end'
        return root
      end

      ELEM_SYMBOLS = ['match', 'source', 'filter', 'system']
      RESERVED_PARAMS = %W(@type @id @label @log_level)

      def parse_element(root_element, elem_name, attrs = {}, elems = [])
        puts 'jinchen ----- config/vq_parser.rb : parse_element start'
        while true
          spacing
          if eof?
            if root_element
              break
            end
            parse_error! "expected end tag '</#{elem_name}>' but got end of file"
          end

          if skip(/\<\//)
            e_name = scan(ELEMENT_NAME)
            spacing
            unless skip(/\>/)
              parse_error! "expected character in tag name"
            end
            unless line_end
              parse_error! "expected end of line after end tag"
            end
            if e_name != elem_name
              parse_error! "unmatched end tag"
            end
            break

          elsif skip(/\</)
            e_name = scan(ELEMENT_NAME)
            spacing
            e_arg = scan_nonquoted_string(/(?:#{ZERO_OR_MORE_SPACING}\>)/)
            spacing
            unless skip(/\>/)
              parse_error! "expected '>'"
            end
            unless line_end
              parse_error! "expected end of line after tag"
            end
            e_arg ||= ''
            # call parse_element recursively
            e_attrs, e_elems = parse_element(false, e_name)
            new_e = Element.new(e_name, e_arg, e_attrs, e_elems)
            new_e.v1_config = true
            elems << new_e

          elsif root_element && skip(/(\@include|include)#{SPACING}/)
            if !prev_match.start_with?('@')
              $log.warn "'include' is deprecated. Use '@include' instead"
            end
            parse_include(attrs, elems)

          else
            k = scan_string(SPACING)
            spacing_without_comment
            if prev_match.include?("\n") # support 'tag_mapped' like "without value" configuration
              attrs[k] = ""
            else
              if k == '@include'
                parse_include(attrs, elems)
              elsif RESERVED_PARAMS.include?(k)
                v = parse_literal
                unless line_end
                  parse_error! "expected end of line"
                end
                attrs[k] = v
              else
                if k.start_with?('@')
                  if root_element || ELEM_SYMBOLS.include?(elem_name)
                    parse_error! "'@' is the system reserved prefix. Don't use '@' prefix parameter in the configuration: #{k}"
                  else
                    # TODO: This is for backward compatibility. It will throw an error in the future.
                    $log.warn "'@' is the system reserved prefix. It works in the nested configuration for now but it will be rejected: #{k}"
                  end
                end

                v = parse_literal
                unless line_end
                  parse_error! "expected end of line"
                end
                attrs[k] = v
              end
            end
          end
        end

        puts 'jinchen ----- config/vq_parser.rb : parse_element end'
        return attrs, elems
      end

      def parse_include(attrs, elems)
        puts 'jinchen ----- config/vq_parser.rb : parse_include start '
        uri = scan_string(LINE_END)
        eval_include(attrs, elems, uri)
        line_end
        puts 'jinchen ----- config/vq_parser.rb : parse_include end'
      end

      def eval_include(attrs, elems, uri)
        puts 'jinchen ----- config/vq_parser.rb : eval_include start'
        u = URI.parse(uri)
        if u.scheme == 'file' || u.path == uri  # file path
          path = u.path
          if path[0] != ?/
            pattern = File.expand_path("#{@include_basepath}/#{path}")
          else
            pattern = path
          end

          Dir.glob(pattern).sort.each { |path|
            basepath = File.dirname(path)
            fname = File.basename(path)
            data = File.read(path)
            data.force_encoding('UTF-8')
            ss = StringScanner.new(data)
            V1Parser.new(ss, basepath, fname, @eval_context).parse_element(true, nil, attrs, elems)
          }
        else
          require 'open-uri'
          basepath = '/'
          fname = path
          data = open(uri) { |f| f.read }
          data.force_encoding('UTF-8')
          ss = StringScanner.new(data)
          V1Parser.new(ss, basepath, fname, @eval_context).parse_element(true, nil, attrs, elems)
        end

        puts 'jinchen ----- config/vq_parser.rb : eval_include end'
      rescue SystemCallError => e
        cpe = ConfigParseError.new("include error #{uri}")
        cpe.set_backtrace(e.backtrace)
        raise cpe
      end

      # override
      def error_sample
        "#{@fname} #{super}"
      end
    end
  end
end
