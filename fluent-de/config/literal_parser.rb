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

puts 'jinchen ### config/literal_parser.rb'
require 'stringio'

require 'json'
require 'yajl'
require 'irb/ruby-lex'  # RubyLex

require 'fluent/config/basic_parser'

module Fluent
  module Config
    class LiteralParser < BasicParser
      def self.unescape_char(c)
        case c
        when '"'
          '\"'
        when "'"
          "\\'"
        when '\\'
          '\\\\'
        when "\r"
          '\r'
        when "\n"
          '\n'
        when "\t"
          '\t'
        when "\f"
          '\f'
        when "\b"
          '\b'
        else
          c
        end
      end

      def initialize(strscan, eval_context)
        puts 'jinchen ----- config/literal_parser.rb : initialize start'
        super(strscan)
        @eval_context = eval_context
        puts 'jinchen ----- config/literal_parser.rb : initialize end'
      end

      def parse_literal(string_boundary_charset = LINE_END)
        puts 'jinchen ----- config/literal_parser.rb : parse_literal start'
        spacing_without_comment

        value = if skip(/\[/)
                  scan_json(true)
                elsif skip(/\{/)
                  scan_json(false)
                else
                  scan_string(string_boundary_charset)
                end
        puts 'jinchen ----- config/literal_parser.rb : parse_literal end'
        value
      end

      def scan_string(string_boundary_charset = LINE_END)
        puts 'jinchen ----- config/literal_parser.rb : scan_string start'
        if skip(/\"/)
          return scan_double_quoted_string
        elsif skip(/\'/)
          return scan_single_quoted_string
        else
          return scan_nonquoted_string(string_boundary_charset)
        end
        puts 'jinchen ----- config/literal_parser.rb : scan_string end'
      end

      def scan_double_quoted_string
        puts 'jinchen ----- config/literal_parser.rb : scan_double_quoted_string start'
        string = []
        while true
          if skip(/\"/)
            return string.join
          elsif check(/[^"]#{LINE_END_WITHOUT_SPACING_AND_COMMENT}/)
            if s = check(/[^\\]#{LINE_END_WITHOUT_SPACING_AND_COMMENT}/)
              string << s
            end
            skip(/[^"]#{LINE_END_WITHOUT_SPACING_AND_COMMENT}/)
          elsif s = scan(/\\./)
            string << eval_escape_char(s[1,1])
          elsif skip(/\#\{/)
            string << eval_embedded_code(scan_embedded_code)
            skip(/\}/)
          elsif s = scan(/./)
            string << s
          else
            parse_error! "unexpected end of file in a double quoted string"
          end
        end
        puts 'jinchen ----- config/literal_parser.rb : scan_double_quoted_string end'
      end

      def scan_single_quoted_string
        puts 'jinchen ----- config/literal_parser.rb : scan_single_quoted_string start'
        string = []
        while true
          if skip(/\'/)
            return string.join
          elsif s = scan(/\\'/)
            string << "'"
          elsif s = scan(/\\\\/)
            string << "\\"
          elsif s = scan(/./)
            string << s
          else
            parse_error! "unexpected end of file in a signle quoted string"
          end
        end
        puts 'jinchen ----- config/literal_parser.rb : scan_single_quoted_string end'
      end

      def scan_nonquoted_string(boundary_charset = LINE_END)
        puts 'jinchen ----- config/literal_parser.rb : scan_noquoted_string start'
        charset = /(?!#{boundary_charset})./

        string = []
        while true
          if s = scan(/\#/)
            string << '#'
          elsif s = scan(charset)
            string << s
          else
            break
          end
        end

        if string.empty?
          return nil
        end

        puts 'jinchen ----- config/literal_parser.rb : scan_noquoted_string end'
        string.join
      end

      def scan_embedded_code
        puts 'jinchen ----- config/literal_parser.rb : scan_embedded_code start'
        rlex = RubyLex.new
        src = '"#{'+@ss.rest+"\n=end\n}"

        input = StringIO.new(src)
        input.define_singleton_method(:encoding) { external_encoding }
        rlex.set_input(input)

        tk = rlex.token
        code = src[3,tk.seek-3]

        if @ss.rest.length < code.length
          @ss.pos += @ss.rest.length
          parse_error! "expected end of embedded code but $end"
        end

        @ss.pos += code.length

        puts 'jinchen ----- config/literal_parser.rb : scan_embedded_code end'
        '"#{' + code + '}"'
        
      end

      def eval_embedded_code(code)
        puts 'jinchen ----- config/literal_parser.rb : eval_embedded_code start'
        if @eval_context.nil?
          parse_error! "embedded code is not allowed in this file"
        end
        @eval_context.instance_eval(code)
        puts 'jinchen ----- config/literal_parser.rb : eval_embedded_code end'
      end

      def eval_escape_char(c)
        puts 'jinchen ----- config/literal_parser.rb : eval_escape_char start'
        case c
        when '"'
          '"'
        when "'"
          "'"
        when "r"
          "\r"
        when "n"
          "\n"
        when "t"
          "\t"
        when "f"
          "\f"
        when "b"
          "\b"
        when /[a-zA-Z0-9]/
          parse_error! "unexpected back-slash escape character '#{c}'"
        else  # symbols
          c
        end
        puts 'jinchen ----- config/literal_parser.rb : eval_escape_char end'
      end

      def scan_json(is_array)
        puts 'jinchen ----- config/literal_parser.rb : scan_json start'
        result = nil
        # Yajl does not raise ParseError for imcomplete json string, like '[1', '{"h"', '{"h":' or '{"h1":1'
        # This is the reason to use JSON module.

        buffer = (is_array ? "[" : "{")
        line_buffer = ""

        until result
          char = getch

          break if char.nil?

          if char == "#"
            # If this is out of json string literals, this object can be parsed correctly
            # '{"foo":"bar", #' -> '{"foo":"bar"}' (to check)
            parsed = nil
            begin
              parsed = JSON.parse(buffer + line_buffer.rstrip.sub(/,$/, '') + (is_array ? "]" : "}"))
            rescue JSON::ParserError => e
              # This '#' is in json string literals
            end

            if parsed
              # ignore chars as comment before newline
              while (char = getch) != "\n"
                # ignore comment char
              end
              buffer << line_buffer + "\n"
              line_buffer = ""
            else
              # '#' is a char in json string
              line_buffer << char
            end

            next # This char '#' MUST NOT terminate json object.
          end

          if char == "\n"
            buffer << line_buffer + "\n"
            line_buffer = ""
            next
          end

          line_buffer << char
          begin
            result = JSON.parse(buffer + line_buffer)
          rescue JSON::ParserError => e
            # Incomplete json string yet
          end
        end

        unless result
          parse_error! "got incomplete JSON #{is_array ? 'array' : 'hash'} configuration"
        end

        puts 'jinchen ----- config/literal_parser.rb : scan_json end'
        JSON.dump(result)
      end
    end
  end
end
