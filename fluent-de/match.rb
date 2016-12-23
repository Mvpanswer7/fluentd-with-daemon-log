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
puts 'jinchen ### match.rb'

module Fluent
  class Match
    def initialize(pattern_str, output)
      puts 'jinchen ----- match.rb : match : initialize start'
      patterns = pattern_str.split(/\s+/).map {|str|
        MatchPattern.create(str)
      }
      if patterns.length == 1
        @pattern = patterns[0]
      else
        @pattern = OrMatchPattern.new(patterns)
      end
      @output = output
      puts 'jinchen ----- match.rb : match : initialize end'
    end

    attr_reader :output

    def emit(tag, es)
      puts 'jinchen ----- match.rb : match : emit start'
      chain = NullOutputChain.instance
      @output.emit(tag, es, chain)
      puts 'jinchen ----- match.rb : match : emit end'
    end

    def start
      puts 'jinchen ----- match.rb : match : start start'
      @output.start
      puts 'jinchen ----- match.rb : match : start end'
    end

    def shutdown
      puts 'jinchen ----- match.rb : match : shutdown start'
      @output.shutdown
      puts 'jinchen ----- match.rb : match : shutdown end'
    end

    def match(tag)
      puts 'jinchen ----- match.rb : match : match start'
      if @pattern.match(tag)
        return true
      end
      puts 'jinchen ----- match.rb : match : match end'
      return false
    end
  end

  class MatchPattern
    def self.create(str)
      puts 'jinchen ----- match.rb : match-pattern : self.create start'
      if str == '**'
        AllMatchPattern.new
      else
        GlobMatchPattern.new(str)
      end
    end
  end

  class AllMatchPattern < MatchPattern
    def match(str)
      puts 'jinchen ----- match.rb : all-match-pattern : match'
      true
    end
  end

  class GlobMatchPattern < MatchPattern
    def initialize(pat)
      puts 'jinchen ----- match.rb : global-match-pattern : initialize start'
      stack = []
      regex = ['']
      escape = false
      dot = false

      i = 0
      while i < pat.length
        c = pat[i,1]

        if escape
          regex.last << Regexp.escape(c)
          escape = false
          i += 1
          next

        elsif pat[i,2] == "**"
          # recursive any
          if dot
            regex.last << "(?![^\\.])"
            dot = false
          end
          if pat[i+2,1] == "."
            regex.last << "(?:.*\\.|\\A)"
            i += 3
          else
            regex.last << ".*"
            i += 2
          end
          next

        elsif dot
          regex.last << "\\."
          dot = false
        end

        if c == "\\"
          escape = true

        elsif c == "."
          dot = true

        elsif c == "*"
          # any
          regex.last << "[^\\.]*"

          # TODO
          #elsif c == "["
          #  # character class
          #  chars = ''
          #  while i < pat.length
          #    c = pat[i,1]
          #    if c == "]"
          #      break
          #    else
          #      chars << c
          #    end
          #    i += 1
          #  end
          #  regex.last << '['+Regexp.escape(chars).gsub("\\-",'-')+']'

        elsif c == "{"
          # or
          stack.push []
          regex.push ''

        elsif c == "}" && !stack.empty?
          stack.last << regex.pop
          regex.last << Regexp.union(*stack.pop.map {|r| Regexp.new(r) }).to_s

        elsif c == "," && !stack.empty?
          stack.last << regex.pop
          regex.push ''

        elsif c =~ /[a-zA-Z0-9_]/
          regex.last << c

        else
          regex.last << "\\#{c}"
        end

        i += 1
      end

      until stack.empty?
        stack.last << regex.pop
        regex.last << Regexp.union(*stack.pop).to_s
      end

      @regex = Regexp.new("\\A"+regex.last+"\\Z")
    end

    def match(str)
      puts 'jinchen ----- match.rb : global-match-pattern : match'
      @regex.match(str) != nil
    end
  end

  class OrMatchPattern < MatchPattern
    def initialize(patterns)
      puts 'jinchen ----- match.rb : or-match-pattern : initialize'
      @patterns = patterns
    end

    def match(str)
      puts 'jinchen ----- match.rb : or-match-pattern : match'
      @patterns.any? {|pattern| pattern.match(str) }
    end
  end
end
