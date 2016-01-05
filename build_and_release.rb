#!/usr/bin/ruby

# This script builds the code, overriding the version number of the pom with
# one of the form MAJOR.MINOR.BUILD. The major and minor version numbers are
# taken from the pom file (see the BLUEFLOOD_VERSION maven parameter). The
# build number should be passed in as the first and only command line argument
# to the script. The script will then turn a snapshot version string, such as
# "2.0-SNAPSHOT", into something suitable for a release build, like "2.0.36".
#
# Running maven from the command line (e.g. `mvn package`), instead of the
# script, will still produce artifacts with the SNAPSHOT version string.
#
# To produce a snapshot build of the software, run the following at the command
# line:
#   mvn package
#
# To produce a release build of the software, run the following:
#   ruby build_and_release VERSION_SUFFIX
# where VERSION_SUFFIX is a number.


require 'rexml/document'


def get_version_number(build_number)
  pom = REXML::Document.new(File.new("pom.xml"))
  version = pom.elements["project/properties/BLUEFLOOD_VERSION"].text

  if version.end_with? '${VERSION_SUFFIX}'
    return version.gsub('${VERSION_SUFFIX}', build_number)
  end

  return version
end

def run_command(command)
  puts "Running command: #{command}"
  ret = IO.popen(command, :err=>[:child, :out]) {|io|
    io.each {|line|
      puts ">> #{line}"
    }
  }
  if $?.exitstatus != 0
    puts "Failed with code #{$?.exitstatus}"
    exit 1
  end
end

def run_command_return_lines(command)
  lines = []
  ret = IO.popen(command, :err=>[:child, :out]) {|io|
    io.each {|line|
      lines << line
    }
  }
  return lines
end

def pom_evaluate(property_name)
  lines = run_command_return_lines("mvn org.apache.maven.plugins:maven-help-plugin:evaluate -Dexpression=#{property_name}")
  return lines.find {|line| not line.start_with?("[")}.chomp
end



if ARGV.length < 1
  $stderr.puts "Usage: #{$0} BUILD_NUMBER"
  exit 1
end

pom_version = pom_evaluate("project.version")
if not pom_version.end_with?("-SNAPSHOT")
  puts "The project's effective version doesn't end with \"-SNAPSHOT\""
  exit 1
end

# Get the new version, as MAJOR.MINOR.BUILD
build_number = ARGV.shift
new_version = get_version_number(build_number)



# build the project
run_command("mvn clean")
run_command("mvn verify -DBUILD_SUFFIX=#{build_number} -P cassandra-2.0 findbugs:findbugs")



# tag the commit so we can associate it with the release
run_command("git tag release-#{new_version}")
run_command("git push --tags")
