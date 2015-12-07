
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


if ARGV.length < 1
  $stderr.puts "Usage: #{$0} BUILD_NUMBER"
  exit 1
end


# Get the new version, as MAJOR.MINOR.BUILD
build_number = ARGV.shift
new_version = get_version_number(build_number)



# build the project
run_command("mvn clean")
run_command("mvn verify -DBUILD_SUFFIX=#{build_number} -P cassandra-2.0 findbugs:findbugs")



# # tag the commit so we can associate it with the release
# run_command("git tag release-#{new_version}")
# run_command("git push --tags")
