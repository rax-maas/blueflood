
require 'rexml/document'


def get_new_pom_version(build_number)
  pom = REXML::Document.new(File.new("pom.xml"))
  version = pom.elements["project/version"].text

  unless version =~ /-SNAPSHOT$/
    raise "Not a snapshot version: #{version}"
  end

  return version.gsub(/-SNAPSHOT$/, ".#{build_number}")
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

next_build_number = ARGV.shift
new_version = get_new_pom_version(next_build_number)

# create a branch so we can commit the updated poms

branch_name = "release-branch-#{new_version}"
run_command("git checkout -b #{branch_name}")

# run the versions:set goal to update all poms

run_command("mvn versions:set -DnewVersion=#{new_version} -DgenerateBackupPoms=false")

# build the project

run_command("mvn clean")
run_command("mvn verify")

# commit the changes

run_command("git commit -a -m \"Release #{new_version}\"")

# create a release tag and push it to the repo

run_command("git tag izrik-release-#{new_version}")
run_command("git push --tags")
