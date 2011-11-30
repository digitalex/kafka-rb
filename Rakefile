# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'rubygems'
require 'rake/gempackagetask'
require 'rubygems/specification'
require 'date'
require 'rspec/core/rake_task'

GEM = 'kafka-rb'
GEM_NAME = 'Kafka Client'
GEM_VERSION = '0.0.7'
AUTHORS = ['Alejandro Crosa']
EMAIL = "alejandrocrosa@gmail.com"
HOMEPAGE = "http://github.com/acrosa/kafka-rb"
SUMMARY = "A Ruby client for the Kafka distributed publish/subscribe messaging service"
DESCRIPTION = "kafka-rb allows you to produce and consume messages using the Kafka distributed publish/subscribe messaging service."

spec = Gem::Specification.new do |s|
  s.name = GEM
  s.version = GEM_VERSION
  s.platform = Gem::Platform::RUBY
  s.has_rdoc = true
  s.extra_rdoc_files = ["LICENSE"]
  s.summary = SUMMARY
  s.description = DESCRIPTION
  s.authors = AUTHORS
  s.email = EMAIL
  s.homepage = HOMEPAGE
  s.add_development_dependency "rspec"
  s.require_path = 'lib'
  s.autorequire = GEM
  s.files = %w(LICENSE README.md Rakefile) + Dir.glob("{lib,tasks,spec}/**/*")
end

task :default => :spec

desc "Run specs"
RSpec::Core::RakeTask.new do |t|
  t.pattern = FileList['spec/**/*_spec.rb']
  t.rspec_opts = %w(-fs --color)
end

Rake::GemPackageTask.new(spec) do |pkg|
  pkg.gem_spec = spec
end

desc "install the gem locally"
task :install => [:package] do
  sh %{sudo gem install pkg/#{GEM}-#{GEM_VERSION}}
end

desc "create a gemspec file"
task :make_spec do
  File.open("#{GEM}.gemspec", "w") do |file|
    file.puts spec.to_ruby
  end
end

desc "Run all examples with RCov"
RSpec::Core::RakeTask.new(:rcov) do |t|
  t.pattern = FileList['spec/**/*_spec.rb']
  t.rcov = true
end
