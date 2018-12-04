require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'activesupport'
  gem 'nationbuilder-rb', require: 'nationbuilder'
end

require 'active_support/all'

api_token = ENV['TOKEN']
nation = ENV['NATION']
client = NationBuilder::Client.new(nation, api_token)

# Config
MEMBER_TAG = ENV['MEMBER_TAG'] || 'national_member'
COUNTED_TAG_PREFIX = %w[
  meeting_general_
  meeting_new_member_
  meeting_campaign_
  education_
  meeting_housing_committee_
  canvassers_
]

puts 'Fetching members...'

# "For each committee, create a list of EBDSA members who have attended two or more committee meetings in the past 12 months.
members_result = client.call(:people_tags, :people, tag: MEMBER_TAG, limit: 100)
members_page = NationBuilder::Paginator.new(client, members_result)

# You can identify membership by seeing if their member ship (named 'National' in NB) status is 'Active'
members = []
loop do
  members_page.body['results'].each do |m|
    members << m
  end

  if members_page.next?
    members_page = members_page.next
  else
    break
  end
end

puts 'Checking member activity...'

active = []
members.each do |m|
  tags = m['tags'].select do |tag|
    tag.end_with?('18') &&
    tag.match?(Regexp.union(COUNTED_TAG_PREFIX.map { |prefix| /#{prefix}.*/ }))
  end

  meeting = m['tags'].any? { |tag| tag.start_with? 'meeting_general_' }

  next unless meeting && tags.count >= 5

  active << {
    id: m['id'],
    score: tags.count,
    email: m['email'],
    first_name: m['first_name'],
    last_name: m['last_name']
  }
end

puts 'Adding active members to list...'

list = client.call(
  :lists,
  :create,
  list: {
    slug: "very_active_members",
    author_id: 671 # this is Dominic's ID
  }
)
list_id = list['list_resource']['id']

# Add committee members to lists
client.call(:lists, :add_people, list_id: list_id, people_ids: active.map{|u| u[:id]})
