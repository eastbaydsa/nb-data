require 'bundler/inline'

gemfile do
 source 'https://rubygems.org'
  gem 'activesupport'
  gem 'dotenv'
  gem 'nationbuilder-rb', require: 'nationbuilder'
end

require 'active_support/all'

Dotenv.load
api_token = ENV['NATION_API_TOKEN']
nation = ENV['NATION_NAME']
client = NationBuilder::Client.new(nation, api_token)

# Config
RAPID_RESPONSE_LABOR = 'rapid_response_labor'

# "Get all members who have signed up for labor rapid response
puts 'Fetching members...'
members_result = client.call(:people_tags, :people, tag: RAPID_RESPONSE_LABOR, limit: 100)
members_page = NationBuilder::Paginator.new(client, members_result)

loop do
  members_page.body['results'].each do |m|
    update = false
    if m.key?('delete_mobile')
      m['mobile'] = nil
      puts 'deleting mobile...'
      puts m['first_name']
      client.call(:people, :update, person:m, id:m['id'])
    end
  end
  if members_page.next?
    members_page = members_page.next
  else
    break
  end
end
