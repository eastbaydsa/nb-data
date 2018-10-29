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
RAPID_RESPONSE_LABOR = 'rapid_response_labor'

# "Get all members who have signed up for labor rapid response
puts 'Fetching members...'
members_result = client.call(:people_tags, :people, tag: RAPID_RESPONSE_LABOR, limit: 100)
members_page = NationBuilder::Paginator.new(client, members_result)

loop do
  members_page.body['results'].each do |m|
    update = false
    if (m['phone'].nil? || m['phone'].empty?) && !(m['mobile'].nil? || m['mobile'].empty?)
      m['phone'] = m['mobile']
      update = true
    elsif (m['mobile'].nil? || m['mobile'].empty?) && !(m['phone'].nil? || m['phone'].empty?)
      m['mobile'] = m['phone']
      update = true
    end
    if update
      puts 'updating...'
      puts m['first_name']
      client.call(:people, :update, person:m, id:m['id'])
      puts 'updated'
    end
  end
  if members_page.next?
    members_page = members_page.next
  else
    break
  end
end
