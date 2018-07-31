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
MEETING_NAMES = %i[electoral housing labor].freeze
COMMITTEE_RULES = {
  default: {
    required_meetings: 1,
    max_months_since: 12
  },
  labor: {
    required_meetings: 1,
    max_months_since: 36
  },
  housing: {
    required_meetings: 1,
    max_months_since: 12
  },
  electoral: {
    required_meetings: 1,
    max_months_since: 12
  }
}.freeze

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

# You can identify meeting attendance tags of the following structure: 'meeting_[committee_name]_[MMDDYY].
puts "Building membership roster..."
committees = {}
members.each do |m|
  meeting_tags = m['tags'].select do |tag|
    tag.match? Regexp.union(MEETING_NAMES.map { |name| /meeting_#{name}.*_[0-9]{6}/ })
  end

  next if meeting_tags.empty?
  MEETING_NAMES.each do |name|
    tags = meeting_tags.select { |tag| tag.match? /meeting_#{name}.*_[0-9]{6}/ }
    dates = tags.map do |tag|
      date_string = tag.split('_').last
      Date.parse("#{date_string[4, 2]}-#{date_string[0, 2]}-#{date_string[2, 2]}") # Format MMDDYY -> actual date
    end

    rules = COMMITTEE_RULES[name] || COMMITTEE_RULES[:default]
    earliest_date = rules[:max_months_since].months.ago
    count = dates.select { |d| d > earliest_date }.count

    next unless count >= rules[:required_meetings]
    committees[name] ||= []
    committees[name] << m['id']
  end
end

# Fetch all lists from NB to remove old member rolls
puts "Fetching lists..."
lists_result = client.call(:lists, :index, limit: 100)
lists_page = NationBuilder::Paginator.new(client, lists_result)
all_lists = []
loop do
  lists_page.body['results'].each do |l|
    all_lists << l
  end
  if lists_page.next?
    lists_page = lists_page.next
  else
    break
  end
end

# You should assign these lists to the committee's chairs and secretary. You can identify them by the tags
# 'committee_[committee name]_chair' and 'committee_[committee name]_secretary'
committees.each do |committee_name, members|
  list_date = Date.today.strftime('%m/%d/%y')
  officers = client.call(:people_tags, :people, tag: "committee_#{committee_name}_chair", limit: 100)['results']
  begin
    officers += client.call(:people_tags, :people, tag: "committee_#{committee_name}_secretary", limit: 100)['results']
  rescue
    # No secretary for committee
  end

  # Clean up old lists + tags
  existing_lists = all_lists.select { |l| l['slug'].starts_with?("mbrs_#{committee_name}") }
  existing_lists.each_with_index do |l, i|
    puts "Removing existing list: #{l['id']} (#{l['slug']})"
    unless i > 0
      client.call(:lists, :delete_tag, list_id: l['id'], tag: "member_#{committee_name}")
    end
    client.call(:lists, :destroy, id: l['id'])
  end

  # Use a standard naming system for the lists: mbrs_[committee name]_[MMDDYY]. Use the date of creattion.
  # Append the list name with '_i' for n = 1 .. . .n where n is the number of chairs and secretaries
  officers.each_with_index do |o, i|
    i += 1
    puts "Assigning committee #{committee_name} list #{i} to #{o['email']}"

    list = client.call(
      :lists,
      :create,
      list: {
        name: "#{committee_name.capitalize} Committee Membership #{list_date} (#{i})",
        slug: "mbrs_#{committee_name}_#{i}",
        author_id: o['id']
      }
    )
    list_id = list['list_resource']['id']

    # Add committee members to lists
    client.call(:lists, :add_people, list_id: list_id, people_ids: members)

    unless i > 0
      # Tag each of the members as 'member_[committee_name]'
      client.call(:lists, :add_tag, list_id: list_id, tag: "member_#{committee_name}")
    end
  end
end
