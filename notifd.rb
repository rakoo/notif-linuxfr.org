# encoding: utf-8
require 'redis'
require 'rest_client'
require 'cgi'

redis = Redis.connect
pub = RestClient::Resource.new('pubsubhubbub.appspot.com', :headers => 
                               {:content_type => 'application/x-www-form-urlencoded'})

lua_script = <<-EOF
  local ret = redis.call('SMEMBERS', KEYS[0])
  redis.call('DEL', KEYS[0])
  return ret
EOF

loop do

  # Fetch urls. The script is used to empty the set atomically
  begin
    urls = redis.eval(lua_script, ["updated_atom"], [])
  rescue Redis::CommandError
    # No scripting, use old-style transactions
    urls, null = redis.multi do
      redis.smembers "updated_atom"
      redis.del "updated_atom"
    end
  end

  unless urls.empty?

    query = urls.map do |url|
      "hub.mode=publish&hub.url=#{CGI.escape(url)}"
    end.join("&")

    pub.post(query) do |response|

      case response.code
      when 204
        puts "Successfully notified hub for #{urls}"

      else
        puts "Uh oh, something broke: #{response.inspect}"

        # Put back the urls, to be treated by someone else
        redis.sadd "updated_atom", urls

        # do not "blpop", begin the loop back from the beginning
        next
      end

    end

  end

  # We can pop the anchor, which will block until a new url is added
  redis.blpop "updated_atom_blocker"

end
