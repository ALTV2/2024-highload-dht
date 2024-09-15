function create_random_value(min_length, max_length)
    local charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    local length = math.random(min_length, max_length)
    local random_string = {}

    for i = 1, length do
        local random_index = math.random(1, #charset)
        random_string[i] = charset:sub(random_index, random_index)
    end

    return table.concat(random_string)
end

request = function()
    local headers = {}
    headers["Host"] = "localhost"
    local id = math.random(1, 1000000)
    local body = create_random_value(50, 70)
    return wrk.format("PUT", "/v0/entity?id=" .. id, headers, body)
end