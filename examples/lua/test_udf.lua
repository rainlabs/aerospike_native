local function get_number(rec)
        return rec['number']
end

local function add(a, b)
        return a + b
end

function sum_number(stream)
        return stream : map(get_number) : reduce(add)
end

function add_testbin_to_number(rec)
        rec['number'] = rec['number'] + rec['testbin'];
        aerospike:update(rec)
end
