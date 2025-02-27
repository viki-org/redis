-- quicksort.lua

-- Three-way partition function
local function three_way_partition(arr, from, to, pivot_val, less_than)
    local i = from -- start index of equal values; everything below is smaller
    local j = from -- start index of unpartitioned values
    local k = to -- start index (exclusive) of larger values
    while j <= k do -- while there are unpartitioned values
        if less_than(arr[j], pivot_val) then -- smaller
            arr[i], arr[j] = arr[j], arr[i] -- swap value to low partition
            i, j = i + 1, j + 1
        elseif less_than(pivot_val, arr[j]) then -- greater
            arr[j], arr[k] = arr[k], arr[j] -- swap value to high partition
            k = k - 1
        else -- equal
            j = j + 1
        end
    end
    return i - 1, k + 1 -- return end index of smaller values and start index of larger values
end

-- Quicksort function
local function quicksort(list, less_than, choose_pivot)
    less_than = less_than or function(a, b)
        return a < b
    end
    choose_pivot = choose_pivot or math.random

    local function sort(from, to)
        if from >= to then
            return
        end
        local pivot_val = list[choose_pivot(from, to)]
        local smaller_to, larger_from = three_way_partition(list, from, to, pivot_val, less_than)
        if smaller_to - from < to - larger_from then
            sort(from, smaller_to)
            sort(larger_from, to)
        else
            sort(larger_from, to)
            sort(from, smaller_to)
        end
    end

    sort(1, #list)
end

-- Return the quicksort function as a module
return {
    quicksort = quicksort
}
