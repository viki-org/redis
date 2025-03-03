-- quicksort.lua

--[[
 * Three-way partition function for quicksort
 *
 * Partitions an array into three sections:
 *   - Elements smaller than the pivot
 *   - Elements equal to the pivot
 *   - Elements greater than the pivot
 *
 * Parameters:
 *   arr (table) - The list to be partitioned
 *   from (number) - Start index of the partition
 *   to (number) - End index of the partition
 *   pivot_val (any) - Pivot element used for partitioning
 *   less_than (function) - Comparator function that determines ordering
 *
 * Returns:
 *   (number, number) - Indices marking the end of the "smaller" section
 *                      and the beginning of the "larger" section.
 ]]
local function three_way_partition(arr, from, to, pivot_val, less_than)
    local i = from
    local j = from
    local k = to
    while j <= k do
        if less_than(arr[j], pivot_val) then
            arr[i], arr[j] = arr[j], arr[i]
            i, j = i + 1, j + 1
        elseif less_than(pivot_val, arr[j]) then
            arr[j], arr[k] = arr[k], arr[j]
            k = k - 1
        else
            j = j + 1
        end
    end
    return i - 1, k + 1
end


--[[
 * Quicksort function
 *
 * Sorts a list in place using a recursive three-way quicksort algorithm.
 *
 * Parameters:
 *   list (table) - The table (list) to be sorted
 *   less_than (function) [optional] - Comparator function that determines sorting order
 *   choose_pivot (function) [optional] - Function to choose a pivot index
 *
 * Returns:
 *   None (modifies the input table in place)
 ]]
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
