start_server {tags {"vsort"}} {
  proc setup_data {} {
    r del l:-2l:us incl excl cap anti
    r zadd l:-2l:us 5 400c 9 500v 10 100c 20 300v 30 200c 100 600v
    r hset r:100c meta r100
    r hset r:200c meta r200
    r hset r:300v meta r300
    r hset r:400c meta r400
    r hset r:500v meta r500
    r hset r:600v meta r600
  }

  proc basics {encoding} {
    if {$encoding == "ziplist"} {
        r config set zset-max-ziplist-entries 128
        r config set zset-max-ziplist-value 64
    } elseif {$encoding == "skiplist"} {
        r config set zset-max-ziplist-entries 0
        r config set zset-max-ziplist-value 0
    } else {
        puts "Unknown sorted set encoding"
        exit
    }

    test "wrong number of parameters - $encoding" {
      set err "ERR wrong number of arguments for 'vsort' command"
      assert_error $err {r vsort }
      assert_error $err {r vsort 5 }
      assert_error $err {r vsort 5 zset}
      assert_error $err {r vsort 5 zset 0}
      assert_error $err {r vsort 5 zset 0 0}
      assert_error $err {r vsort 5 zset 0 0 0}
    }

    test "wrong type for count - $encoding" {
      set err "ERR value is not an integer or out of range"
      assert_error $err {r vsort abc l:-2l:us 1 anti 1 cap 1 100v}
    }

    test "returns less results than requested if we don't have enough matches - $encoding" {
      setup_data
      assert_equal {r100 r200} [r vsort 5 l:-2l:us 1 anti 1 cap 2 100c 200c]
    }

    test "returns results when the count matches the exact number requested - $encoding" {
      setup_data
      assert_equal {r100 r200 r300} [r vsort 3 l:-2l:us 1 anti 1 cap 3 100c 200c 300v]
    }

    test "returns the results limited by most viewed - $encoding" {
      setup_data
      assert_equal {r600 r200 r300} [r vsort 3 l:-2l:us 1 anti 1 cap 6 100c 200c 300v 400c 500v 600v]
    }

    test "returns the results limited by most viewed with unranked members - $encoding" {
      setup_data
      assert_equal {r600 r200 r300} [r vsort 3 l:-2l:us 1 anti 1 cap 7 1000c 100c 200c 300v 400c 500v 600v]
    }

    test "applies holdbacks - $encoding" {
      setup_data
      r sadd cap 200c 300v
      assert_equal {r600 r100 r500} [r vsort 3 l:-2l:us 1 anti 1 cap 6 100c 200c 300v 400c 500v 600v]
    }

    test "applies anti_cap - $encoding" {
      setup_data
      r sadd cap 200c 300v
      r sadd anti 300v
      assert_equal {r600 r300 r100} [r vsort 3 l:-2l:us 1 anti 1 cap 6 100c 200c 300v 400c 500v 600v]
    }

    test "applies inclusion list - $encoding" {
      setup_data
      r sadd cap 200c 300v
      r sadd incl 200c
      assert_equal {r600 r200 r100 r500} [r vsort 4 l:-2l:us 2 anti incl 1 cap 6 100c 200c 300v 400c 500v 600v]
    }

    test "applies exclusion list - $encoding" {
      setup_data
      r sadd cap 300v
      r sadd excl 200c
      assert_equal {r600 r100 r500} [r vsort 3 l:-2l:us 1 anti 2 cap excl 6 100c 200c 300v 400c 500v 600v]
    }
  }
  basics ziplist
  basics skiplist
}
