start_server {tags {"vsort"}} {
  proc setup_data {} {
    r del l:-2l:us incl excl cap anti r:100c r:200c r:300v r:400c r:500v r:600v
    r zadd l:-2l:us 5 400c 9 500v 10 100c 20 300v 30 200c 100 600v
    r hset r:100c meta "{\"r\":100}"
    r hset r:200c meta "{\"r\":200}"
    r hset r:300v meta "{\"r\":300}"
    r hset r:400c meta "{\"r\":400}"
    r hset r:500v meta "{\"r\":500}"
    r hset r:600v meta "{\"r\":600}"
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
      set expected {{{"id":"100c","blocked":false}} {{"id":"200c","blocked":false}}}
      assert_equal $expected [r vsort 5 l:-2l:us 1 anti 1 cap 2 100c 200c]
    }

    test "returns results when the count matches the exact number requested - $encoding" {
      setup_data
      set expected {{{"id":"100c","blocked":false}} {{"id":"200c","blocked":false}} {{"id":"300v","blocked":false}}}
      assert_equal $expected [r vsort 3 l:-2l:us 1 anti 1 cap 3 100c 200c 300v]
    }

    test "returns the results limited by most viewed - $encoding" {
      setup_data
      set expected {{{"id":"600v","blocked":false}} {{"id":"200c","blocked":false}} {{"id":"300v","blocked":false}}}
      assert_equal $expected [r vsort 3 l:-2l:us 1 anti 1 cap 6 100c 200c 300v 400c 500v 600v]
    }

    test "returns the results limited by most viewed with unranked members - $encoding" {
      setup_data
      set expected {{{"id":"600v","blocked":false}} {{"id":"200c","blocked":false}} {{"id":"300v","blocked":false}}}
      assert_equal $expected [r vsort 3 l:-2l:us 1 anti 1 cap 7 1000c 100c 200c 300v 400c 500v 600v]
    }

    test "applies holdbacks - $encoding" {
      setup_data
      r sadd cap 200c 300v
      set expected {{{"id":"600v","blocked":false}} {{"id":"100c","blocked":false}} {{"id":"500v","blocked":false}}}
      assert_equal $expected [r vsort 3 l:-2l:us 1 anti 1 cap 6 100c 200c 300v 400c 500v 600v]
    }

    test "applies anti_cap - $encoding" {
      setup_data
      r sadd cap 200c 300v
      r sadd anti 300v
      set expected {{{"id":"600v","blocked":false}} {{"id":"300v","blocked":false}} {{"id":"100c","blocked":false}}}
      assert_equal $expected [r vsort 3 l:-2l:us 1 anti 1 cap 6 100c 200c 300v 400c 500v 600v]
    }

    test "applies inclusion list - $encoding" {
      setup_data
      r sadd cap 200c 300v
      r sadd incl 200c
      set expected {{{"id":"600v","blocked":false}} {{"id":"200c","blocked":false}} {{"id":"100c","blocked":false}} {{"id":"500v","blocked":false}}}
      assert_equal $expected [r vsort 4 l:-2l:us 2 anti incl 1 cap 6 100c 200c 300v 400c 500v 600v]
    }

    test "applies exclusion list - $encoding" {
      setup_data
      r sadd cap 300v
      r sadd excl 200c
      set expected {{{"id":"600v","blocked":false}} {{"id":"100c","blocked":false}} {{"id":"500v","blocked":false}}}
      assert_equal $expected [r vsort 3 l:-2l:us 1 anti 2 cap excl 6 100c 200c 300v 400c 500v 600v]
    }
  }
  basics ziplist
  basics skiplist
}
