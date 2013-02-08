start_server {tags {"vsort"}} {
  proc setup_data {} {
    r del cap anti l:-2l:video:us l:-2l:container:us
    r zadd l:-2l:video:us 20 300v 9 500v 100 600v
    r zadd l:-2l:container:us 10 100c 30 200c 5 400c
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
      assert_error $err {r vsort cap }
      assert_error $err {r vsort cap anti}
      assert_error $err {r vsort cap anti 5}
      assert_error $err {r vsort cap anti 5 6}
    }

    test "wrong type for count - $encoding" {
      set err "ERR value is not an integer or out of range"
      assert_error $err {r vsort cap anti abc l:-2l:video:us l:-2l:container:us 100v}
    }

    test "cap is not a set - $encoding" {
      r set cap over9000
      set err "ERR Operation against a key holding the wrong kind of value"
      assert_error $err {r vsort cap anti 5 l:-2l:video:us l:-2l:container:us 100v}
    }

    test "anti is not a set - $encoding" {
      r set anti under
      set err "ERR Operation against a key holding the wrong kind of value"
      assert_error $err {r vsort cap anti 5 l:-2l:video:us l:-2l:container:us 100v}
    }

    test "returns less results than requested if we don't have enough matches - $encoding" {
      setup_data
      assert_equal {r100 r200} [r vsort cap anti 5 l:-2l:video:us l:-2l:container:us 100c 200c]
    }

    test "returns results when the count matches the exact number requested - $encoding" {
      setup_data
      assert_equal {r100 r200 r300} [r vsort cap anti 3 l:-2l:video:us l:-2l:container:us 100c 200c 300v]
    }

    test "returns the results limited by most viewed - $encoding" {
      setup_data
      assert_equal {r600 r200 r300} [r vsort cap anti 3 l:-2l:video:us l:-2l:container:us 100c 200c 300v 400c 500v 600v]
    }

    test "returns the results limited by most viewed with unranked members - $encoding" {
      setup_data
      assert_equal {r600 r200 r300} [r vsort cap anti 3 l:-2l:video:us l:-2l:container:us 1000c 100c 200c 300v 400c 500v 600v]
    }

    test "applies holdbacks - $encoding" {
      setup_data
      r sadd cap 200c 300v
      assert_equal {r600 r100 r500} [r vsort cap anti 3 l:-2l:video:us l:-2l:container:us 100c 200c 300v 400c 500v 600v]
    }

    test "applies anti_cap - $encoding" {
      setup_data
      r sadd cap 200c 300v
      r sadd anti 300v
      assert_equal {r600 r300 r100} [r vsort cap anti 3 l:-2l:video:us l:-2l:container:us 100c 200c 300v 400c 500v 600v]
    }
  }
  basics ziplist
  basics skiplist
}
