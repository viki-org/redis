start_server {tags {"vfind"}} {
  proc setup_data {} {
    r del zset filter1 filter2 cap r:a r:v r:c r:d r:e r:f r:g r:mnm anti incl excl

    r zadd zset 1 a 10 v 3 c 15 d 5 e 6 f 7 g 0 mnm
    r hset r:a details "{\"s_a\":1}"
    r hset r:v details "{\"s_v\":1}"
    r hset r:c details "{\"s_c\":1}"
    r hset r:d details "{\"s_d\":1}"
    r hset r:e details "{\"s_e\":1}"
    r hset r:f details "{\"s_f\":1}"
    r hset r:g details "{\"s_g\":1}"
    r hset r:mnm details "{\"good_stuff\":1}"

    r sadd filter1 v c d f g h i o u mnm
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

    test "vfind parameter number - $encoding" {
      set err "ERR wrong number of arguments for 'vfind' command"
      assert_error $err {r vfind }
      assert_error $err {r vfind list}
      assert_error $err {r vfind list 0}
      assert_error $err {r vfind list 0 10}
      assert_error $err {r vfind list 0 10 1000}
      assert_error $err {r vfind list 0 10 1000 desc}
      assert_error $err {r vfind list 0 10 1000 desc noblocked}
      assert_error $err {r vfind list 0 10 1000 desc noblocked 0}
      assert_error $err {r vfind list 0 10 1000 desc noblocked 0 0}
      assert_error $err {r vfind list 0 10 1000 desc noblocked 1 0}
      assert_error $err {r vfind list 0 10 1000 desc noblocked 0 1}
      assert_error $err {r vfind list 0 10 1000 desc noblocked 0 0 0}
    }

    test "vfind invalid offset count of up_to - $encoding" {
      setup_data
      set err "ERR value is not an integer or out of range"
      assert_error $err {r vfind zset a 10 1000 desc noblocked details 0 0 0}
      assert_error $err {r vfind zset 0 b 1000 desc noblocked details 0 0 0}
      assert_error $err {r vfind zset 0 10 c desc noblocked details 0 0 0}
    }

    test "vfind invalid list type - $encoding" {
      set err "ERR Operation against a key holding the wrong kind of value"
      r sadd myset a v c d e f g mnm
      assert_error $err {r vfind myset 0 10 1000 desc noblocked details 0 0 0}
    }

    test "vfind returns empty for a missing list - $encoding" {
      assert_equal {0} [r vfind x 0 10 1000 desc noblocked details 0 0 0]
    }

    test "vfind filter and holdback descending - $encoding" {
      setup_data
      r sadd cap a c f z y mnm
      set expected {{{"s_d":1,"blocked":false}} {{"s_v":1,"blocked":false}} {{"s_g":1,"blocked":false}} 3}
      assert_equal $expected [r vfind zset 0 10 11 desc noblocked details 0 1 cap 1 filter1]
    }

    test "vfind filter holdback ascending $encoding" {
      setup_data
      r sadd cap a c f z y
      set expected {{{"good_stuff":1,"blocked":false}} {{"s_g":1,"blocked":false}} {{"s_v":1,"blocked":false}} {{"s_d":1,"blocked":false}} 4}
      assert_equal $expected [r vfind zset 0 10 11 asc noblocked details 0 1 cap 1 filter1]
    }

    test "vfind filter all with a non existing filter - $encoding" {
      setup_data
      r sadd cap a
      assert_equal {0} [r vfind zset 0 10 11 asc noblocked details 0 1 cap 1 filter2]
    }

    test "vfind filter all with a highly restrictive filter - $encoding" {
      setup_data
      r sadd filter2 z
      r sadd cap a
      assert_equal {0} [r vfind zset 0 10 11 asc noblocked details 0 1 cap 1 filter2]
    }

    test "vfind no holdbacks - $encoding" {
      setup_data
      set expected {{{"s_d":1,"blocked":false}} {{"s_v":1,"blocked":false}} {{"s_g":1,"blocked":false}} {{"s_f":1,"blocked":false}} {{"s_c":1,"blocked":false}} {{"good_stuff":1,"blocked":false}} 6}
      assert_equal $expected [r vfind zset 0 10 11 desc noblocked details 0 0 1 filter1]
    }

    test "vfind holdback all - $encoding" {
      setup_data
      r sadd cap d v g f c mnm
      assert_equal {0} [r vfind zset 0 10 11 desc noblocked details 0 1 cap 1 filter1]
    }

    test "vfind mulitple filters - $encoding" {
      setup_data
      r sadd filter2 d g u y x o p
      r sadd cap d v
      assert_equal {{{"s_g":1,"blocked":false}} 1} [r vfind zset 0 10 11 desc noblocked details 0 1 cap 2 filter1 filter2]
    }

    test "vfind mulitple filters 2 - $encoding" {
      setup_data
      r sadd filter2 d g u
      r sadd cap u a j
      assert_equal {{{"s_d":1,"blocked":false}} {{"s_g":1,"blocked":false}} 2} [r vfind zset 0 10 11 desc noblocked details 0 1 cap 2 filter1 filter2]
    }

    test "vfind mulitple filters 3 - $encoding" {
      setup_data
      r sadd filter2 d v g
      r sadd cap d
      assert_equal {{{"s_g":1,"blocked":false}} {{"s_v":1,"blocked":false}} 2} [r vfind zset 0 10 11 asc noblocked details 0 1 cap 2 filter1 filter2]
    }

    test "vfind mulitple filters 4 - $encoding" {
      setup_data
      r sadd filter2 d v g
      r sadd cap d
      assert_equal {{{"s_v":1,"blocked":false}} {{"s_g":1,"blocked":false}} 2} [r vfind zset 0 10 11 desc noblocked details 0 1 cap 2 filter1 filter2]
    }

    test "vfind paging offset - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {{{"s_f":1,"blocked":false}} {{"s_c":1,"blocked":false}} 5} [r vfind zset 2 2 10 desc noblocked details 0 1 cap 1 filter1]
    }

    test "vfind paging offset 2 - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {{{"s_g":1,"blocked":false}} {{"s_f":1,"blocked":false}} 5} [r vfind zset 1 2 10 desc noblocked details 0 1 cap 1 filter1]
    }

    test "vfind paging offset ascending - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {{{"s_a":1,"blocked":false}} {{"s_c":1,"blocked":false}} 7} [r vfind zset 1 2 10 asc noblocked details 0 1 cap 0]
    }

    test "vfind anti cap - $encoding" {
      setup_data
      r sadd cap c f g
      r sadd anti c g
      set expected {{{"good_stuff":1,"blocked":false}} {{"s_c":1,"blocked":false}} {{"s_g":1,"blocked":false}} {{"s_v":1,"blocked":false}} {{"s_d":1,"blocked":false}} 5}
      assert_equal $expected [r vfind zset 0 10 10 asc noblocked details 1 anti 1 cap 1 filter1]
    }

    test "vfind upto limit - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {{{"s_f":1,"blocked":false}} {{"s_e":1,"blocked":false}} 4} [r vfind zset 2 2 3 desc noblocked details 0 1 cap 0]
    }

    test "bricks 1 - $encoding" {
      r del zset r:1b r:3v cap
      r zadd a_zset 1 1b 10 3v
      r hmset r:1b details "{\"name\":\"1b_details\"}" resource_id 3v
      r hmset r:3v details "{\"name\":\"3v_details\"}"
      r sadd cap a v c 3v
      set expected {{{"name":"1b_details", "resource": {"name":"3v_details"},"blocked":false}} 1}
      assert_equal $expected [r vfind a_zset 0 10 10 desc noblocked details 0 1 cap 0]
    }

    test "bricks 2 - $encoding" {
      r del zset r:1b r:3v cap
      r zadd a_zset 1 1b 10 3v
      r hmset r:1b details "{\"name\":\"1b_details\"}"
      r hmset r:3v details "{\"name\":\"3v_details\"}"
      r sadd cap a v c 3v
      set expected {{{"name":"1b_details","blocked":false}} 1}
      assert_equal $expected [r vfind a_zset 0 10 10 desc noblocked details 0 1 cap 0]
    }

    test "bricks 3 - $encoding" {
      r del zset r:1b r:3v cap
      r zadd a_zset 1 1b 10 3v
      r hmset r:1b details "{\"name\":\"1b_details\"}" resource_id 3v
      r sadd cap a v c 3v
      set expected {{{"name":"1b_details","blocked":false}} 1}
      assert_equal $expected [r vfind a_zset 0 10 10 desc noblocked details 0 1 cap 0]
    }

    test "vfind get specific field - $encoding" {
      setup_data
      r hset r:a en "{\"sa_en\":2}"
      r hset r:c en "{\"sc_en\":2}"
      r hset r:d en "{\"sd_en\":2}"
      r hset r:e en "{\"se_en\":2}"
      r hset r:f en "{\"sf_en\":2}"
      r hset r:g en "{\"sg_en\":2}"
      set expected {{{"sd_en":2,"blocked":false}} {{"sg_en":2,"blocked":false}} {{"sf_en":2,"blocked":false}} {{"sc_en":2,"blocked":false}} 4}
      assert_equal $expected [r vfind zset 0 10 10 desc noblocked en 0 0 1 filter1]
    }

    test "vfind applies inclusion list - $encoding" {
      setup_data
      r sadd cap a c f z y mnm
      r sadd incl f c
      set expected {{{"s_d":1,"blocked":false}} {{"s_v":1,"blocked":false}} {{"s_g":1,"blocked":false}} {{"s_f":1,"blocked":false}} {{"s_c":1,"blocked":false}} 5}
      assert_equal $expected [r vfind zset 0 10 11 excl noblocked details 1 incl 1 cap 1 filter1]
    }

    test "vfind applies exclusion list - $encoding" {
      setup_data
      r sadd cap a c f z y mnm
      r sadd excl v d
      assert_equal {{{"s_g":1,"blocked":false}} 1} [r vfind zset 0 10 11 desc noblocked details 0 2 cap excl 1 filter1]
    }

    test "vfind applies include blocked list using vfindZWithFilters - $encoding" {
      setup_data
      r sadd cap v x
      set expected {{{"s_v":1,"blocked":true}} {{"s_g":1,"blocked":false}} 6}
      assert_equal $expected [r vfind zset 1 2 10 desc withblocked details 0 1 cap 1 filter1]
    }

    test "vfind applies include blocked list using vfindWithFilters - $encoding" {
      setup_data
      r sadd filter2 d v g
      r sadd cap d
      set expected {{{"s_g":1,"blocked":false}} {{"s_v":1,"blocked":false}} {{"s_d":1,"blocked":true}} 3}
      assert_equal $expected [r vfind zset 0 10 11 asc withblocked details 0 1 cap 2 filter1 filter2]
    }

    test "ignores missing allows set - $encoding" {
      setup_data
      r sadd filter2 d v g
      set expected {{{"s_g":1,"blocked":false}} {{"s_v":1,"blocked":false}} {{"s_d":1,"blocked":false}} 3}
      assert_equal $expected [r vfind zset 0 10 11 asc withblocked details 1 anti 0 2 filter1 filter2]
    }

    test "ignores missing block set - $encoding" {
      setup_data
      r sadd filter2 d v g
      set expected {{{"s_g":1,"blocked":false}} {{"s_v":1,"blocked":false}} {{"s_d":1,"blocked":false}} 3}
      assert_equal $expected [r vfind zset 0 10 11 asc withblocked details 0 1 cap 2 filter1 filter2]
    }
  }
  basics ziplist
  basics skiplist
}
