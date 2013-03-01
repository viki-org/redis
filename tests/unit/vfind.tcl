start_server {tags {"vfind"}} {
  proc setup_data {} {
    r del zset filter1 filter2 cap r:a r:v r:c r:d r:e r:f r:g r:mnm anti

    r zadd zset 1 a 10 v 3 c 15 d 5 e 6 f 7 g 0 mnm
    r hset r:a details s_a
    r hset r:v details s_v
    r hset r:c details s_c
    r hset r:d details s_d
    r hset r:e details s_e
    r hset r:f details s_f
    r hset r:g details s_g
    r hset r:mnm details good_stuff

    r sadd filter1 v c d f g h i o u mnm

    #zset and filter1 have d v g f c in common

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
    test "vdiff params 1 - $encoding" {
      set err "ERR wrong number of arguments for 'vfind' command"
      assert_error $err {r vfind }
      assert_error $err {r vfind ds }
      assert_error $err {r vfind a v}
      assert_error $err {r vfind a v 0 desc a}
      assert_error $err {r vfind a v 0 desc a v}
      assert_error $err {r vfind a v 0 0 desc a v}
    }

    test "vdiff params 2 - $encoding" {
      set err "ERR value is not an integer or out of range"
      assert_error $err {r vfind a v x a x 0 10 0}
      assert_error $err {r vfind a v x 0 desc a 0 0}
      assert_error $err {r vfind a v x 0 desc 0 a 0}
    }

    test "vdiff empty zset - $encoding" {
      r sadd cap a c f z y mnm
      assert_equal {0} [r vfind x cap 0 0 desc 0 10 10]
    }

    test "vdiff invalid cap - $encoding" {
      setup_data
      set err "ERR Operation against a key holding the wrong kind of value"
      r set fail over9000
      assert_error $err {r vfind zset fail 0 0 desc 0 10 10}
    }

    test "vdiff invalid offset count of up_to - $encoding" {
      set err "ERR value is not an integer or out of range"
      assert_error $err {r vfind zset cap 0 0 desc a 10 10}
      assert_error $err {r vfind zset cap 0 0 desc 0 v 10}
      assert_error $err {r vfind zset cap 0 0 desc 0 0 v}
    }

    test "vdiff 1 - $encoding" {
      setup_data
      r sadd cap a c f z y mnm
      assert_equal {s_d s_v s_g 3} [r vfind zset cap 0 1 filter1 desc 0 10 10]
    }

    test "vdiff 2 - $encoding" {
      setup_data
      r sadd cap a c f z y
      assert_equal {good_stuff s_g s_v s_d 4} [r vfind zset cap anti 1 filter1 asc 0 10 10]
    }

    test "vdiff 3 - $encoding" {
      setup_data
      r sadd cap a
      assert_equal {0} [r vfind zset cap anti 1 filter2 asc 0 10 10]
    }

    test "vdiff 4 - $encoding" {
      setup_data
      r sadd filter2 z
      r sadd cap a
      assert_equal {0} [r vfind zset cap anti 1 filter2 asc 0 10 10]
    }

    test "vdiff 5 - $encoding" {
      setup_data
      assert_equal {s_d s_v s_g s_f s_c good_stuff 6} [r vfind zset cap anti 1 filter1 desc 0 10 10]
    }

    test "vdiff 6 - $encoding" {
      setup_data
      r sadd cap d v g f c mnm
      assert_equal {0} [r vfind zset cap anti 1 filter1 desc 0 10 10]
    }

    test "vdiff 7 - $encoding" {
      setup_data
      r sadd filter2 d g u y x o p
      r sadd cap d v
      assert_equal {s_g 1} [r vfind zset cap anti 2 filter1 filter2 desc 0 10 10]
    }

    test "vdiff 8 - $encoding" {
      setup_data
      r sadd filter2 d g u
      r sadd cap u a j
      assert_equal {s_d s_g 2} [r vfind zset cap anti 2 filter1 filter2 desc 0 10 10]
    }

    test "vdiff 9 - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {s_f s_c 5} [r vfind zset cap anti 1 filter1 desc 2 2 10]
    }

    test "vdiff 10 - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {s_g s_f 5} [r vfind zset cap anti 1 filter1 desc 1 2 10]
    }

    test "vdiff 11 - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {s_g s_f 7} [r vfind zset cap anti 0 desc 1 2 10]
    }

    test "vdiff 12 - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {s_a s_c 7} [r vfind zset cap anti 0 asc 1 2 10]
    }

    test "vdiff 13 - $encoding" {
      setup_data
      r sadd filter2 d v g
      r sadd cap d
      assert_equal {s_g s_v 2} [r vfind zset cap anti 2 filter1 filter2 asc 0 10 10]
    }

    test "vdiff 14 - $encoding" {
      setup_data
      r sadd filter2 d v g
      r sadd cap d
      assert_equal {s_v s_g 2} [r vfind zset cap anti 2 filter1 filter2 desc 0 10 10]
    }

    test "vdiff 15 - $encoding" {
      setup_data
      r sadd cap c f g
      r sadd anti c g
      assert_equal {good_stuff s_c s_g s_v s_d 5} [r vfind zset cap anti 1 filter1 asc 0 10 10]
    }

    test "vdiff 16 up_to- $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {s_f s_e 4} [r vfind zset cap anti 0 desc 2 2 3]
    }

    test "bricks - $encoding" {
      r zadd a_zset 1 1b 10 3v
      r hmset r:1b details "{\"name\":\"1b_details\"}" resource_id 3v
      r hmset r:3v details "{\"name\":\"3v_details\"}"
      r sadd cap a v c 3v
      assert_equal {{{"name":"1b_details", "resource": {"name":"3v_details"}}} 1} [r vfind a_zset cap 0 0 desc 0 10 10]
    }
  }
  basics ziplist
 basics skiplist
}
