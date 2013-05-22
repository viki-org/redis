start_server {tags {"vfind"}} {
  proc setup_data {} {
    r del zset filter1 filter2 cap r:a r:v r:c r:d r:e r:f r:g r:mnm anti incl excl

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
      assert_error $err {r vfind list }
      assert_error $err {r vfind list cap}
      assert_error $err {r vfind list cap anti }
      assert_error $err {r vfind list cap anti 0}
      assert_error $err {r vfind list cap anti 0 10}
      assert_error $err {r vfind list cap anti 0 10 1000}
      assert_error $err {r vfind list cap anti 0 10 1000 desc}
      assert_error $err {r vfind list cap anti 0 10 1000 desc incl}
      assert_error $err {r vfind list cap anti 0 10 1000 desc incl excl}
    }

    test "vfind cap is not a set - $encoding" {
      set err "ERR Operation against a key holding the wrong kind of value"
      setup_data
      r set cap nocap
      assert_error $err {r vfind zset cap anti 0 10 1000 desc incl excl details 0}
    }

    test "vfind anti is not a set - $encoding" {
      set err "ERR Operation against a key holding the wrong kind of value"
      setup_data
      r set anti nocap
      assert_error $err {r vfind zset cap anti 0 10 1000 desc incl excl details 0}
    }

    test "vfind inclusion is not a zset - $encoding" {
      set err "ERR Operation against a key holding the wrong kind of value"
      setup_data
      r set incl noincl
      assert_error $err {r vfind zset cap anti 0 10 1000 desc incl excl details 0}
    }

    test "vfind exclusion is not a zset - $encoding" {
      setup_data
      set err "ERR Operation against a key holding the wrong kind of value"
      r set excl noexcl
      assert_error $err {r vfind zset cap anti 0 10 1000 desc incl excl details 0}
    }

    test "vfind invalid offset count of up_to - $encoding" {
      setup_data
      set err "ERR value is not an integer or out of range"
      assert_error $err {r vfind zset cap anti a 10 1000 desc incl excl details 0}
      assert_error $err {r vfind zset cap anti 0 b 1000 desc incl excl details 0}
      assert_error $err {r vfind zset cap anti 0 10 c desc incl excl details 0}
    }

    test "vfind invalid list type - $encoding" {
      set err "ERR Operation against a key holding the wrong kind of value"
      r sadd myset a v c d e f g mnm
      assert_error $err {r vfind myset cap anti 0 10 1000 desc incl excl details 0}
    }

    test "vfind returns empty for a missing list - $encoding" {
      assert_equal {0} [r vfind x cap anti 0 10 1000 desc incl excl details 0]
    }

    test "vfind filter and holdback descending - $encoding" {
      setup_data
      r sadd cap a c f z y mnm
      assert_equal {s_d s_v s_g 3} [r vfind zset cap 0 0 10 11 desc incl excl details 1 filter1]
    }

    test "vfind filter holdback ascending $encoding" {
      setup_data
      r sadd cap a c f z y
      assert_equal {good_stuff s_g s_v s_d 4} [r vfind zset cap 0 0 10 11 asc incl excl details 1 filter1]
    }

    test "vfind filter all with a non existing filter - $encoding" {
      setup_data
      r sadd cap a
      assert_equal {0} [r vfind zset cap 0 0 10 11 asc incl excl details 1 filter2]
    }

    test "vfind filter all with a highly restrictive filter - $encoding" {
      setup_data
      r sadd filter2 z
      r sadd cap a
      assert_equal {0} [r vfind zset cap 0 0 10 11 asc incl excl details 1 filter2]
    }

    test "vfind no holdbacks - $encoding" {
      setup_data
      assert_equal {s_d s_v s_g s_f s_c good_stuff 6} [r vfind zset cap 0 0 10 11 desc incl excl details 1 filter1]
    }

    test "vfind holdback all - $encoding" {
      setup_data
      r sadd cap d v g f c mnm
      assert_equal {0} [r vfind zset cap 0 0 10 11 desc incl excl details 1 filter1]
    }

    test "vfind mulitple filters - $encoding" {
      setup_data
      r sadd filter2 d g u y x o p
      r sadd cap d v
      assert_equal {s_g 1} [r vfind zset cap 0 0 10 11 desc incl excl details 2 filter1 filter2]
    }

    test "vfind mulitple filters 2 - $encoding" {
      setup_data
      r sadd filter2 d g u
      r sadd cap u a j
      assert_equal {s_d s_g 2} [r vfind zset cap 0 0 10 11 desc incl excl details 2 filter1 filter2]
    }

    test "vfind mulitple filters 3 - $encoding" {
      setup_data
      r sadd filter2 d v g
      r sadd cap d
      assert_equal {s_g s_v 2} [r vfind zset cap 0 0 10 11 asc incl excl details 2 filter1 filter2]
    }

    test "vfind mulitple filters 4 - $encoding" {
      setup_data
      r sadd filter2 d v g
      r sadd cap d
      assert_equal {s_v s_g 2} [r vfind zset cap 0 0 10 11 desc incl excl details 2 filter1 filter2]
    }

    test "vfind paging offset - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {s_f s_c 5} [r vfind zset cap 0 2 2 10 desc incl excl details 1 filter1]
    }

    test "vfind paging offset 2 - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {s_g s_f 5} [r vfind zset cap 0 1 2 10 desc incl excl details 1 filter1]
    }

    test "vfind paging offset ascending - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {s_a s_c 7} [r vfind zset cap 0 1 2 10 asc incl excl details 0]
    }

    test "vfind anti cap - $encoding" {
      setup_data
      r sadd cap c f g
      r sadd anti c g
      assert_equal {good_stuff s_c s_g s_v s_d 5} [r vfind zset cap anti 0 10 10 asc incl excl details 1 filter1]
    }

    test "vfind upto limit - $encoding" {
      setup_data
      r sadd cap v x
      assert_equal {s_f s_e 4} [r vfind zset cap anti 2 2 3 desc incl excl details 0]
    }

    test "bricks 1 - $encoding" {
      r del zset r:1b r:3v cap
      r zadd a_zset 1 1b 10 3v
      r hmset r:1b details "{\"name\":\"1b_details\"}" resource_id 3v
      r hmset r:3v details "{\"name\":\"3v_details\"}"
      r sadd cap a v c 3v
      assert_equal {{{"name":"1b_details", "resource": {"name":"3v_details"}}} 1} [r vfind a_zset cap 0 0 10 10 desc incl excl details 0]
    }

    test "bricks 2 - $encoding" {
      r del zset r:1b r:3v cap
      r zadd a_zset 1 1b 10 3v
      r hmset r:1b details "{\"name\":\"1b_details\"}"
      r hmset r:3v details "{\"name\":\"3v_details\"}"
      r sadd cap a v c 3v
      assert_equal {{{"name":"1b_details"}} 1} [r vfind a_zset cap 0 0 10 10 desc incl excl details 0]
    }

    test "bricks 3 - $encoding" {
      r del zset r:1b r:3v cap
      r zadd a_zset 1 1b 10 3v
      r hmset r:1b details "{\"name\":\"1b_details\"}" resource_id 3v
      r sadd cap a v c 3v
      assert_equal {{{"name":"1b_details"}} 1} [r vfind a_zset cap 0 0 10 10 desc incl excl details 0]
    }

    test "vfind get specific field - $encoding" {
      setup_data
      r hset r:a en sa_en
      r hset r:c en sc_en
      r hset r:d en sd_en
      r hset r:e en se_en
      r hset r:f en sf_en
      r hset r:g en sg_en
      assert_equal {sd_en sg_en sf_en sc_en 4} [r vfind zset cap 0 0 10 10 desc incl excl en 1 filter1]
    }

    test "vfind applies inclusion list - $encoding" {
      setup_data
      r sadd cap a c f z y mnm
      r zadd incl 0 f 1 c
      assert_equal {s_d s_v s_g s_f s_c 5} [r vfind zset cap 0 0 10 11 desc incl excl details 1 filter1]
    }

    test "vfind applies exclusion list - $encoding" {
      setup_data
      r sadd cap a c f z y mnm
      r sadd excl v d
      assert_equal {s_g 1} [r vfind zset cap 0 0 10 11 desc incl excl details 1 filter1]
    }
  }
  basics ziplist
  basics skiplist
}
