start_server {tags {"viki"}} {
  proc setup_data {} {
    r del zset filter1 filter2 cap r:a r:b r:c r:d r:e r:f r:g r:mnm
    r hset r:a summary s_a
    r hset r:b summary s_b
    r hset r:c summary s_c
    r hset r:d summary s_d
    r hset r:e summary s_e
    r hset r:f summary s_f
    r hset r:g summary s_g
    r hset r:mnm summary good_stuff

    r zadd zset 1 a 10 b 3 c 15 d 5 e 6 f 7 g 0 mnm
    r sadd filter1 b c d f g h i o u mnm
    #zset and filter1 have d b g f c in common
  }

  proc basics {encoding} {
    test "vdiff params 1 - $encoding" {
      set err "ERR wrong number of arguments for 'vfind' command"
      assert_error $err { r vfind }
      assert_error $err { r vfind ds }
      assert_error $err { r vfind a b}
      assert_error $err { r vfind a b 0 desc a}
    }

    test "vdiff params 2 - $encoding" {
      set err "ERR value is not an integer or out of range"
      assert_error $err {r vfind a b a x 0 10}
      assert_error $err {r vfind a b 0 desc a 0}
      assert_error $err {r vfind a b 0 desc 0 a}
    }

    test "vdiff 1 - $encoding" {
      setup_data
      r sadd cap a c f z y mnm
      assert_equal {s_d s_b s_g 3} [r vfind zset cap 1 filter1 desc 0 10]
    }

    test "vdiff 2 - $encoding" {
      setup_data
      r sadd cap a c f z y
      assert_equal {good_stuff s_g s_b s_d 4} [r vfind zset cap 1 filter1 asc 0 10]
    }

    test "vdiff 3 - $encoding" {
      setup_data
      r sadd cap a
      assert_equal {0} [r vfind zset cap 1 filter2 asc 0 10]
    }

    test "vdiff 4 - $encoding" {
      setup_data
      r sadd filter2 z
      r sadd cap a
      assert_equal {0} [r vfind zset cap 1 filter2 asc 0 10]
    }

    test "vdiff 5 - $encoding" {
      setup_data
      assert_equal {s_d s_b s_g s_f s_c good_stuff 6} [r vfind zset cap 1 filter1 desc 0 10]
    }

    test "vdiff 6 - $encoding" {
      setup_data
      r sadd cap d b g f c mnm
      assert_equal {0} [r vfind zset cap 1 filter1 desc 0 10]
    }

    test "vdiff 7 - $encoding" {
      setup_data
      r sadd filter2 d g u y x o p
      r sadd cap d b
      assert_equal {s_g 1} [r vfind zset cap 2 filter1 filter2 desc 0 10]
    }

    test "vdiff 8 - $encoding" {
      setup_data
      r sadd filter2 d g u
      r sadd cap u a j
      assert_equal {s_d s_g 2} [r vfind zset cap 2 filter1 filter2 desc 0 10]
    }

    test "vdiff 9 - $encoding" {
      setup_data
      r sadd cap b x
      assert_equal {s_f s_c 5} [r vfind zset cap 1 filter1 desc 2 2]
    }

    test "vdiff 10 - $encoding" {
      setup_data
      r sadd cap b x
      assert_equal {s_g s_f 5} [r vfind zset cap 1 filter1 desc 1 2]
    }

    test "vdiff 11 - $encoding" {
      setup_data
      r sadd cap b x
      assert_equal {s_g s_f 7} [r vfind zset cap 0 desc 1 2]
    }

    test "vdiff 12 - $encoding" {
      setup_data
      r sadd cap b x
      assert_equal {s_a s_c 7} [r vfind zset cap 0 asc 1 2]
    }

    test "vdiff 13 - $encoding" {
      setup_data
      r sadd filter2 d b g
      r sadd cap d
      assert_equal {s_g s_b 2} [r vfind zset cap 2 filter1 filter2 asc 0 10]
    }

    test "vdiff 14 - $encoding" {
      setup_data
      r sadd filter2 d b g
      r sadd cap d
      assert_equal {s_b s_g 2} [r vfind zset cap 2 filter1 filter2 desc 0 10]
    }
  }
  basics ziplist
  basics skiplist
}