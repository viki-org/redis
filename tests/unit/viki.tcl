start_server {tags {"viki"}} {
  proc setup_data {} {
    r del zset filter1 filter2 cap r:a r:b r:c r:d r:e r:f r:g
    r hset r:a summary s_a
    r hset r:b summary s_b
    r hset r:c summary s_c
    r hset r:d summary s_d
    r hset r:e summary s_e
    r hset r:f summary s_f
    r hset r:g summary s_g

    r zadd zset 1 a 10 b 3 c 15 d 5 e 6 f 7 g
    r sadd filter1 b c d f g h i o u
    #zset and filter1 have d b g f c in common
  }

  proc basics {encoding} {
    test "vdiff 1 - $encoding" {
      setup_data
      r sadd cap a c f z y
      assert_equal {3 s_d s_b s_g} [vfind 3 zset filter1 cap desc 0 10]
    }

    test "vdiff 2 - $encoding" {
      setup_data
      r sadd cap a c f z y
      assert_equal {3 s_g s_b s_d} [vfind 3 zset filter1 cap asc 0 10]
    }

    test "vdiff 3 - $encoding" {
      setup_data
      r sadd cap a
      assert_equal {0} [vfind 3 zset filter2 cap asc 0 10]
    }

    test "vdiff 4 - $encoding" {
      setup_data
      r sadd filter2 z
      r sadd cap a
      assert_equal {0} [vfind 3 zset filter2 cap asc 0 10]
    }

    test "vdiff 5 - $encoding" {
      setup_data
      assert_equal {5 s_d s_b s_g s_f s_c} [vfind 3 zset filter1 cap desc 0 10]
    }

    test "vdiff 6 - $encoding" {
      setup_data
      r sadd cap d b g f c
      assert_equal {5 s_d s_b s_g s_f s_c} [vfind 3 zset filter1 cap desc 0 10]
    }

    test "vdiff 7 - $encoding" {
      setup_data
      r sadd filter2 d g u
      r sadd cap d b
      assert_equal {1 s_g} [vfind 4 zset filter1 filter2 cap desc 0 10]
    }

    test "vdiff 8 - $encoding" {
      setup_data
      r sadd filter2 d g u
      r sadd cap u a j
      assert_equal {3 s_d s_b s_g} [vfind 4 zset filter1 filter2 cap desc 0 10]
    }

    test "vdiff 9 - $encoding" {
      setup_data
      r sadd cap b x
      assert_equal {4 s_f s_c} [vfind 3 zset filter1 cap desc 2 2]
    }

    test "vdiff 10 - $encoding" {
      setup_data
      r sadd cap b x
      assert_equal {4 s_g s_f} [vfind 3 zset filter1 cap desc 1 2]
    }
  }
  basics ziplist
  basics skiplist
}